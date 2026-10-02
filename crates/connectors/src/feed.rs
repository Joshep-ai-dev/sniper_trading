use crate::protocol::{self, Idl, PUMP, SWAP};
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sniper_domain::*;
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{protocol::WebSocketConfig, Message};

#[derive(Clone)]
pub struct ParsedEvent {
    pub event: MarketEvent,
    pub pool: Option<Pubkey>,
}
pub struct TokenParser {
    pump: Idl,
    swap: Idl,
}
impl Default for TokenParser {
    fn default() -> Self {
        Self {
            pump: Idl::load(Venue::PumpFun),
            swap: Idl::load(Venue::PumpSwap),
        }
    }
}
impl TokenParser {
    pub fn preprocessed(&self, frame: &[u8], received: u64) -> Result<Vec<ParsedEvent>> {
        ensure!(
            (74..=1305).contains(&frame.len()),
            "invalid preprocessed frame length"
        );
        ensure!(frame[0] == 1, "unsupported preprocessed schema");
        let slot = u64::from_le_bytes(frame[1..9].try_into()?);
        let transaction: VersionedTransaction = bincode::deserialize(&frame[73..])
            .context("unsupported or invalid Solana wire transaction")?;
        ensure!(
            transaction
                .signatures
                .first()
                .is_some_and(|s| s.as_ref() == &frame[9..73]),
            "frame signature mismatch"
        );
        // Loaded ALT indices are not guessed. Missing ALT accounts skip only affected instructions.
        let keys = transaction.message.static_account_keys();
        let signature = transaction.signatures[0].to_string();
        let mut result = vec![];
        for (index, ix) in transaction.message.instructions().iter().enumerate() {
            let Some(program) = keys.get(ix.program_id_index as usize) else {
                continue;
            };
            let (idl, venue) = if *program == self.pump.program {
                (&self.pump, Venue::PumpFun)
            } else if *program == self.swap.program {
                (&self.swap, Venue::PumpSwap)
            } else {
                continue;
            };
            let Some(name) = idl.identify(&ix.data) else {
                continue;
            };
            let accounts: Option<Vec<Pubkey>> = ix
                .accounts
                .iter()
                .map(|n| keys.get(*n as usize).copied())
                .collect();
            let Some(accounts) = accounts else { continue };
            let Ok(ctx) = idl.instruction_accounts(name, &accounts) else {
                continue;
            };
            let kind = match name {
                "create" | "create_v2" | "create_pool" => EventKind::Create,
                "buy" | "buy_exact_sol_in" | "buy_exact_quote_in" => EventKind::Buy,
                "sell" => EventKind::Sell,
                "migrate" => EventKind::Migration,
                _ => continue,
            };
            let mint = ctx
                .get("mint")
                .or_else(|| ctx.get("base_mint"))
                .copied()
                .context("mint missing")?;
            let creator = ctx
                .get("creator")
                .or_else(|| ctx.get("user"))
                .copied()
                .unwrap_or_default();
            result.push(ParsedEvent {
                pool: ctx.get("pool").copied(),
                event: MarketEvent {
                    signature: signature.clone(),
                    slot,
                    instruction_index: index as u16,
                    mint: protocol::key(mint),
                    creator: protocol::key(creator),
                    venue,
                    kind,
                    observed_ms: received,
                    blockchain_ms: None,
                    price: 0,
                    market_cap: 0,
                    trader: ctx.get("user").copied().map(protocol::key),
                    sol_amount: 0,
                    token_amount: 0,
                    speculative: true,
                    facts: None,
                },
            });
        }
        Ok(result)
    }
    pub fn processed(&self, response: &Value, received: u64) -> Result<Vec<ParsedEvent>> {
        let result = &response["params"]["result"];
        let tx = &result["transaction"];
        let meta = tx
            .get("meta")
            .or_else(|| tx["transaction"].get("meta"))
            .context("transaction metadata missing")?;
        if !meta["err"].is_null() {
            return Ok(vec![]);
        }
        let signature = result["signature"]
            .as_str()
            .or_else(|| tx["transaction"]["signatures"][0].as_str())
            .context("signature missing")?;
        let slot = result["slot"].as_u64().unwrap_or(0);
        let mut stack = Vec::<String>::new();
        let mut events = vec![];
        // Log events must be emitted by the active Pump program, not spoofed by another program.
        for (index, log) in meta["logMessages"]
            .as_array()
            .context("log messages missing")?
            .iter()
            .enumerate()
        {
            let Some(log) = log.as_str() else { continue };
            if log.starts_with("Program ") && log.contains(" invoke [") {
                if let Some(program) = log.split_whitespace().nth(1) {
                    stack.push(program.into());
                }
                continue;
            }
            if log.starts_with("Program ")
                && (log.ends_with(" success") || log.contains(" failed:"))
            {
                stack.pop();
                continue;
            }
            let Some(encoded) = log.strip_prefix("Program data: ") else {
                continue;
            };
            let (idl, venue) = match stack.last().map(String::as_str) {
                Some(PUMP) => (&self.pump, Venue::PumpFun),
                Some(SWAP) => (&self.swap, Venue::PumpSwap),
                _ => continue,
            };
            let Ok(data) = STANDARD.decode(encoded) else {
                continue;
            };
            let Ok((name, fields)) = idl.decode_event(&data) else {
                continue;
            };
            let kind = match name.as_str() {
                "CreateEvent" | "CreatePoolEvent" => EventKind::Create,
                "TradeEvent" => {
                    if fields["is_buy"].as_bool().unwrap_or(false) {
                        EventKind::Buy
                    } else {
                        EventKind::Sell
                    }
                }
                "BuyEvent" => EventKind::Buy,
                "SellEvent" => EventKind::Sell,
                "CompleteEvent" => EventKind::Migration,
                _ => continue,
            };
            let mint = fields["mint"]
                .as_str()
                .or_else(|| fields["base_mint"].as_str());
            // PumpSwap trade logs omit the mint: resolve it locally from instruction account keys.
            let mint = if let Some(mint) = mint {
                mint.parse()?
            } else {
                let message = tx["transaction"]["message"]
                    .as_object()
                    .context("message missing")?;
                let keys = message
                    .get("accountKeys")
                    .and_then(Value::as_array)
                    .context("account keys")?;
                let mut found = None;
                for ix in message
                    .get("instructions")
                    .and_then(Value::as_array)
                    .context("instructions")?
                {
                    let account = |v: &Value| -> Option<Pubkey> {
                        let v = keys.get(v.as_u64()? as usize)?;
                        v.as_str()
                            .or_else(|| v["pubkey"].as_str())
                            .and_then(|s| s.parse().ok())
                    };
                    if ix["programIdIndex"]
                        .as_u64()
                        .and_then(|n| account(&json!(n)))
                        .is_some_and(|p| p == idl.program)
                    {
                        let data = bs58::decode(ix["data"].as_str().unwrap_or_default())
                            .into_vec()
                            .unwrap_or_default();
                        if let Some(name) = idl.identify(&data) {
                            let accounts: Option<Vec<_>> = ix["accounts"]
                                .as_array()
                                .and_then(|a| a.iter().map(account).collect());
                            if let Some(accounts) = accounts {
                                if let Ok(context) = idl.instruction_accounts(name, &accounts) {
                                    if context.get("pool").is_some_and(|p| {
                                        Some(p.to_string().as_str()) == fields["pool"].as_str()
                                    }) {
                                        found =
                                            context.get("base_mint").copied().map(protocol::key);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                let Some(mint) = found else { continue };
                mint
            };
            let creator = fields["creator"]
                .as_str()
                .or_else(|| fields["coin_creator"].as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or_default();
            let base = fields["virtual_token_reserves"]
                .as_u64()
                .or_else(|| fields["pool_base_token_reserves"].as_u64())
                .unwrap_or(0);
            let raw_quote = fields["virtual_sol_reserves"]
                .as_u64()
                .or_else(|| fields["pool_quote_token_reserves"].as_u64())
                .unwrap_or(0);
            let virtual_quote = if venue == Venue::PumpSwap {
                fields["virtual_quote_reserves"]
                    .as_str()
                    .unwrap_or("0")
                    .parse::<i128>()
                    .unwrap_or(0)
            } else {
                0
            };
            let quote = raw_quote as i128 + virtual_quote;
            let price = if base > 0 && quote > 0 {
                (quote as u128 * PRICE_SCALE as u128 / base as u128).min(u64::MAX as u128) as u64
            } else {
                0
            };
            events.push(ParsedEvent {
                pool: fields["pool"].as_str().and_then(|s| s.parse().ok()),
                event: MarketEvent {
                    signature: signature.into(),
                    slot,
                    instruction_index: index as u16,
                    mint,
                    creator,
                    venue,
                    kind,
                    observed_ms: received,
                    blockchain_ms: fields["timestamp"].as_u64().map(|n| n * 1000),
                    price,
                    market_cap: 0,
                    trader: fields["user"].as_str().and_then(|s| s.parse().ok()),
                    sol_amount: fields["sol_amount"]
                        .as_u64()
                        .or_else(|| fields["user_quote_amount_in"].as_u64())
                        .or_else(|| fields["user_quote_amount_out"].as_u64())
                        .unwrap_or(0),
                    token_amount: fields["token_amount"]
                        .as_u64()
                        .or_else(|| fields["base_amount_out"].as_u64())
                        .or_else(|| fields["base_amount_in"].as_u64())
                        .unwrap_or(0),
                    speculative: false,
                    facts: None,
                },
            });
        }
        Ok(events)
    }
}

pub async fn run(
    endpoint: String,
    preprocessed: bool,
    output: mpsc::Sender<ParsedEvent>,
    health: watch::Sender<ServiceStatus>,
) {
    let parser = TokenParser::default();
    let mut retry = 0u32;
    loop {
        let result = stream_once(&endpoint, preprocessed, &parser, &output, &health).await;
        let _ = health.send(ServiceStatus {
            name: if preprocessed {
                "Helius Preprocessed"
            } else {
                "Helius Confirmed"
            }
            .into(),
            error: Some(
                result
                    .err()
                    .map(|_| {
                        "Feed disconnected, authentication rejected, or parser protocol unsupported"
                            .into()
                    })
                    .unwrap_or_else(|| "Feed closed".into()),
            ),
            status: "FAILED".into(),
            ..ServiceStatus::missing("Helius Feed")
        });
        if output.is_closed() {
            break;
        }
        let delay = (250u64.saturating_mul(1u64 << retry.min(6))).min(15_000);
        retry = retry.saturating_add(1);
        let jitter = rand::random::<u64>() % 250;
        tokio::time::sleep(Duration::from_millis(delay + jitter)).await;
    }
}
async fn stream_once(
    endpoint: &str,
    preprocessed: bool,
    parser: &TokenParser,
    output: &mpsc::Sender<ParsedEvent>,
    health: &watch::Sender<ServiceStatus>,
) -> Result<()> {
    crate::rpc::validate_endpoint(endpoint, true)?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(2 << 20))
        .max_frame_size(Some(2 << 20));
    let (mut stream, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async_with_config(endpoint, Some(config), true),
    )
    .await?
    .map_err(|_| anyhow::anyhow!("feed connection failure"))?;
    let request = if preprocessed {
        json!({"jsonrpc":"2.0","id":1,"method":"preprocessedSubscribe","params":{"accountInclude":[PUMP,SWAP],"accountExclude":[],"accountRequired":[]}})
    } else {
        json!({"jsonrpc":"2.0","id":1,"method":"transactionSubscribe","params":[{"accountInclude":[PUMP,SWAP],"failed":false,"vote":false},{"commitment":"confirmed","encoding":"json","transactionDetails":"full","maxSupportedTransactionVersion":0}]})
    };
    stream
        .send(Message::Text(request.to_string().into()))
        .await?;
    let mut last = Instant::now();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _=heartbeat.tick()=>{ensure!(last.elapsed()<Duration::from_secs(30),"feed stale");stream.send(Message::Ping(vec![].into())).await?;},
            message=stream.next()=>{
                let message=message.context("feed closed")??;last=Instant::now();let now=SystemClock.now_ms();
                let events=match message {
                    Message::Binary(data) if preprocessed=>parser.preprocessed(&data,now)?,
                    Message::Text(data)=>{let json:Value=serde_json::from_str(&data)?;ensure!(json.get("error").is_none(),"feed subscription rejected");
                        if json.get("result").is_some(){let _=health.send(ServiceStatus {name:if preprocessed{"Helius Preprocessed"}else{"Helius Confirmed"}.into(),status:"CONNECTED".into(),last_success_ms:Some(now),..ServiceStatus::missing("Helius Feed")});vec![]}
                        else if !preprocessed {parser.processed(&json,now)?}else{vec![]}},
                    Message::Ping(data)=>{stream.send(Message::Pong(data)).await?;vec![]},Message::Close(_)=>return Ok(()),_=>vec![],
                };
                for event in events {output.send(event).await.context("market queue closed")?;}
            }
        }
    }
}

/// Bounded signature/instruction cache; confirmed observations remain distinct from speculative ones.
pub struct Dedup {
    seen: HashMap<(String, u16, bool), u64>,
    max: usize,
    ttl_ms: u64,
}
impl Dedup {
    pub fn new(max: usize, ttl_ms: u64) -> Self {
        Self {
            seen: HashMap::new(),
            max,
            ttl_ms,
        }
    }
    pub fn accept(&mut self, event: &MarketEvent) -> bool {
        let now = event.observed_ms;
        if self.seen.len() >= self.max {
            self.seen
                .retain(|_, t| now.saturating_sub(*t) < self.ttl_ms);
        }
        let key = (
            event.signature.clone(),
            event.instruction_index,
            event.speculative,
        );
        if self.seen.contains_key(&key) {
            return false;
        }
        if self.seen.len() >= self.max {
            return false;
        }
        self.seen.insert(key, now);
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_frames_fail() {
        let p = TokenParser::default();
        for n in 0..74 {
            assert!(p.preprocessed(&vec![0; n], 1).is_err());
        }
        assert!(p.preprocessed(&[2; 100], 1).is_err());
    }
}
