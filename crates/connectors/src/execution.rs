use crate::{
    protocol::{self, Idl},
    rpc::{Rpc, DEVNET_GENESIS, MAINNET_GENESIS},
};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sniper_domain::*;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as system_instruction;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub struct PaperExecutor {
    pub mode: Mode,
    pub config: PaperConfig,
}
impl PaperExecutor {
    pub fn new(mode: Mode, config: PaperConfig) -> Result<Self> {
        ensure!(
            matches!(mode, Mode::Paper | Mode::Replay),
            "virtual executor requires PAPER or REPLAY"
        );
        ensure!(
            config.slippage_bps < 10_000 && config.failure_bps <= 10_000,
            "invalid simulation configuration"
        );
        Ok(Self { mode, config })
    }
}
#[async_trait]
impl TradeExecutor for PaperExecutor {
    fn mode(&self) -> Mode {
        self.mode
    }
    fn prepare(&self, request: OrderRequest) -> Result<PreparedOrder> {
        validate_mode(self.mode, request.mode)?;
        ensure!(request.price > 0 && request.amount > 0, "invalid quote");
        let hash = Sha256::digest(bincode::serialize(&request)?);
        let signature = format!("virtual-{:x}", hash);
        Ok(PreparedOrder {
            request,
            pool: None,
            signature,
            wire: vec![],
            last_valid_block_height: 0,
            fee_budget: self.config.fee,
            tip: self.config.tip,
        })
    }
    async fn submit(&self, order: &PreparedOrder) -> Result<()> {
        validate_mode(self.mode, order.request.mode)?;
        if self.mode != Mode::Replay {
            tokio::time::sleep(Duration::from_millis(self.config.network_delay_ms)).await;
        }
        Ok(())
    }
    async fn reconcile(&self, order: &PreparedOrder) -> Result<Settlement> {
        validate_mode(self.mode, order.request.mode)?;
        if self.mode != Mode::Replay {
            tokio::time::sleep(Duration::from_millis(self.config.landing_delay_ms)).await;
        }
        let r = &order.request;
        let hash = Sha256::digest(format!("{}:{}", order.signature, self.config.seed));
        if u16::from_le_bytes([hash[0], hash[1]]) as u32 % 10_000 < self.config.failure_bps as u32 {
            return Ok(Settlement::ProvenFailed { fee: 0 });
        }
        let slip = self.config.slippage_bps as u128;
        let (quantity, sol) = if r.buy {
            (
                (r.amount as u128 * PRICE_SCALE as u128 * 10_000)
                    / (r.price as u128 * (10_000 + slip)),
                r.amount as u128,
            )
        } else {
            (
                r.amount as u128,
                (r.amount as u128 * r.price as u128 * (10_000 - slip))
                    / (PRICE_SCALE as u128 * 10_000),
            )
        };
        ensure!(
            quantity > 0 && quantity <= u64::MAX as u128 && sol <= u64::MAX as u128,
            "simulation amount overflow"
        );
        Ok(Settlement::Filled(Fill {
            signature: order.signature.clone(),
            token_quantity: quantity as u64,
            sol_amount: sol as u64,
            network_fee: self.config.fee,
            priority_fee: 0,
            tip: self.config.tip,
            timestamp_ms: r.created_ms
                + self.config.network_delay_ms
                + self.config.landing_delay_ms,
        }))
    }
    fn ready(&self) -> Vec<PreflightCheck> {
        vec![PreflightCheck {
            name: "Virtual executor".into(),
            pass: true,
            reason: "No blockchain submission".into(),
        }]
    }
}

#[derive(Clone)]
pub struct CachedMarket {
    pub venue: Venue,
    pub context: HashMap<String, Pubkey>,
    pub facts: TokenFacts,
    pub price: u64,
    pub market_cap: u64,
    pub refreshed: Instant,
}
pub struct WarmState {
    pub blockhash: Option<Hash>,
    pub blockhash_at: Option<Instant>,
    pub last_valid_height: u64,
    pub network_verified: bool,
    pub balance: u64,
    pub priority_micro_lamports: u64,
    pub tip: u64,
    pub tip_account: Option<Pubkey>,
    pub sender_ready: bool,
    pub jito_ready: bool,
    pub markets: HashMap<Key, CachedMarket>,
    pub statuses: Vec<ServiceStatus>,
}
impl Default for WarmState {
    fn default() -> Self {
        Self {
            blockhash: None,
            blockhash_at: None,
            last_valid_height: 0,
            network_verified: false,
            balance: 0,
            priority_micro_lamports: 1_000_000,
            tip: 1_000_000,
            tip_account: None,
            sender_ready: false,
            jito_ready: false,
            markets: HashMap::new(),
            statuses: vec![],
        }
    }
}
pub struct NetworkExecutor {
    mode: Mode,
    signer: Mutex<Keypair>,
    pub rpc: Rpc,
    sender: Rpc,
    jito: Rpc,
    pub warm: Arc<RwLock<WarmState>>,
    live_authorized: AtomicBool,
    pub address: Key,
    risk: RwLock<RiskConfig>,
}
impl NetworkExecutor {
    pub fn analysis_only(rpc: Rpc) -> Result<Arc<Self>> {
        let keypair = Keypair::new();
        let secret = serde_json::to_string(&keypair.to_bytes().to_vec())?;
        Self::new(
            Mode::Live,
            &secret,
            rpc.clone(),
            rpc.clone(),
            rpc,
            RiskConfig::default(),
        )
    }
    pub fn new(
        mode: Mode,
        secret: &str,
        rpc: Rpc,
        sender: Rpc,
        jito: Rpc,
        risk: RiskConfig,
    ) -> Result<Arc<Self>> {
        ensure!(
            matches!(mode, Mode::Live | Mode::Devnet),
            "network executor requires LIVE or DEVNET"
        );
        let raw = zeroize::Zeroizing::new(if secret.starts_with('[') {
            serde_json::from_str::<Vec<u8>>(secret)?
        } else {
            base58(secret)?
        });
        let signer = Keypair::try_from(raw.as_slice())
            .map_err(|_| anyhow::anyhow!("invalid trading keypair"))?;
        let address = protocol::key(signer.pubkey());
        Ok(Arc::new(Self {
            mode,
            signer: Mutex::new(signer),
            rpc,
            sender,
            jito,
            warm: Arc::new(RwLock::new(WarmState::default())),
            live_authorized: AtomicBool::new(false),
            address,
            risk: RwLock::new(risk),
        }))
    }
    pub async fn refresh(&self) -> Result<()> {
        let expected = if self.mode == Mode::Live {
            MAINNET_GENESIS
        } else {
            DEVNET_GENESIS
        };
        ensure!(self.rpc.genesis().await? == expected, "WRONG_NETWORK");
        let block = self
            .rpc
            .call("getLatestBlockhash", json!([{"commitment":"confirmed"}]))
            .await?;
        let balance = self.rpc.balance(&self.address.to_string()).await?;
        let hash = Hash::from_str(block["value"]["blockhash"].as_str().context("blockhash")?)?;
        let height = block["value"]["lastValidBlockHeight"]
            .as_u64()
            .context("block height")?;
        // No Mainnet tip service is contacted by the DEVNET executor.
        let tip_accounts = if self.mode == Mode::Live {
            self.jito
                .with_path("/api/v1/bundles")?
                .call("getTipAccounts", json!([]))
                .await
                .ok()
        } else {
            None
        };
        let sender_ready = self.mode == Mode::Devnet || self.sender.ping().await.is_ok();
        let fees = self
            .rpc
            .call("getRecentPrioritizationFees", json!([]))
            .await
            .ok();
        let mut state = self.warm.write();
        state.network_verified = true;
        state.balance = balance;
        state.blockhash = Some(hash);
        state.blockhash_at = Some(Instant::now());
        state.last_valid_height = height;
        state.sender_ready = sender_ready;
        if let Some(fees) = fees {
            let mut values: Vec<_> = fees
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["prioritizationFee"].as_u64())
                .collect();
            values.sort_unstable();
            if let Some(value) = values.get(values.len() * 3 / 4) {
                state.priority_micro_lamports = *value;
            }
        }
        if self.mode == Mode::Devnet {
            state.sender_ready = true;
            state.jito_ready = true;
        } else if let Some(tips) = tip_accounts {
            state.tip_account = tips
                .as_array()
                .and_then(|t| t.first())
                .and_then(Value::as_str)
                .and_then(|s| Pubkey::from_str(s).ok());
            state.jito_ready = state.tip_account.is_some();
        }
        state
            .markets
            .retain(|_, m| m.refreshed.elapsed() < Duration::from_secs(300));
        Ok(())
    }
    pub fn set_sender_ready(&self, ready: bool) {
        self.warm.write().sender_ready = ready;
    }
    pub async fn warm_market(
        &self,
        mint: Key,
        venue: Venue,
        pool: Option<Pubkey>,
    ) -> Result<CachedMarket> {
        let idl = Idl::load(venue);
        let global = Pubkey::find_program_address(
            &[if venue == Venue::PumpFun {
                b"global"
            } else {
                b"global_config"
            }],
            &idl.program,
        )
        .0;
        let market = if venue == Venue::PumpFun {
            Pubkey::find_program_address(&[b"bonding-curve", &mint.0], &idl.program).0
        } else {
            pool.or_else(|| {
                self.warm
                    .read()
                    .markets
                    .get(&mint)
                    .and_then(|m| m.context.get("pool").copied())
            })
            .context("pool not discovered")?
        };
        let response=self.rpc.call("getMultipleAccounts",json!([[global.to_string(),market.to_string(),mint.to_string()],{"encoding":"base64","commitment":"confirmed"}])).await?;
        let accounts = response["value"].as_array().context("accounts")?;
        let account_data = |i: usize| -> Result<Vec<u8>> {
            ensure!(!accounts[i].is_null(), "market account unavailable");
            Ok(STANDARD.decode(accounts[i]["data"][0].as_str().context("account data")?)?)
        };
        ensure!(
            accounts[0]["owner"] == idl.program.to_string()
                && accounts[1]["owner"] == idl.program.to_string(),
            "wrong market owner"
        );
        let (_, g) = idl.decode_account(&account_data(0)?)?;
        let (_, m) = idl.decode_account(&account_data(1)?)?;
        ensure!(
            !m["is_mayhem_mode"].as_bool().unwrap_or(false)
                && !m["is_cashback_coin"].as_bool().unwrap_or(false),
            "unsupported special market variant"
        );
        let token = Pubkey::from_str(accounts[2]["owner"].as_str().context("mint owner")?)?;
        ensure!(
            token == Pubkey::from_str(protocol::TOKEN)?
                || token == Pubkey::from_str(protocol::TOKEN_2022)?,
            "unsupported token program"
        );
        let mint_data = account_data(2)?;
        ensure!(mint_data.len() >= 82, "invalid mint data");
        // Extensions require separate transfer-hook/transfer-fee validation. Fail closed.
        ensure!(
            mint_data.len() == 82 || token == Pubkey::from_str(protocol::TOKEN)?,
            "token extensions require protocol validation"
        );
        let mut facts = TokenFacts {
            mint_authority: u32::from_le_bytes(mint_data[..4].try_into()?) != 0,
            freeze_authority: u32::from_le_bytes(mint_data[46..50].try_into()?) != 0,
            creator_score: 15,
            verified: true,
            ..Default::default()
        };
        let mut ctx = HashMap::new();
        ctx.insert("user".into(), protocol::pubkey(self.address));
        for field in ["buy", "sell"] {
            for a in idl.instruction(field)?["accounts"]
                .as_array()
                .context("IDL accounts")?
            {
                if let Some(pk) = a["address"].as_str() {
                    ctx.insert(
                        a["name"].as_str().context("name")?.into(),
                        Pubkey::from_str(pk)?,
                    );
                }
            }
        }
        let (price, cap) = if venue == Venue::PumpFun {
            ensure!(
                !m["complete"].as_bool().unwrap_or(true),
                "bonding curve migrated"
            );
            ensure!(
                m["quote_mint"]
                    .as_str()
                    .unwrap_or("11111111111111111111111111111111")
                    == "11111111111111111111111111111111",
                "only SOL curves supported"
            );
            ctx.insert("mint".into(), protocol::pubkey(mint));
            ctx.insert("token_program".into(), token);
            ctx.insert(
                "bonding_curve.creator".into(),
                Pubkey::from_str(m["creator"].as_str().context("creator")?)?,
            );
            ctx.insert(
                "fee_recipient".into(),
                Pubkey::from_str(g["fee_recipient"].as_str().context("fee recipient")?)?,
            );
            ctx.insert(
                "associated_user".into(),
                protocol::ata(
                    protocol::pubkey(self.address),
                    protocol::pubkey(mint),
                    token,
                ),
            );
            let quote = m["virtual_quote_reserves"]
                .as_u64()
                .or_else(|| m["virtual_sol_reserves"].as_u64())
                .context("quote reserves")?;
            let base = m["virtual_token_reserves"]
                .as_u64()
                .context("base reserves")?;
            ensure!(base > 0, "empty pool");
            let price = (quote as u128 * PRICE_SCALE as u128 / base as u128) as u64;
            let supply = m["token_total_supply"].as_u64().unwrap_or(0);
            (
                price,
                (supply as u128 * price as u128 / PRICE_SCALE as u128) as u64,
            )
        } else {
            ensure!(
                m["base_mint"] == mint.to_string() && m["quote_mint"] == protocol::WSOL,
                "only canonical SOL pools supported"
            );
            ctx.insert("pool".into(), market);
            ctx.insert("global_config".into(), global);
            for field in [
                "base_mint",
                "quote_mint",
                "pool_base_token_account",
                "pool_quote_token_account",
            ] {
                ctx.insert(
                    field.into(),
                    Pubkey::from_str(m[field].as_str().context("pool field")?)?,
                );
            }
            ctx.insert(
                "pool.coin_creator".into(),
                Pubkey::from_str(m["coin_creator"].as_str().context("coin creator")?)?,
            );
            ctx.insert(
                "protocol_fee_recipient".into(),
                Pubkey::from_str(
                    g["protocol_fee_recipients"][0]
                        .as_str()
                        .context("fee recipient")?,
                )?,
            );
            let quote_token = Pubkey::from_str(protocol::TOKEN)?;
            ctx.insert("base_token_program".into(), token);
            ctx.insert("quote_token_program".into(), quote_token);
            ctx.insert(
                "user_base_token_account".into(),
                protocol::ata(
                    protocol::pubkey(self.address),
                    protocol::pubkey(mint),
                    token,
                ),
            );
            ctx.insert(
                "user_quote_token_account".into(),
                protocol::ata(
                    protocol::pubkey(self.address),
                    Pubkey::from_str(protocol::WSOL)?,
                    quote_token,
                ),
            );
            let reserves=self.rpc.call("getMultipleAccounts",json!([[ctx["pool_base_token_account"].to_string(),ctx["pool_quote_token_account"].to_string()],{"encoding":"base64","commitment":"confirmed"}])).await?;
            let amount = |i: usize| -> Result<u64> {
                let bytes = STANDARD.decode(
                    reserves["value"][i]["data"][0]
                        .as_str()
                        .context("vault data")?,
                )?;
                ensure!(bytes.len() >= 72, "vault data truncated");
                Ok(u64::from_le_bytes(bytes[64..72].try_into()?))
            };
            let base = amount(0)?;
            let quote = amount(1)? as i128
                + m["virtual_quote_reserves"]
                    .as_str()
                    .unwrap_or("0")
                    .parse::<i128>()?;
            ensure!(base > 0 && quote > 0, "empty pool");
            let price = (quote as u128 * PRICE_SCALE as u128 / base as u128) as u64;
            let supply = u64::from_le_bytes(mint_data[36..44].try_into()?);
            (
                price,
                (supply as u128 * price as u128 / PRICE_SCALE as u128) as u64,
            )
        };
        let supply = u64::from_le_bytes(mint_data[36..44].try_into()?);
        ensure!(supply > 0, "empty token supply");
        let creator = if venue == Venue::PumpFun {
            ctx["bonding_curve.creator"]
        } else {
            ctx["pool.coin_creator"]
        };
        let excluded = if venue == Venue::PumpFun {
            protocol::ata(market, protocol::pubkey(mint), token)
        } else {
            ctx["pool_base_token_account"]
        };
        let largest = self
            .rpc
            .call(
                "getTokenLargestAccounts",
                json!([mint.to_string(),{"commitment":"confirmed"}]),
            )
            .await?;
        let mut max_holding = 0u64;
        for account in largest["value"]
            .as_array()
            .context("holder distribution unavailable")?
        {
            if account["address"] != excluded.to_string() {
                let amount = account["amount"]
                    .as_str()
                    .context("holder amount")?
                    .parse::<u64>()?;
                max_holding = max_holding.max(amount);
            }
        }
        facts.concentration_bps =
            (max_holding as u128 * 10_000 / supply as u128).min(10_000) as u16;
        let creator_accounts=self.rpc.call("getTokenAccountsByOwner",json!([creator.to_string(),{"mint":mint.to_string()},{"encoding":"jsonParsed","commitment":"confirmed"}])).await?;
        let mut dev_tokens = 0u128;
        for account in creator_accounts["value"]
            .as_array()
            .context("creator allocation unavailable")?
        {
            dev_tokens += account["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
                .as_str()
                .context("creator token amount")?
                .parse::<u64>()? as u128;
        }
        facts.dev_allocation_bps = (dev_tokens * 10_000 / supply as u128).min(10_000) as u16;
        let cached = CachedMarket {
            venue,
            context: ctx,
            facts,
            price,
            market_cap: cap,
            refreshed: Instant::now(),
        };
        let mut warm = self.warm.write();
        if warm.markets.len() >= 10_000 {
            warm.markets
                .retain(|_, m| m.refreshed.elapsed() < Duration::from_secs(30));
        }
        ensure!(warm.markets.len() < 10_000, "market cache full");
        warm.markets.insert(mint, cached.clone());
        Ok(cached)
    }
}
fn base58(s: &str) -> Result<Vec<u8>> {
    Ok(bs58::decode(s).into_vec()?)
}

#[async_trait]
impl TradeExecutor for NetworkExecutor {
    fn mode(&self) -> Mode {
        self.mode
    }
    fn wallet_balance(&self) -> Option<u64> {
        let w = self.warm.read();
        w.network_verified.then_some(w.balance)
    }
    async fn validate_position(&self, p: &Position) -> Result<()> {
        validate_mode(self.mode, p.mode)?;
        let response=self.rpc.call("getTokenAccountsByOwner",json!([self.address.to_string(),{"mint":p.mint.to_string()},{"encoding":"jsonParsed","commitment":"finalized"}])).await?;
        let mut total = 0u64;
        for account in response["value"]
            .as_array()
            .context("token accounts missing")?
        {
            let n = account["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
                .as_str()
                .context("token balance missing")?
                .parse::<u64>()?;
            total = total.checked_add(n).context("token balance overflow")?;
        }
        ensure!(
            total == p.quantity,
            "wallet holdings differ from recovered position; reconciliation required"
        );
        Ok(())
    }
    fn authorize_live(&self, enabled: bool) {
        self.live_authorized.store(enabled, Ordering::Release);
    }
    fn configure_risk(&self, risk: RiskConfig) {
        *self.risk.write() = risk;
    }
    fn market_route(&self, mint: Key) -> Option<Key> {
        self.warm
            .read()
            .markets
            .get(&mint)
            .and_then(|m| m.context.get("pool").copied())
            .map(protocol::key)
    }
    fn ready(&self) -> Vec<PreflightCheck> {
        let w = self.warm.read();
        let risk = self.risk.read();
        vec![
            PreflightCheck {
                name: "Network".into(),
                pass: w.network_verified,
                reason: "Genesis hash verified against configured execution mode".into(),
            },
            PreflightCheck {
                name: "Blockhash".into(),
                pass: w
                    .blockhash_at
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(15)),
                reason: "Cached blockhash must be younger than 15 seconds".into(),
            },
            PreflightCheck {
                name: "Balance".into(),
                pass: w.balance > risk.min_balance.saturating_add(risk.sol_per_trade),
                reason: "Real wallet balance includes required reserve".into(),
            },
            PreflightCheck {
                name: "Sender".into(),
                pass: w.sender_ready,
                reason: "Sender transport health".into(),
            },
            PreflightCheck {
                name: "Jito".into(),
                pass: w.jito_ready,
                reason: "Tip accounts available (not used on Devnet)".into(),
            },
        ]
    }
    fn prepare(&self, request: OrderRequest) -> Result<PreparedOrder> {
        validate_mode(self.mode, request.mode)?;
        let risk = self.risk.read();
        ensure!(
            request.price > 0 && request.amount > 0 && request.slippage_bps < 10_000,
            "invalid quote"
        );
        ensure!(
            self.mode != Mode::Live || !request.buy || self.live_authorized.load(Ordering::Acquire),
            "LIVE_CONFIRMATION_REQUIRED"
        );
        let state = self.warm.read();
        ensure!(state.network_verified, "WRONG_NETWORK");
        ensure!(
            state
                .blockhash_at
                .is_some_and(|t| t.elapsed() < Duration::from_secs(15)),
            "BLOCKHASH_EXPIRED"
        );
        let market = state
            .markets
            .get(&request.mint)
            .context("market not warmed")?;
        ensure!(
            market.venue == request.venue,
            "market venue changed; refresh position before signing"
        );
        ensure!(
            market.refreshed.elapsed() < Duration::from_secs(15),
            "market cache stale"
        );
        let signer = self.signer.lock();
        let payer = signer.pubkey();
        let idl = Idl::load(market.venue);
        let mut ctx = market.context.clone();
        let units = 300_000u32;
        let max_micro = risk.max_priority_fee.saturating_mul(1_000_000) / units as u64;
        let micro = state.priority_micro_lamports.min(max_micro);
        let mut ix = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(units),
            ComputeBudgetInstruction::set_compute_unit_price(micro),
        ];
        let base_token = ctx
            .get("token_program")
            .or_else(|| ctx.get("base_token_program"))
            .copied()
            .context("token program")?;
        ix.push(protocol::create_ata(
            payer,
            payer,
            protocol::pubkey(request.mint),
            base_token,
        ));
        let qty = if request.buy {
            (request.amount as u128 * PRICE_SCALE as u128 * 10_000
                / (request.price as u128 * (10_000 + request.slippage_bps as u128)))
                .try_into()
                .context("token quote overflow")?
        } else {
            request.amount
        };
        ensure!(qty > 0, "quote amount too small");
        let limit = if request.buy {
            request.amount
        } else {
            (qty as u128 * request.price as u128 * (10_000 - request.slippage_bps as u128)
                / (PRICE_SCALE as u128 * 10_000)) as u64
        };
        if market.venue == Venue::PumpSwap {
            let quote = ctx["user_quote_token_account"];
            let token = ctx["quote_token_program"];
            ix.push(protocol::create_ata(payer, payer, ctx["quote_mint"], token));
            if request.buy {
                ix.push(system_instruction::transfer(&payer, &quote, limit));
                ix.push(Instruction {
                    program_id: token,
                    accounts: vec![AccountMeta::new(quote, false)],
                    data: vec![17],
                });
            }
        }
        ix.push(idl.build(
            if request.buy { "buy" } else { "sell" },
            &[qty, limit],
            &mut ctx,
        )?);
        if market.venue == Venue::PumpSwap && !request.buy {
            ix.push(Instruction {
                program_id: ctx["quote_token_program"],
                accounts: vec![
                    AccountMeta::new(ctx["user_quote_token_account"], false),
                    AccountMeta::new(payer, false),
                    AccountMeta::new_readonly(payer, true),
                ],
                data: vec![9],
            });
        }
        let tip = if self.mode == Mode::Live {
            state.tip.min(risk.max_jito_tip)
        } else {
            0
        };
        if self.mode == Mode::Live {
            ensure!(
                tip >= 1_000_000,
                "Sender Max requires a minimum 0.001 SOL tip"
            );
            ix.push(system_instruction::transfer(
                &payer,
                &state.tip_account.context("tip account unavailable")?,
                tip,
            ));
        }
        let tx = Transaction::new_signed_with_payer(
            &ix,
            Some(&payer),
            &[&*signer],
            state.blockhash.context("blockhash")?,
        );
        let wire = bincode::serialize(&tx)?;
        ensure!(wire.len() <= 1232, "transaction exceeds packet limit");
        Ok(PreparedOrder {
            signature: tx.signatures[0].to_string(),
            pool: ctx.get("pool").copied().map(protocol::key),
            request,
            wire,
            last_valid_block_height: state.last_valid_height,
            fee_budget: 5000 + micro * units as u64 / 1_000_000,
            tip,
        })
    }
    async fn submit(&self, order: &PreparedOrder) -> Result<()> {
        validate_mode(self.mode, order.request.mode)?;
        ensure!(
            self.mode != Mode::Live
                || !order.request.buy
                || self.live_authorized.load(Ordering::Acquire),
            "LIVE_CONFIRMATION_REQUIRED"
        );
        let params = json!([STANDARD.encode(&order.wire),{"encoding":"base64","skipPreflight":true,"maxRetries":0}]);
        if self.mode == Mode::Devnet {
            self.rpc.call("sendTransaction", params).await?;
        } else if self
            .sender
            .call("sendTransaction", params.clone())
            .await
            .is_err()
        {
            // Identical signed bytes; the two routes cannot create two distinct buys.
            self.jito
                .with_path("/api/v1/transactions")?
                .call("sendTransaction", params)
                .await?;
        }
        Ok(())
    }
    async fn reconcile(&self, order: &PreparedOrder) -> Result<Settlement> {
        validate_mode(self.mode, order.request.mode)?;
        let response=self.rpc.call("getTransaction",json!([order.signature,{"encoding":"json","commitment":"finalized","maxSupportedTransactionVersion":0}])).await?;
        decode_settlement(self.address, order, &response)
    }
}
fn decode_settlement(address: Key, order: &PreparedOrder, response: &Value) -> Result<Settlement> {
    if response.is_null() {
        return Ok(Settlement::Unknown);
    }
    let meta = &response["meta"];
    let fee = meta["fee"].as_u64().context("transaction fee")?;
    if !meta["err"].is_null() {
        return Ok(Settlement::ProvenFailed { fee });
    }
    let owner = address.to_string();
    let mint = order.request.mint.to_string();
    let token_sum = |field: &str, mint: &str| -> Result<u64> {
        let mut total = 0u64;
        for balance in meta[field].as_array().context("token balances")? {
            if balance["owner"] == owner && balance["mint"] == mint {
                total = total
                    .checked_add(
                        balance["uiTokenAmount"]["amount"]
                            .as_str()
                            .context("raw amount")?
                            .parse()?,
                    )
                    .context("balance overflow")?;
            }
        }
        Ok(total)
    };
    let pre = token_sum("preTokenBalances", &mint)?;
    let post = token_sum("postTokenBalances", &mint)?;
    let quantity = if order.request.buy {
        post.checked_sub(pre)
    } else {
        pre.checked_sub(post)
    }
    .context("unexpected token balance delta")?;
    ensure!(quantity > 0, "confirmed transaction lacks token fill");
    let keys = response["transaction"]["message"]["accountKeys"]
        .as_array()
        .context("account keys")?;
    let payer_index = keys
        .iter()
        .position(|k| k == &Value::String(owner.clone()))
        .context("wallet account")?;
    let native_pre = meta["preBalances"][payer_index]
        .as_u64()
        .context("pre balance")?;
    let native_post = meta["postBalances"][payer_index]
        .as_u64()
        .context("post balance")?;
    let wsol_pre = token_sum("preTokenBalances", protocol::WSOL)?;
    let wsol_post = token_sum("postTokenBalances", protocol::WSOL)?;
    // Remove fees/tip and account rent changes from native SOL balance movement.
    let payer = protocol::pubkey(address);
    let token = Pubkey::from_str(protocol::TOKEN)?;
    let wsol = protocol::ata(payer, Pubkey::from_str(protocol::WSOL)?, token).to_string();
    let owned_accounts = [
        protocol::ata(payer, protocol::pubkey(order.request.mint), token).to_string(),
        protocol::ata(
            payer,
            protocol::pubkey(order.request.mint),
            Pubkey::from_str(protocol::TOKEN_2022)?,
        )
        .to_string(),
        wsol.clone(),
    ];
    let principal = |field: &str, index: usize| -> Result<u64> {
        for balance in meta[field].as_array().context("token balances")? {
            if balance["accountIndex"].as_u64() == Some(index as u64)
                && balance["mint"] == protocol::WSOL
                && balance["owner"] == owner
            {
                return Ok(balance["uiTokenAmount"]["amount"]
                    .as_str()
                    .context("WSOL principal")?
                    .parse()?);
            }
        }
        Ok(0)
    };
    let mut rent_delta = 0i128;
    for (i, account) in keys.iter().enumerate() {
        if !account
            .as_str()
            .is_some_and(|key| owned_accounts.iter().any(|owned| key == owned))
        {
            continue;
        }
        let before = meta["preBalances"][i].as_u64().unwrap_or(0);
        let after = meta["postBalances"][i].as_u64().unwrap_or(0);
        if before == 0 && after > 0 {
            rent_delta += after as i128 - principal("postTokenBalances", i)? as i128;
        } else if before > 0 && after == 0 {
            rent_delta -= before as i128 - principal("preTokenBalances", i)? as i128;
        }
    }
    let delta = native_post as i128 - native_pre as i128 + wsol_post as i128 - wsol_pre as i128;
    let amount = if order.request.buy {
        -delta - fee as i128 - order.tip as i128 - rent_delta
    } else {
        delta + fee as i128 + order.tip as i128 + rent_delta
    };
    ensure!(
        amount > 0 && amount <= u64::MAX as i128,
        "ambiguous SOL delta; retain unresolved order"
    );
    Ok(Settlement::Filled(Fill {
        signature: order.signature.clone(),
        token_quantity: quantity,
        sol_amount: amount as u64,
        network_fee: fee.min(5000),
        priority_fee: fee.saturating_sub(5000),
        tip: order.tip,
        timestamp_ms: response["blockTime"]
            .as_u64()
            .unwrap_or(SystemClock.now_ms() / 1000)
            * 1000,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(mode: Mode, buy: bool) -> OrderRequest {
        OrderRequest {
            id: 1,
            mode,
            mint: Key([3; 32]),
            creator: Key([4; 32]),
            venue: Venue::PumpSwap,
            buy,
            amount: 50_000_000,
            price: 500_000_000_000,
            market_cap: 0,
            score: 100,
            strategy: StrategyConfig::default(),
            slippage_bps: 500,
            reason: None,
            created_ms: 1,
            detected_ms: 1,
        }
    }
    #[tokio::test]
    async fn mode_guards_and_live_confirmation_precede_transport() {
        for mode in [Mode::Paper, Mode::Replay] {
            let paper = PaperExecutor::new(mode, PaperConfig::default()).unwrap();
            assert!(paper.prepare(request(Mode::Live, true)).is_err());
        }
        let signer = Keypair::new();
        let rpc = Rpc::new("https://invalid.test".into()).unwrap();
        let network = NetworkExecutor::new(
            Mode::Live,
            &serde_json::to_string(&signer.to_bytes().to_vec()).unwrap(),
            rpc.clone(),
            rpc.clone(),
            rpc,
            RiskConfig::default(),
        )
        .unwrap();
        assert_eq!(
            network
                .prepare(request(Mode::Live, true))
                .unwrap_err()
                .to_string(),
            "LIVE_CONFIRMATION_REQUIRED"
        );
        let paper_order = PaperExecutor::new(Mode::Paper, PaperConfig::default())
            .unwrap()
            .prepare(request(Mode::Paper, true))
            .unwrap();
        assert!(network.submit(&paper_order).await.is_err());
        let risk = RiskConfig {
            min_balance: 2 * SOL,
            ..Default::default()
        };
        network.configure_risk(risk.clone());
        assert_eq!(network.risk.read().min_balance, risk.min_balance);
    }
    #[test]
    fn real_fill_removes_own_rent_and_tip_without_counting_unrelated_accounts() {
        let owner = Key([1; 32]);
        let payer = protocol::pubkey(owner);
        let token = Pubkey::from_str(protocol::TOKEN).unwrap();
        let base = protocol::ata(payer, protocol::pubkey(Key([3; 32])), token);
        let wsol = protocol::ata(payer, Pubkey::from_str(protocol::WSOL).unwrap(), token);
        let rent = 2_039_280u64;
        let mut order = PreparedOrder {
            request: request(Mode::Live, true),
            pool: None,
            signature: "fixture".into(),
            wire: vec![],
            last_valid_block_height: 0,
            fee_budget: 5000,
            tip: 1_000_000,
        };
        let balance = |index: u64, mint: &str, amount: u64| json!({"accountIndex":index,"mint":mint,"owner":owner.to_string(),"uiTokenAmount":{"amount":amount.to_string()}});
        let mut response = json!({"transaction":{"message":{"accountKeys":[owner.to_string(),base.to_string(),Key([5;32]).to_string(),wsol.to_string()]}},"meta":{"err":null,"fee":5000,"preBalances":[SOL,0,0,0],"postBalances":[SOL-50_000_000-order.tip-5000-rent,rent,order.tip,0],"preTokenBalances":[],"postTokenBalances":[balance(1,&order.request.mint.to_string(),100)]},"blockTime":1});
        let Settlement::Filled(fill) = decode_settlement(owner, &order, &response).unwrap() else {
            panic!("expected buy fill")
        };
        assert_eq!((fill.sol_amount, fill.token_quantity), (50_000_000, 100));
        order.request.buy = false;
        response["meta"]["preBalances"] = json!([200_000_000, rent, 0, 9_000_000 + rent]);
        response["meta"]["postBalances"] = json!([
            200_000_000 + 50_000_000 + 9_000_000 + rent - order.tip - 5000,
            rent,
            order.tip,
            0
        ]);
        response["meta"]["preTokenBalances"] = json!([
            balance(1, &order.request.mint.to_string(), 100),
            balance(3, protocol::WSOL, 9_000_000)
        ]);
        response["meta"]["postTokenBalances"] =
            json!([balance(1, &order.request.mint.to_string(), 0)]);
        let Settlement::Filled(fill) = decode_settlement(owner, &order, &response).unwrap() else {
            panic!("expected sell fill")
        };
        assert_eq!((fill.sol_amount, fill.token_quantity), (50_000_000, 100));
        response["meta"]["err"] = json!({"InstructionError":[1,"Custom"]});
        assert!(matches!(
            decode_settlement(owner, &order, &response).unwrap(),
            Settlement::ProvenFailed { fee: 5000 }
        ));
        assert!(matches!(
            decode_settlement(owner, &order, &Value::Null).unwrap(),
            Settlement::Unknown
        ));
    }
}
