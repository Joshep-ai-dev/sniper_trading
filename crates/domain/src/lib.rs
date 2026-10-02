//! Deterministic domain. No storage, transport, signing, or frontend dependencies.
use anyhow::{bail, ensure, Result};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

pub const SOL: u64 = 1_000_000_000;
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub [u8; 32]);
impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&bs58::encode(self.0).into_string())
    }
}
impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl FromStr for Key {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let bytes = bs58::decode(s).into_vec()?;
        ensure!(bytes.len() == 32, "public key must be 32 bytes");
        Ok(Self(bytes.try_into().expect("length checked")))
    }
}
impl Serialize for Key {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&self.to_string())
        } else {
            self.0.serialize(s)
        }
    }
}
impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        if d.is_human_readable() {
            String::deserialize(d)?
                .parse()
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Self(<[u8; 32]>::deserialize(d)?))
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    Replay,
    Paper,
    Devnet,
    Live,
}
impl Mode {
    pub fn byte(self) -> u8 {
        match self {
            Self::Replay => 0,
            Self::Paper => 1,
            Self::Devnet => 2,
            Self::Live => 3,
        }
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BotState {
    Starting,
    Running,
    Pausing,
    Paused,
    Stopping,
    Stopped,
    Degraded,
    Error,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PositionState {
    Opening,
    Open,
    SellTriggered,
    SellSubmitted,
    Closing,
    Closed,
    Failed,
    Unknown,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
    BotStop,
    ManualSell,
    DevSell,
    RiskEvent,
    MaxHoldTime,
    LiquidityFailure,
    MigrationFailure,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Venue {
    PumpFun,
    PumpSwap,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    Create,
    Buy,
    Sell,
    Migration,
    Price,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntryMode {
    Fast,
    Confirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StrategyConfig {
    pub version: u64,
    pub entry_mode: EntryMode,
    pub confirmation_ms: u64,
    pub take_profit_bps: u32,
    pub stop_loss_bps: u32,
    pub min_score: u8,
    pub min_creator_score: u8,
    pub max_dev_allocation_bps: u16,
    pub max_concentration_bps: u16,
    pub notes: String,
}
impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            version: 1,
            entry_mode: EntryMode::Fast,
            confirmation_ms: 150,
            take_profit_bps: 6000,
            stop_loss_bps: 3000,
            min_score: 75,
            min_creator_score: 15,
            max_dev_allocation_bps: 2000,
            max_concentration_bps: 4000,
            notes: String::new(),
        }
    }
}
impl StrategyConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (100..=300).contains(&self.confirmation_ms),
            "confirmation window must be 100–300 ms"
        );
        ensure!(
            self.take_profit_bps > 0 && self.take_profit_bps <= 100_000,
            "invalid take profit"
        );
        ensure!(
            self.stop_loss_bps > 0 && self.stop_loss_bps < 10_000,
            "stop loss must be between 0 and 100%"
        );
        ensure!(
            self.min_score <= 100 && self.min_creator_score <= 30,
            "invalid score threshold"
        );
        ensure!(
            self.max_dev_allocation_bps <= 10_000 && self.max_concentration_bps <= 10_000,
            "invalid allocation limit"
        );
        ensure!(self.notes.len() <= 1024, "strategy notes too long");
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    pub sol_per_trade: u64,
    pub max_positions: usize,
    pub max_exposure: u64,
    pub max_daily_loss: u64,
    pub max_daily_trades: u32,
    pub slippage_bps: u16,
    pub max_priority_fee: u64,
    pub max_jito_tip: u64,
    pub min_balance: u64,
    pub max_hold_ms: u64,
    pub max_pending: usize,
    pub max_failed_submissions: u32,
    pub max_consecutive_losses: u32,
}
impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            sol_per_trade: 50_000_000,
            max_positions: 4,
            max_exposure: 200_000_000,
            max_daily_loss: 100_000_000,
            max_daily_trades: 50,
            slippage_bps: 500,
            max_priority_fee: 500_000,
            max_jito_tip: 2_000_000,
            min_balance: 100_000_000,
            max_hold_ms: 300_000,
            max_pending: 4,
            max_failed_submissions: 5,
            max_consecutive_losses: 5,
        }
    }
}
impl RiskConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.sol_per_trade > 0 && self.sol_per_trade <= self.max_exposure,
            "invalid position size"
        );
        ensure!(
            (1..=1000).contains(&self.max_positions) && (1..=128).contains(&self.max_pending),
            "invalid position or queue limit"
        );
        ensure!(
            self.slippage_bps > 0 && self.slippage_bps <= 2000,
            "slippage must be 0–20%"
        );
        ensure!(
            self.max_daily_loss > 0 && self.max_daily_trades > 0 && self.max_hold_ms > 0,
            "risk limits must be positive"
        );
        ensure!(
            self.max_failed_submissions > 0 && self.max_consecutive_losses > 0,
            "failure limits must be positive"
        );
        ensure!(
            self.max_exposure < i64::MAX as u64 && self.max_daily_loss < i64::MAX as u64,
            "risk limit too large"
        );
        ensure!(
            self.min_balance as u128
                + self.max_priority_fee as u128
                + self.max_jito_tip as u128
                + self.max_exposure as u128
                + 5_000_000
                < i64::MAX as u128,
            "combined risk budget too large"
        );
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PaperConfig {
    pub starting_balance: u64,
    pub network_delay_ms: u64,
    pub landing_delay_ms: u64,
    pub slippage_bps: u16,
    pub fee: u64,
    pub tip: u64,
    pub failure_bps: u16,
    pub seed: u64,
}
impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            starting_balance: 5 * SOL,
            network_delay_ms: 15,
            landing_delay_ms: 200,
            slippage_bps: 100,
            fee: 5000,
            tip: 1_000_000,
            failure_bps: 0,
            seed: 42,
        }
    }
}

/// An integer price is lamports per 1,000,000 raw token units.
pub const PRICE_SCALE: u64 = 1_000_000;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub signature: String,
    pub slot: u64,
    pub instruction_index: u16,
    pub mint: Key,
    pub creator: Key,
    pub venue: Venue,
    pub kind: EventKind,
    pub observed_ms: u64,
    pub blockchain_ms: Option<u64>,
    pub price: u64,
    pub market_cap: u64,
    pub trader: Option<Key>,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub speculative: bool,
    pub facts: Option<TokenFacts>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenFacts {
    pub mint_authority: bool,
    pub freeze_authority: bool,
    pub dev_allocation_bps: u16,
    pub concentration_bps: u16,
    pub unique_buyers: u16,
    pub buys: u16,
    pub sells: u16,
    pub creator_score: u8,
    pub suspicious: bool,
    pub verified: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Score {
    pub total: u8,
    pub creator: u8,
    pub structure: u8,
    pub buyers: u8,
    pub momentum: u8,
    pub distribution: u8,
    pub reasons: Vec<String>,
    pub approved: bool,
}
pub fn score(f: &TokenFacts, c: &StrategyConfig) -> Score {
    let creator = f.creator_score.min(30);
    let structure = if f.verified && !f.mint_authority && !f.freeze_authority {
        20
    } else {
        0
    };
    let buyers = (f.unique_buyers.min(10) * 2) as u8;
    let momentum = if f.buys == 0 {
        0
    } else {
        ((f.buys as u32 * 20) / (f.buys as u32 + f.sells as u32)) as u8
    };
    let distribution = 10_u8.saturating_sub((f.concentration_bps / 1000).min(10) as u8);
    let total = creator + structure + buyers + momentum + distribution;
    let mut reasons = Vec::new();
    if !f.verified {
        reasons.push("TOKEN_FACTS_UNVERIFIED".into());
    }
    if f.mint_authority || f.freeze_authority {
        reasons.push("UNSAFE_AUTHORITY".into());
    }
    if f.suspicious {
        reasons.push("SUSPICIOUS_WALLET".into());
    }
    if creator < c.min_creator_score {
        reasons.push("CREATOR_SCORE".into());
    }
    if f.dev_allocation_bps > c.max_dev_allocation_bps {
        reasons.push("DEV_ALLOCATION".into());
    }
    if f.concentration_bps > c.max_concentration_bps {
        reasons.push("WALLET_CONCENTRATION".into());
    }
    if total < c.min_score {
        reasons.push("MINIMUM_SCORE".into());
    }
    let approved = reasons.is_empty();
    Score {
        total,
        creator,
        structure,
        buyers,
        momentum,
        distribution,
        reasons,
        approved,
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub id: u64,
    pub mode: Mode,
    pub mint: Key,
    pub creator: Key,
    pub venue: Venue,
    pub buy: bool,
    pub amount: u64,
    pub price: u64,
    pub market_cap: u64,
    pub score: u8,
    pub strategy: StrategyConfig,
    pub slippage_bps: u16,
    pub reason: Option<ExitReason>,
    pub created_ms: u64,
    pub detected_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedOrder {
    pub request: OrderRequest,
    pub pool: Option<Key>,
    pub signature: String,
    pub wire: Vec<u8>,
    pub last_valid_block_height: u64,
    pub fee_budget: u64,
    pub tip: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub signature: String,
    pub token_quantity: u64,
    pub sol_amount: u64,
    pub network_fee: u64,
    pub priority_fee: u64,
    pub tip: u64,
    pub timestamp_ms: u64,
}
#[derive(Debug, Clone)]
pub enum Settlement {
    Filled(Fill),
    ProvenFailed { fee: u64 },
    Unknown,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reservation {
    pub mint: Key,
    pub mode: Mode,
    pub order_id: u64,
    pub status: String,
    pub created_ms: u64,
    pub signature: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurchasedToken {
    pub mint: Key,
    pub creator: Key,
    pub first_detected_ms: u64,
    pub first_submitted_ms: u64,
    pub confirmed_ms: u64,
    pub signature: String,
    pub entry_price: u64,
    pub entry_market_cap: u64,
    pub strategy_version: u64,
    pub exit_signature: Option<String>,
    pub exit_price: Option<u64>,
    pub final_pnl: Option<i64>,
    pub updated_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub mint: Key,
    pub pool: Option<Key>,
    pub creator: Key,
    pub mode: Mode,
    pub venue: Venue,
    pub state: PositionState,
    pub entry_signature: String,
    pub entry_ms: u64,
    pub detected_ms: u64,
    pub quantity: u64,
    pub spent: u64,
    pub entry_price: u64,
    pub current_price: u64,
    pub entry_market_cap: u64,
    pub current_market_cap: u64,
    pub entry_fees: u64,
    pub score: u8,
    pub strategy: StrategyConfig,
    pub exit_order: Option<u64>,
    pub exit_reason: Option<ExitReason>,
}
impl Position {
    pub fn value(&self) -> u64 {
        ((self.quantity as u128 * self.current_price as u128) / PRICE_SCALE as u128)
            .min(u64::MAX as u128) as u64
    }
    pub fn pnl(&self) -> i64 {
        (self.value() as i128 - self.spent as i128 - self.entry_fees as i128)
            .clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }
    pub fn exit_trigger(&self, now: u64, max_hold: u64) -> Option<ExitReason> {
        if self.state != PositionState::Open {
            return None;
        }
        let value = self.value() as u128;
        if value * 10_000 >= self.spent as u128 * (10_000 + self.strategy.take_profit_bps as u128) {
            Some(ExitReason::TakeProfit)
        } else if value * 10_000
            <= self.spent as u128 * (10_000 - self.strategy.stop_loss_bps as u128)
        {
            Some(ExitReason::StopLoss)
        } else if now.saturating_sub(self.entry_ms) >= max_hold {
            Some(ExitReason::MaxHoldTime)
        } else {
            None
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub position: Position,
    pub exit: Fill,
    pub reason: ExitReason,
    pub net_pnl: i64,
    pub gross_pnl: i64,
    pub hold_ms: u64,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Aggregate {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub net_pnl: i64,
    pub gross_pnl: i64,
    pub fees: u64,
    pub hold_ms: u64,
    pub score_sum: u64,
    pub tp: u64,
    pub sl: u64,
}
impl Aggregate {
    pub fn add(&mut self, t: &ClosedTrade) {
        self.trades += 1;
        self.wins += u64::from(t.net_pnl > 0);
        self.losses += u64::from(t.net_pnl <= 0);
        self.net_pnl = self.net_pnl.saturating_add(t.net_pnl);
        self.gross_pnl = self.gross_pnl.saturating_add(t.gross_pnl);
        self.fees = self.fees.saturating_add(
            t.position.entry_fees + t.exit.network_fee + t.exit.priority_fee + t.exit.tip,
        );
        self.hold_ms += t.hold_ms;
        self.score_sum += t.position.score as u64;
        self.tp += u64::from(t.reason == ExitReason::TakeProfit);
        self.sl += u64::from(t.reason == ExitReason::StopLoss);
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Audit {
    pub timestamp_ms: u64,
    pub mode: Mode,
    pub event: String,
    pub result: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightCheck {
    pub name: String,
    pub pass: bool,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub name: String,
    pub status: String,
    pub region: String,
    pub latency_ms: Option<u64>,
    pub slot: Option<u64>,
    pub network: Option<String>,
    pub last_success_ms: Option<u64>,
    pub error: Option<String>,
}
impl ServiceStatus {
    pub fn missing(name: &str) -> Self {
        Self {
            name: name.into(),
            status: "NOT_CONFIGURED".into(),
            region: String::new(),
            latency_ms: None,
            slot: None,
            network: None,
            last_success_ms: None,
            error: None,
        }
    }
}

#[async_trait]
pub trait TradeExecutor: Send + Sync {
    fn mode(&self) -> Mode;
    /// Must use warmed RAM state only; no network calls or filesystem reads.
    fn prepare(&self, request: OrderRequest) -> Result<PreparedOrder>;
    async fn submit(&self, order: &PreparedOrder) -> Result<()>;
    async fn reconcile(&self, order: &PreparedOrder) -> Result<Settlement>;
    fn ready(&self) -> Vec<PreflightCheck>;
    fn authorize_live(&self, _enabled: bool) {}
    fn configure_risk(&self, _risk: RiskConfig) {}
    fn market_route(&self, _mint: Key) -> Option<Key> {
        None
    }
    fn wallet_balance(&self) -> Option<u64> {
        None
    }
    async fn validate_position(&self, _position: &Position) -> Result<()> {
        Ok(())
    }
}
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}
pub struct ReplayClock(pub std::sync::atomic::AtomicU64);
impl ReplayClock {
    pub fn set(&self, time: u64) {
        self.0.store(time, std::sync::atomic::Ordering::Release);
    }
}
impl Clock for ReplayClock {
    fn now_ms(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}
pub trait TokenAnalyzer: Send + Sync {
    fn analyze(&self, event: &MarketEvent) -> TokenFacts;
}
pub trait Strategy: Send + Sync {
    fn decide(&self, facts: &TokenFacts, config: &StrategyConfig) -> Score;
}
pub struct LocalStrategy;
impl Strategy for LocalStrategy {
    fn decide(&self, f: &TokenFacts, c: &StrategyConfig) -> Score {
        score(f, c)
    }
}
pub fn validate_mode(executor: Mode, request: Mode) -> Result<()> {
    if executor != request {
        bail!("EXECUTION_MODE_MISMATCH");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    #[test]
    fn defaults_and_guards() {
        assert_eq!(StrategyConfig::default().take_profit_bps, 6000);
        assert_eq!(StrategyConfig::default().stop_loss_bps, 3000);
        assert!(validate_mode(Mode::Live, Mode::Paper).is_err());
    }
    #[test]
    fn unverified_rejected() {
        assert!(!score(&TokenFacts::default(), &StrategyConfig::default()).approved);
    }
    proptest! { #[test] fn scores_bounded(b in any::<u16>(),s in any::<u16>(),c in any::<u8>(),n in any::<u16>()) {
        let f=TokenFacts {buys:b,sells:s,creator_score:c,unique_buyers:n,..Default::default()};
        prop_assert!(score(&f,&StrategyConfig::default()).total<=100);
    } }
}
