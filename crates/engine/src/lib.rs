//! One actor owns trading state. Network tasks return through a bounded completion queue.
pub mod metrics;
pub mod replay;
use anyhow::{ensure, Context, Result};
use metrics::Metrics;
use serde::{Deserialize, Serialize};
use sniper_connectors::feed::Dedup;
use sniper_domain::*;
use sniper_store::{mint_key, order_key, time_key, DbHealth, Mutation, Store};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRecord {
    pub state: BotState,
    pub balance: u64,
    pub next_id: u64,
    pub day: u64,
    pub daily_trades: u32,
    pub daily_pnl: i64,
    pub failed_submissions: u32,
    pub consecutive_losses: u32,
}
impl RuntimeRecord {
    fn new(balance: u64, now: u64) -> Self {
        Self {
            state: BotState::Paused,
            balance,
            next_id: 1,
            day: now / 86_400_000,
            daily_trades: 0,
            daily_pnl: 0,
            failed_submissions: 0,
            consecutive_losses: 0,
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct Detection {
    pub event: MarketEvent,
    pub score: Option<Score>,
    pub decision: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub sequence: u64,
    pub mode: Mode,
    pub state: BotState,
    pub balance: u64,
    pub exposure: u64,
    pub daily_pnl: i64,
    pub positions: Vec<Position>,
    pub pending: Vec<Reservation>,
    pub feed: Vec<Detection>,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub services: Vec<ServiceStatus>,
    pub preflight: Vec<PreflightCheck>,
    pub database: Option<DbHealth>,
    pub error: Option<String>,
    pub market_queue: usize,
    pub uptime_secs: u64,
    pub aggregate: Aggregate,
    pub unresolved_orders: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Action {
    Start { confirmation: Option<String> },
    Pause,
    Stop,
    SetMode { mode: Mode },
    Strategy { config: StrategyConfig },
    Risk { config: RiskConfig },
}
struct Control {
    action: Action,
    reply: oneshot::Sender<std::result::Result<(), String>>,
}
struct Sell {
    mint: Key,
    reply: oneshot::Sender<std::result::Result<(), String>>,
}
struct Completed {
    id: u64,
    settlement: Settlement,
    elapsed_us: u64,
}
struct MarketInput {
    event: MarketEvent,
    reply: Option<oneshot::Sender<()>>,
}
#[derive(Clone)]
pub struct EngineHandle {
    control: mpsc::Sender<Control>,
    sell: mpsc::Sender<Sell>,
    market: mpsc::Sender<MarketInput>,
    services: mpsc::Sender<ServiceStatus>,
    snapshot: watch::Receiver<Arc<Snapshot>>,
    pub updates: broadcast::Sender<Arc<Snapshot>>,
    gate: Arc<AtomicBool>,
    pub metrics: Arc<Metrics>,
    lifecycle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}
impl EngineHandle {
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.borrow().clone()
    }
    pub async fn action(&self, action: Action) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.control.send(Control { action, reply: tx }).await?;
        rx.await?.map_err(anyhow::Error::msg)
    }
    pub async fn sell(&self, mint: Key) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sell.send(Sell { mint, reply: tx }).await?;
        rx.await?.map_err(anyhow::Error::msg)
    }
    pub async fn event(&self, event: MarketEvent) -> Result<()> {
        if self.market.capacity() < 32 {
            self.gate.store(false, Ordering::Release);
            self.metrics.saturation.inc();
        }
        self.market
            .send(MarketInput { event, reply: None })
            .await
            .context("market queue closed")
    }
    pub async fn replay_event(&self, event: MarketEvent) -> Result<()> {
        ensure!(
            self.snapshot().mode == Mode::Replay,
            "replay requires REPLAY mode"
        );
        let (tx, rx) = oneshot::channel();
        self.market
            .send(MarketInput {
                event,
                reply: Some(tx),
            })
            .await?;
        rx.await?;
        Ok(())
    }
    pub async fn service(&self, status: ServiceStatus) -> Result<()> {
        self.services.send(status).await?;
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<()> {
        // STOPPING must survive graceful shutdown, including unfinished liquidation.
        if self.snapshot().state != BotState::Stopping {
            let _ = self.action(Action::Pause).await;
        }
        self.gate.store(false, Ordering::Release);
        if let Some(task) = self.lifecycle.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
}
pub struct Engine {
    store: Arc<Store>,
    clock: Arc<dyn Clock>,
    executors: HashMap<Mode, Arc<dyn TradeExecutor>>,
    mode: Mode,
    runtime: RuntimeRecord,
    strategy: StrategyConfig,
    risk: RiskConfig,
    purchased: HashMap<Key, PurchasedToken>,
    pending: HashMap<Key, Reservation>,
    positions: HashMap<Key, Position>,
    orders: HashMap<u64, PreparedOrder>,
    active_jobs: HashSet<u64>,
    last_job: HashMap<u64, Instant>,
    facts: HashMap<Key, TokenFacts>,
    candidates: HashMap<Key, MarketEvent>,
    observations: HashMap<Key, (HashSet<Key>, u16, u16)>,
    prices: HashMap<Key, u64>,
    feed: VecDeque<Detection>,
    services: HashMap<String, ServiceStatus>,
    dedup: Dedup,
    aggregate: Aggregate,
    error: Option<String>,
    database: Option<DbHealth>,
    health: mpsc::Receiver<std::result::Result<DbHealth, String>>,
    sequence: u64,
    started: Instant,
    gate: Arc<AtomicBool>,
    completion: mpsc::Sender<Completed>,
    metrics: Arc<Metrics>,
    event_writer: mpsc::Sender<(Mode, MarketEvent)>,
}
impl Engine {
    pub async fn launch(
        store: Arc<Store>,
        mode: Mode,
        strategy: StrategyConfig,
        risk: RiskConfig,
        balance: u64,
        executors: HashMap<Mode, Arc<dyn TradeExecutor>>,
        clock: Arc<dyn Clock>,
    ) -> Result<EngineHandle> {
        strategy.validate()?;
        risk.validate()?;
        ensure!(executors.contains_key(&mode), "executor not configured");
        let load_store = store.clone();
        let loaded = tokio::task::spawn_blocking(move || -> Result<_> {
            for other in [Mode::Live, Mode::Devnet] {
                if other != mode {
                    ensure!(
                        load_store.load::<Position>("positions", other)?.is_empty()
                            && load_store
                                .load::<PreparedOrder>("orders", other)?
                                .is_empty(),
                        "restart in mode with unresolved real exposure"
                    );
                }
            }
            Ok((
                load_store.load::<PurchasedToken>("purchased_tokens", mode)?,
                load_store.load::<Reservation>("purchase_reservations", mode)?,
                load_store.load::<Position>("positions", mode)?,
                load_store.load::<PreparedOrder>("orders", mode)?,
                load_store.get::<RuntimeRecord>("bot_state", &[mode.byte()])?,
                load_store.get::<StrategyConfig>("config", &[mode.byte(), 0])?,
                load_store.get::<RiskConfig>("config", &[mode.byte(), 1])?,
            ))
        })
        .await??;
        let (purchased, mut pending, positions, orders, saved, saved_strategy, saved_risk) = loaded;
        let unsigned: Vec<_> = pending
            .iter()
            .filter(|r| r.signature.is_none() && !orders.iter().any(|o| o.request.id == r.order_id))
            .map(|r| r.mint)
            .collect();
        if !unsigned.is_empty() {
            store
                .write(
                    unsigned
                        .iter()
                        .map(|mint| {
                            Mutation::Delete("purchase_reservations", mint_key(mode, *mint))
                        })
                        .collect(),
                )
                .await?;
            pending.retain(|r| !unsigned.contains(&r.mint));
        }
        let strategy = saved_strategy.unwrap_or(strategy);
        let risk = saved_risk.unwrap_or(risk);
        strategy.validate()?;
        risk.validate()?;
        let mut runtime = saved.unwrap_or(RuntimeRecord::new(balance, clock.now_ms()));
        executors[&mode].configure_risk(risk.clone());
        runtime.state = if runtime.state == BotState::Stopping {
            BotState::Stopping
        } else {
            BotState::Paused
        };
        runtime.next_id = runtime.next_id.max(
            orders
                .iter()
                .map(|o| o.request.id.saturating_add(1))
                .max()
                .unwrap_or(1),
        );
        if matches!(mode, Mode::Live | Mode::Devnet) {
            runtime.balance = balance;
            for position in &positions {
                if !orders
                    .iter()
                    .any(|o| !o.request.buy && o.request.mint == position.mint)
                {
                    executors[&mode].validate_position(position).await?;
                }
            }
        }
        let (controls, control_rx) = mpsc::channel(64);
        let (sells, sell_rx) = mpsc::channel(128);
        let (market, market_rx) = mpsc::channel(2048);
        let (services, service_rx) = mpsc::channel(64);
        let (completion, completion_rx) = mpsc::channel(128);
        let (event_writer, mut event_rx) = mpsc::channel::<(Mode, MarketEvent)>(4096);
        let metrics = Arc::new(Metrics::new()?);
        let history_store = store.clone();
        let history_metrics = metrics.clone();
        tokio::spawn(async move {
            while let Some((mode, event)) = event_rx.recv().await {
                if let Ok(mutation) = Mutation::put(
                    "market_events",
                    time_key(
                        mode,
                        event.observed_ms,
                        event.slot.wrapping_mul(65536) + event.instruction_index as u64,
                    ),
                    &event,
                ) {
                    if history_store.write(vec![mutation]).await.is_err() {
                        history_metrics.failures.inc();
                    }
                }
            }
        });
        let aggregate_store = store.clone();
        let store_aggregate = tokio::task::spawn_blocking(move || {
            aggregate_store.get::<Aggregate>("config", &[mode.byte(), 2])
        })
        .await??
        .unwrap_or_default();
        let (health_tx, health_rx) = mpsc::channel(2);
        let health_store = Arc::downgrade(&store);
        tokio::spawn(async move {
            loop {
                let Some(store) = health_store.upgrade() else {
                    break;
                };
                let health = tokio::task::spawn_blocking(move || store.health())
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|r| r.map_err(|e| e.to_string()));
                if health_tx.send(health).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        let gate = Arc::new(AtomicBool::new(false));
        let engine = Self {
            store,
            clock,
            executors,
            mode,
            runtime,
            strategy,
            risk,
            purchased: purchased.into_iter().map(|p| (p.mint, p)).collect(),
            pending: pending
                .into_iter()
                .map(|mut r| {
                    r.status = "UNKNOWN_PENDING".into();
                    (r.mint, r)
                })
                .collect(),
            positions: positions.into_iter().map(|p| (p.mint, p)).collect(),
            orders: orders.into_iter().map(|o| (o.request.id, o)).collect(),
            active_jobs: HashSet::new(),
            last_job: HashMap::new(),
            facts: HashMap::new(),
            candidates: HashMap::new(),
            observations: HashMap::new(),
            prices: HashMap::new(),
            feed: VecDeque::new(),
            services: HashMap::new(),
            dedup: Dedup::new(100_000, 30_000),
            aggregate: store_aggregate,
            error: None,
            database: None,
            health: health_rx,
            sequence: 0,
            started: Instant::now(),
            gate: gate.clone(),
            completion,
            metrics: metrics.clone(),
            event_writer,
        };
        let (snapshot_tx, snapshot) = watch::channel(Arc::new(engine.snapshot()));
        let (updates, _) = broadcast::channel(64);
        let lifecycle = Arc::new(tokio::sync::Mutex::new(None));
        let handle = EngineHandle {
            control: controls,
            sell: sells,
            market,
            services,
            snapshot,
            updates: updates.clone(),
            gate,
            metrics,
            lifecycle: lifecycle.clone(),
        };
        engine
            .persist_runtime("RECOVERY", "Critical state loaded; new entries paused")
            .await?;
        *lifecycle.lock().await = Some(tokio::spawn(engine.run(
            control_rx,
            sell_rx,
            market_rx,
            service_rx,
            completion_rx,
            (snapshot_tx, updates),
        )));
        Ok(handle)
    }
    async fn run(
        mut self,
        mut controls: mpsc::Receiver<Control>,
        mut sells: mpsc::Receiver<Sell>,
        mut markets: mpsc::Receiver<MarketInput>,
        mut services: mpsc::Receiver<ServiceStatus>,
        mut completions: mpsc::Receiver<Completed>,
        publishers: (
            watch::Sender<Arc<Snapshot>>,
            broadcast::Sender<Arc<Snapshot>>,
        ),
    ) {
        let (snapshots, updates) = publishers;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {biased;
                Some(done)=completions.recv()=>{if let Err(e)=self.complete(done).await {self.degrade(e);}},
                Some(sell)=sells.recv()=>{let result=self.exit(sell.mint,ExitReason::ManualSell).await.map_err(|e|e.to_string());let _=sell.reply.send(result);},
                Some(control)=controls.recv()=>{let result=self.control(control.action).await.map_err(|e|e.to_string());if !self.store.writable(){self.degrade(anyhow::anyhow!("LOCAL_PERSISTENCE_UNAVAILABLE"));}let _=control.reply.send(result);},
                Some(health)=self.health.recv()=>{match health {Ok(health)=>self.database=Some(health),Err(error)=>self.degrade(anyhow::anyhow!(error))}},
                _=tick.tick()=>{if let Err(e)=self.maintenance().await {self.degrade(e);}self.sequence+=1;let mut value=self.snapshot();value.market_queue=markets.len();let value=Arc::new(value);snapshots.send_replace(value.clone());let _=updates.send(value);},
                Some(status)=services.recv()=>{self.services.insert(status.name.clone(),status);},
                Some(input)=markets.recv()=>{
                    if let Err(e)=self.market(input.event).await {self.degrade(e);}
                    if let Some(reply)=input.reply {
                        while !self.active_jobs.is_empty(){
                            if let Some(done)=completions.recv().await {
                                if let Err(e)=self.complete(done).await{self.degrade(e);}
                            }else{break;}
                        }
                        self.sequence+=1;
                        snapshots.send_replace(Arc::new(self.snapshot()));
                        let _=reply.send(());
                    }
                },else=>break,
            }
        }
    }
    fn degrade(&mut self, error: anyhow::Error) {
        self.gate.store(false, Ordering::Release);
        if self.runtime.state != BotState::Stopping {
            self.runtime.state = BotState::Degraded;
        }
        self.error = Some(error.to_string());
        self.metrics.failures.inc();
    }
    fn preflight(&self) -> Vec<PreflightCheck> {
        let mut list = vec![
            PreflightCheck {
                name: "RocksDB".into(),
                pass: self.store.writable()
                    && self.database.as_ref().is_some_and(|d| d.status == "READY"),
                reason: "Synchronous WAL and local disk health required".into(),
            },
            PreflightCheck {
                name: "Recovery".into(),
                pass: self.orders.is_empty() && self.pending.is_empty(),
                reason: "No unresolved transaction or reservation".into(),
            },
            PreflightCheck {
                name: "Ownership".into(),
                pass: true,
                reason: "Database and wallet OS locks held".into(),
            },
            PreflightCheck {
                name: "Strategy".into(),
                pass: self.strategy.validate().is_ok(),
                reason: format!("Version {}", self.strategy.version),
            },
            PreflightCheck {
                name: "Risk limits".into(),
                pass: self.risk.validate().is_ok(),
                reason: "Backend enforces limits".into(),
            },
            PreflightCheck {
                name: "Buy registry".into(),
                pass: true,
                reason: format!("{} permanent mint records loaded", self.purchased.len()),
            },
            PreflightCheck {
                name: "Position protection".into(),
                pass: self.positions.is_empty()
                    || !self.services.values().any(|s| {
                        s.name == format!("{:?} position protection", self.mode)
                            && s.status == "FAILED"
                    }),
                reason: "Open market refreshes must be healthy".into(),
            },
        ];
        if self.mode != Mode::Replay {
            list.push(PreflightCheck {
                name: "Helius feed".into(),
                pass: self
                    .services
                    .values()
                    .any(|s| s.name == "Helius Confirmed" && s.status == "CONNECTED"),
                reason: "Confirmed stream required for position protection".into(),
            });
        }
        if let Some(executor) = self.executors.get(&self.mode) {
            list.extend(executor.ready());
        }
        list
    }
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            sequence: self.sequence,
            mode: self.mode,
            state: self.runtime.state,
            balance: self.runtime.balance,
            exposure: self.positions.values().map(|p| p.spent).sum(),
            daily_pnl: self.runtime.daily_pnl,
            positions: self.positions.values().cloned().collect(),
            pending: self.pending.values().cloned().collect(),
            feed: self.feed.iter().rev().cloned().collect(),
            strategy: self.strategy.clone(),
            risk: self.risk.clone(),
            services: self.services.values().cloned().collect(),
            preflight: self.preflight(),
            database: self.database.clone(),
            error: self.error.clone(),
            market_queue: 0,
            uptime_secs: self.started.elapsed().as_secs(),
            aggregate: self.aggregate.clone(),
            unresolved_orders: self.orders.len(),
        }
    }
    async fn persist_runtime(&self, event: &str, result: &str) -> Result<()> {
        let now = self.clock.now_ms();
        self.store
            .write(vec![
                Mutation::put("bot_state", vec![self.mode.byte()], &self.runtime)?,
                Mutation::put(
                    "audit_history",
                    time_key(self.mode, now, self.runtime.next_id),
                    &Audit {
                        timestamp_ms: now,
                        mode: self.mode,
                        event: event.into(),
                        result: result.into(),
                    },
                )?,
            ])
            .await
    }
    async fn control(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Start { confirmation } => {
                ensure!(
                    self.runtime.state != BotState::Stopping,
                    "STOPPING cannot start"
                );
                ensure!(self.preflight().iter().all(|c| c.pass), "PREFLIGHT_FAILED");
                if self.mode == Mode::Live {
                    ensure!(
                        confirmation.as_deref() == Some("ENABLE LIVE MAINNET TRADING"),
                        "LIVE_CONFIRMATION_REQUIRED"
                    );
                }
                self.runtime.state = BotState::Running;
                self.persist_runtime("START", "User started selected mode")
                    .await?;
                self.executors[&self.mode].authorize_live(self.mode == Mode::Live);
                self.error = None;
                self.gate.store(true, Ordering::Release);
            }
            Action::Pause => {
                ensure!(
                    self.runtime.state != BotState::Stopping,
                    "STOPPING must finish liquidation before pause"
                );
                self.gate.store(false, Ordering::Release);
                self.runtime.state = BotState::Paused;
                self.persist_runtime("PAUSE", "Exits remain active").await?;
            }
            Action::Stop => {
                self.gate.store(false, Ordering::Release);
                self.runtime.state = BotState::Stopping;
                if let Err(e) = self
                    .persist_runtime("STOP_AND_SELL_ALL", "Liquidation requested")
                    .await
                {
                    self.degrade(e);
                }
                let mints: Vec<_> = self.positions.keys().copied().collect();
                for mint in mints {
                    if let Err(e) = self.exit(mint, ExitReason::BotStop).await {
                        self.error = Some(e.to_string());
                    }
                }
                self.maybe_stopped().await?;
            }
            Action::SetMode { mode } => {
                ensure!(
                    mode != Mode::Replay && self.mode != Mode::Replay,
                    "restart with REPLAY configured to select the deterministic replay clock"
                );
                ensure!(
                    self.positions.is_empty() && self.orders.is_empty() && self.pending.is_empty(),
                    "cannot change mode with open exposure"
                );
                ensure!(
                    matches!(self.runtime.state, BotState::Paused | BotState::Stopped),
                    "pause before changing mode"
                );
                ensure!(
                    self.executors.contains_key(&mode),
                    "mode executor not configured"
                );
                self.gate.store(false, Ordering::Release);
                self.executors[&self.mode].authorize_live(false);
                let store = self.store.clone();
                let (purchased, runtime, strategy, risk, aggregate) =
                    tokio::task::spawn_blocking(move || -> Result<_> {
                        ensure!(
                            store.load::<Position>("positions", mode)?.is_empty()
                                && store.load::<PreparedOrder>("orders", mode)?.is_empty()
                                && store
                                    .load::<Reservation>("purchase_reservations", mode)?
                                    .is_empty(),
                            "restart to recover selected mode exposure"
                        );
                        Ok((
                            store.load::<PurchasedToken>("purchased_tokens", mode)?,
                            store.get::<RuntimeRecord>("bot_state", &[mode.byte()])?,
                            store.get::<StrategyConfig>("config", &[mode.byte(), 0])?,
                            store.get::<RiskConfig>("config", &[mode.byte(), 1])?,
                            store.get::<Aggregate>("config", &[mode.byte(), 2])?,
                        ))
                    })
                    .await??;
                let strategy = strategy.unwrap_or_else(|| self.strategy.clone());
                let risk = risk.unwrap_or_else(|| self.risk.clone());
                strategy.validate()?;
                risk.validate()?;
                let mut runtime = runtime.unwrap_or(RuntimeRecord::new(
                    if matches!(mode, Mode::Paper | Mode::Replay) {
                        5 * SOL
                    } else {
                        0
                    },
                    self.clock.now_ms(),
                ));
                runtime.state = BotState::Paused;
                if matches!(mode, Mode::Live | Mode::Devnet) {
                    runtime.balance = self.executors[&mode]
                        .wallet_balance()
                        .context("verified real wallet balance unavailable")?;
                }
                self.mode = mode;
                self.runtime = runtime;
                self.strategy = strategy;
                self.risk = risk;
                self.executors[&mode].configure_risk(self.risk.clone());
                self.purchased = purchased.into_iter().map(|p| (p.mint, p)).collect();
                self.feed.clear();
                self.candidates.clear();
                self.facts.clear();
                self.observations.clear();
                self.prices.clear();
                self.dedup = Dedup::new(100_000, 30_000);
                self.aggregate = aggregate.unwrap_or_default();
                self.persist_runtime("MODE_CHANGED", "Explicit selection; entries paused")
                    .await?;
            }
            Action::Strategy { mut config } => {
                config.validate()?;
                config.version = self
                    .strategy
                    .version
                    .checked_add(1)
                    .context("strategy version overflow")?;
                self.store
                    .write(vec![
                        Mutation::put(
                            "strategy_versions",
                            order_key(self.mode, config.version),
                            &config,
                        )?,
                        Mutation::put("config", vec![self.mode.byte(), 0], &config)?,
                    ])
                    .await?;
                self.strategy = config;
                self.persist_runtime("STRATEGY_CHANGED", "New strategy version activated")
                    .await?;
            }
            Action::Risk { config } => {
                config.validate()?;
                ensure!(
                    self.orders.is_empty(),
                    "cannot change risk with pending orders"
                );
                self.store
                    .write(vec![Mutation::put(
                        "config",
                        vec![self.mode.byte(), 1],
                        &config,
                    )?])
                    .await?;
                self.executors[&self.mode].configure_risk(config.clone());
                self.risk = config;
                self.persist_runtime("RISK_CHANGED", "Risk limits activated")
                    .await?;
            }
        }
        Ok(())
    }
    async fn market(&mut self, mut event: MarketEvent) -> Result<()> {
        let dedup = Instant::now();
        if event.facts.is_none() && !self.dedup.accept(&event) {
            return Ok(());
        }
        self.metrics.observe("dedup", dedup);
        self.metrics.events.inc();
        if self
            .event_writer
            .try_send((self.mode, event.clone()))
            .is_err()
        {
            self.metrics.dropped.inc();
        }
        if !event.speculative && event.price > 0 {
            self.prices.insert(event.mint, event.price);
            if let Some(p) = self.positions.get_mut(&event.mint) {
                let previous_route = (p.venue, p.pool);
                p.current_price = event.price;
                p.current_market_cap = event.market_cap;
                if event.venue == Venue::PumpSwap {
                    p.venue = Venue::PumpSwap;
                    p.pool = self.executors[&self.mode]
                        .market_route(event.mint)
                        .or(p.pool);
                }
                let reason = if self.runtime.state == BotState::Stopping {
                    Some(ExitReason::BotStop)
                } else if event.kind == EventKind::Sell && event.trader == Some(p.creator) {
                    Some(ExitReason::DevSell)
                } else {
                    p.exit_trigger(self.clock.now_ms(), self.risk.max_hold_ms)
                };
                let route_update = (previous_route != (p.venue, p.pool)).then(|| p.clone());
                if let Some(position) = route_update {
                    if let Err(error) = self
                        .store
                        .write(vec![Mutation::put(
                            "positions",
                            mint_key(self.mode, event.mint),
                            &position,
                        )?])
                        .await
                    {
                        self.degrade(error);
                    }
                }
                if let Some(reason) = reason {
                    self.exit(event.mint, reason).await?;
                }
            }
        }
        if let Some(facts) = event.facts.take() {
            self.facts.insert(event.mint, facts);
        }
        if !event.speculative && matches!(event.kind, EventKind::Buy | EventKind::Sell) {
            let obs = self.observations.entry(event.mint).or_default();
            if event.kind == EventKind::Buy {
                obs.1 = obs.1.saturating_add(1);
                if obs.0.len() < 1000 {
                    if let Some(trader) = event.trader {
                        obs.0.insert(trader);
                    }
                }
            } else {
                obs.2 = obs.2.saturating_add(1);
            }
        }
        if event.kind == EventKind::Create {
            self.candidates
                .entry(event.mint)
                .or_insert_with(|| event.clone());
        }
        if let Some(mut candidate) = self.candidates.get(&event.mint).cloned() {
            candidate.price = if event.price > 0 {
                event.price
            } else {
                *self.prices.get(&event.mint).unwrap_or(&0)
            };
            candidate.market_cap = event.market_cap;
            if candidate.price > 0 {
                self.try_entry(candidate).await?;
            }
        }
        if self.candidates.len() > 10_000 {
            let now = self.clock.now_ms();
            self.candidates
                .retain(|_, e| now.saturating_sub(e.observed_ms) < 30_000);
            self.facts.retain(|mint, _| {
                self.candidates.contains_key(mint) || self.positions.contains_key(mint)
            });
            self.observations
                .retain(|mint, _| self.candidates.contains_key(mint));
            self.prices.retain(|mint, _| {
                self.candidates.contains_key(mint) || self.positions.contains_key(mint)
            });
        }
        Ok(())
    }
    async fn try_entry(&mut self, event: MarketEvent) -> Result<()> {
        let mint = event.mint;
        if self.purchased.contains_key(&mint) {
            self.detection(event, None, "ALREADY_PURCHASED");
            return Ok(());
        }
        if self.pending.contains_key(&mint) {
            self.detection(event, None, "UNKNOWN_PENDING");
            return Ok(());
        }
        if self.runtime.state != BotState::Running || !self.gate.load(Ordering::Acquire) {
            return Ok(());
        }
        ensure!(self.store.writable(), "LOCAL_PERSISTENCE_UNAVAILABLE");
        let now = self.clock.now_ms();
        if now.saturating_sub(event.observed_ms) > 5000 {
            self.detection(event, None, "ENTRY_EXPIRED");
            self.candidates.remove(&mint);
            return Ok(());
        }
        if self.strategy.entry_mode == EntryMode::Confirmed
            && now.saturating_sub(event.observed_ms) < self.strategy.confirmation_ms
        {
            return Ok(());
        }
        let started = Instant::now();
        let mut facts = self.facts.get(&mint).cloned().unwrap_or_default();
        if let Some(obs) = self.observations.get(&mint) {
            facts.unique_buyers = obs.0.len() as u16;
            facts.buys = obs.1;
            facts.sells = obs.2;
        }
        let scored = score(&facts, &self.strategy);
        self.metrics.observe("analysis", started);
        if !scored.approved {
            self.detection(event, Some(scored), "REJECTED");
            return Ok(());
        }
        let reserved = (self.pending.len() as u64).saturating_mul(self.risk.sol_per_trade);
        let exposure: u64 = self.positions.values().map(|p| p.spent).sum();
        let overhead = self.risk.max_priority_fee + self.risk.max_jito_tip + 5_000_000;
        if self.positions.len() + self.pending.len() >= self.risk.max_positions
            || self.pending.len() >= self.risk.max_pending
            || exposure
                .saturating_add(reserved)
                .saturating_add(self.risk.sol_per_trade)
                > self.risk.max_exposure
            || self.runtime.balance
                < reserved
                    .saturating_add(self.risk.sol_per_trade)
                    .saturating_add(overhead)
                    .saturating_add(self.risk.min_balance)
            || self.runtime.daily_pnl <= -(self.risk.max_daily_loss as i64)
            || self.runtime.daily_trades + self.pending.len() as u32 >= self.risk.max_daily_trades
            || self.runtime.failed_submissions >= self.risk.max_failed_submissions
            || self.runtime.consecutive_losses >= self.risk.max_consecutive_losses
        {
            self.detection(event, Some(scored), "RISK_LIMIT");
            return Ok(());
        }
        if !self.executors[&self.mode].ready().iter().all(|c| c.pass) {
            self.gate.store(false, Ordering::Release);
            self.error = Some("Executor health failed".into());
            return Ok(());
        }
        let id = self.runtime.next_id;
        self.runtime.next_id = id.checked_add(1).context("order id overflow")?;
        let mut reservation = Reservation {
            mint,
            mode: self.mode,
            order_id: id,
            status: "RESERVED".into(),
            created_ms: now,
            signature: None,
        };
        self.pending.insert(mint, reservation.clone());
        let start = Instant::now();
        self.store
            .write(vec![
                Mutation::put(
                    "purchase_reservations",
                    mint_key(self.mode, mint),
                    &reservation,
                )?,
                Mutation::put("bot_state", vec![self.mode.byte()], &self.runtime)?,
            ])
            .await?;
        self.metrics.observe("rocksdb_reservation", start);
        let request = OrderRequest {
            id,
            mode: self.mode,
            mint,
            creator: event.creator,
            venue: event.venue,
            buy: true,
            amount: self.risk.sol_per_trade,
            price: event.price,
            market_cap: event.market_cap,
            score: scored.total,
            strategy: self.strategy.clone(),
            slippage_bps: self.risk.slippage_bps,
            reason: None,
            created_ms: now,
            detected_ms: event.observed_ms,
        };
        let build = Instant::now();
        let order = match self.executors[&self.mode].prepare(request) {
            Ok(o) => o,
            Err(e) => {
                self.store
                    .write(vec![Mutation::Delete(
                        "purchase_reservations",
                        mint_key(self.mode, mint),
                    )])
                    .await?;
                self.pending.remove(&mint);
                self.error = Some(e.to_string());
                return Ok(());
            }
        };
        self.metrics.observe("transaction_build_and_sign", build);
        reservation.signature = Some(order.signature.clone());
        reservation.status = "UNKNOWN_PENDING".into();
        self.store
            .write(vec![
                Mutation::put("orders", order_key(self.mode, id), &order)?,
                Mutation::put(
                    "purchase_reservations",
                    mint_key(self.mode, mint),
                    &reservation,
                )?,
            ])
            .await?;
        self.pending.insert(mint, reservation);
        self.orders.insert(id, order.clone());
        self.dispatch(order, true);
        self.detection(event, Some(scored), "BUY_SUBMITTED");
        Ok(())
    }
    fn detection(&mut self, event: MarketEvent, score: Option<Score>, decision: &str) {
        if self.feed.len() >= 200 {
            self.feed.pop_front();
        }
        self.feed.push_back(Detection {
            event,
            score,
            decision: decision.into(),
        });
    }
    fn dispatch(&mut self, order: PreparedOrder, submit: bool) {
        let id = order.request.id;
        if !self.active_jobs.insert(id) {
            return;
        }
        self.last_job.insert(id, Instant::now());
        let executor = self.executors[&order.request.mode].clone();
        let completion = self.completion.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            if submit {
                metrics
                    .submissions
                    .with_label_values(&[
                        &format!("{:?}", order.request.mode),
                        if order.request.buy { "buy" } else { "sell" },
                    ])
                    .inc();
                let s = Instant::now();
                let _ = executor.submit(&order).await;
                metrics.observe("submission", s);
            }
            let settlement = match tokio::time::timeout(
                Duration::from_secs(8),
                executor.reconcile(&order),
            )
            .await
            {
                Ok(Ok(v)) => v,
                _ => Settlement::Unknown,
            };
            let _ = completion
                .send(Completed {
                    id,
                    settlement,
                    elapsed_us: start.elapsed().as_micros() as u64,
                })
                .await;
        });
    }
    async fn exit(&mut self, mint: Key, reason: ExitReason) -> Result<()> {
        let mut p = self
            .positions
            .get(&mint)
            .cloned()
            .context("position not found")?;
        if p.exit_order.is_some() {
            return Ok(());
        }
        ensure!(
            self.active_jobs.len() < 128,
            "critical execution queue saturated"
        );
        let id = self.runtime.next_id;
        self.runtime.next_id = id.checked_add(1).context("order id overflow")?;
        let request = OrderRequest {
            id,
            mode: self.mode,
            mint,
            creator: p.creator,
            venue: p.venue,
            buy: false,
            amount: p.quantity,
            price: p.current_price,
            market_cap: p.current_market_cap,
            score: p.score,
            strategy: p.strategy.clone(),
            slippage_bps: self.risk.slippage_bps,
            reason: Some(reason),
            created_ms: self.clock.now_ms(),
            detected_ms: p.detected_ms,
        };
        let order = self.executors[&self.mode].prepare(request)?;
        p.exit_order = Some(id);
        p.exit_reason = Some(reason);
        p.state = PositionState::SellSubmitted;
        if let Err(e) = self
            .store
            .write(vec![
                Mutation::put("orders", order_key(self.mode, id), &order)?,
                Mutation::put("positions", mint_key(self.mode, mint), &p)?,
                Mutation::put("bot_state", vec![self.mode.byte()], &self.runtime)?,
            ])
            .await
        {
            self.degrade(e);
            self.error =
                Some("Emergency exit tracked in RAM; durable reconciliation required".into());
        }
        self.positions.insert(mint, p);
        self.orders.insert(id, order.clone());
        self.dispatch(order, true);
        Ok(())
    }
    async fn complete(&mut self, done: Completed) -> Result<()> {
        self.active_jobs.remove(&done.id);
        let Some(order) = self.orders.get(&done.id).cloned() else {
            return Ok(());
        };
        self.metrics
            .stages
            .with_label_values(&["submission_to_settlement"])
            .observe(done.elapsed_us as f64 / 1_000_000.0);
        let r = &order.request;
        let key = mint_key(r.mode, r.mint);
        let mut batch = vec![];
        let mut runtime = self.runtime.clone();
        match done.settlement {
            Settlement::Unknown => {
                self.error = Some(format!("UNKNOWN_PENDING: {}", order.signature));
                return Ok(());
            }
            Settlement::ProvenFailed { fee } => {
                runtime.failed_submissions += 1;
                runtime.balance = runtime.balance.saturating_sub(fee);
                batch.push(Mutation::Delete("orders", order_key(r.mode, r.id)));
                if r.buy {
                    batch.push(Mutation::Delete("purchase_reservations", key.clone()));
                } else if let Some(p) = self.positions.get(&r.mint) {
                    let mut p = p.clone();
                    p.state = PositionState::Open;
                    p.exit_order = None;
                    batch.push(Mutation::put("positions", key.clone(), &p)?);
                }
                batch.push(Mutation::put("bot_state", vec![r.mode.byte()], &runtime)?);
                self.store.write(batch).await?;
                self.runtime = runtime;
                self.orders.remove(&r.id);
                if r.buy {
                    self.pending.remove(&r.mint);
                } else if let Some(p) = self.positions.get_mut(&r.mint) {
                    p.state = PositionState::Open;
                    p.exit_order = None;
                }
            }
            Settlement::Filled(fill) => {
                let fees = fill.network_fee + fill.priority_fee + fill.tip;
                batch.push(Mutation::put("fills", order_key(r.mode, r.id), &fill)?);
                batch.push(Mutation::Delete("orders", order_key(r.mode, r.id)));
                if r.buy {
                    let p = Position {
                        mint: r.mint,
                        pool: order.pool,
                        creator: r.creator,
                        mode: r.mode,
                        venue: r.venue,
                        state: PositionState::Open,
                        entry_signature: fill.signature.clone(),
                        entry_ms: fill.timestamp_ms,
                        detected_ms: r.detected_ms,
                        quantity: fill.token_quantity,
                        spent: fill.sol_amount,
                        entry_price: (fill.sol_amount as u128 * PRICE_SCALE as u128
                            / fill.token_quantity as u128)
                            as u64,
                        current_price: r.price,
                        entry_market_cap: r.market_cap,
                        current_market_cap: r.market_cap,
                        entry_fees: fees,
                        score: r.score,
                        strategy: r.strategy.clone(),
                        exit_order: None,
                        exit_reason: None,
                    };
                    let bought = PurchasedToken {
                        mint: r.mint,
                        creator: r.creator,
                        first_detected_ms: r.detected_ms,
                        first_submitted_ms: r.created_ms,
                        confirmed_ms: fill.timestamp_ms,
                        signature: fill.signature.clone(),
                        entry_price: p.entry_price,
                        entry_market_cap: r.market_cap,
                        strategy_version: r.strategy.version,
                        exit_signature: None,
                        exit_price: None,
                        final_pnl: None,
                        updated_ms: fill.timestamp_ms,
                    };
                    runtime.balance = runtime.balance.saturating_sub(fill.sol_amount + fees);
                    runtime.daily_trades += 1;
                    batch.extend([
                        Mutation::put("purchased_tokens", key.clone(), &bought)?,
                        Mutation::put("positions", key.clone(), &p)?,
                        Mutation::Delete("purchase_reservations", key.clone()),
                        Mutation::put("bot_state", vec![r.mode.byte()], &runtime)?,
                    ]);
                    self.positions.insert(r.mint, p);
                    self.purchased.insert(r.mint, bought);
                    self.store.write(batch).await?;
                    self.pending.remove(&r.mint);
                    self.orders.remove(&r.id);
                    self.runtime = runtime;
                } else {
                    let mut p = self
                        .positions
                        .get(&r.mint)
                        .cloned()
                        .context("sell lacks position")?;
                    ensure!(
                        fill.token_quantity == p.quantity,
                        "partial sell requires reconciliation; position retained"
                    );
                    p.state = PositionState::Closed;
                    let gross = (fill.sol_amount as i128 - p.spent as i128)
                        .clamp(i64::MIN as i128, i64::MAX as i128)
                        as i64;
                    let net = (gross as i128 - p.entry_fees as i128 - fees as i128)
                        .clamp(i64::MIN as i128, i64::MAX as i128)
                        as i64;
                    let trade = ClosedTrade {
                        hold_ms: fill.timestamp_ms.saturating_sub(p.entry_ms),
                        position: p.clone(),
                        exit: fill.clone(),
                        reason: r.reason.unwrap_or(ExitReason::ManualSell),
                        net_pnl: net,
                        gross_pnl: gross,
                    };
                    runtime.balance = runtime
                        .balance
                        .saturating_add(fill.sol_amount)
                        .saturating_sub(fees);
                    runtime.daily_pnl = runtime.daily_pnl.saturating_add(net);
                    runtime.consecutive_losses = if net < 0 {
                        runtime.consecutive_losses + 1
                    } else {
                        0
                    };
                    batch.extend([
                        Mutation::put("closed_positions", key.clone(), &trade)?,
                        Mutation::put(
                            "trade_history",
                            time_key(r.mode, fill.timestamp_ms, r.id),
                            &trade,
                        )?,
                        Mutation::Delete("positions", key.clone()),
                        Mutation::put("bot_state", vec![r.mode.byte()], &runtime)?,
                    ]);
                    if let Some(bought) = self.purchased.get(&r.mint) {
                        let mut bought = bought.clone();
                        bought.exit_signature = Some(fill.signature.clone());
                        bought.exit_price = Some(r.price);
                        bought.final_pnl = Some(net);
                        bought.updated_ms = fill.timestamp_ms;
                        batch.push(Mutation::put("purchased_tokens", key.clone(), &bought)?);
                    }
                    let mut updated_aggregate = self.aggregate.clone();
                    updated_aggregate.add(&trade);
                    batch.push(Mutation::put(
                        "config",
                        vec![r.mode.byte(), 2],
                        &updated_aggregate,
                    )?);
                    self.store.write(batch).await?;
                    self.aggregate = updated_aggregate;
                    self.positions.remove(&r.mint);
                    self.orders.remove(&r.id);
                    self.runtime = runtime;
                }
            }
        }
        self.maybe_stopped().await?;
        Ok(())
    }
    async fn maybe_stopped(&mut self) -> Result<()> {
        if self.runtime.state == BotState::Stopping
            && self.positions.is_empty()
            && self.orders.is_empty()
            && self.pending.is_empty()
        {
            self.runtime.state = BotState::Stopped;
            self.executors[&self.mode].authorize_live(false);
            self.persist_runtime("STOPPED", "Zero positions and unresolved orders")
                .await?;
        }
        Ok(())
    }
    async fn maintenance(&mut self) -> Result<()> {
        let now = self.clock.now_ms();
        if now / 86_400_000 != self.runtime.day {
            self.runtime.day = now / 86_400_000;
            self.runtime.daily_pnl = 0;
            self.runtime.daily_trades = 0;
            self.persist_runtime("DAY_ROLLOVER", "Daily counters reset")
                .await?;
        }
        if !self.store.writable() {
            self.gate.store(false, Ordering::Release);
            if self.runtime.state != BotState::Stopping {
                self.runtime.state = BotState::Degraded;
            }
        }
        if self.runtime.state == BotState::Running
            && (self.database.as_ref().is_some_and(|d| d.status != "READY")
                || (self.mode != Mode::Replay
                    && !self
                        .services
                        .values()
                        .any(|s| s.name == "Helius Confirmed" && s.status == "CONNECTED"))
                || !self.executors[&self.mode]
                    .ready()
                    .iter()
                    .all(|check| check.pass)
                || (!self.positions.is_empty()
                    && self.services.values().any(|s| {
                        s.name == format!("{:?} position protection", self.mode)
                            && s.status == "FAILED"
                    })))
        {
            self.gate.store(false, Ordering::Release);
            self.error = Some("Critical preflight lost; new entries disabled".into());
        }
        let orders: Vec<_> = self
            .orders
            .values()
            .filter(|o| {
                !self.active_jobs.contains(&o.request.id)
                    && self
                        .last_job
                        .get(&o.request.id)
                        .is_none_or(|t| t.elapsed() > Duration::from_secs(2))
            })
            .cloned()
            .collect();
        for order in orders {
            self.dispatch(order, false);
        }
        let exits: Vec<_> = self
            .positions
            .values()
            .filter_map(|p| {
                if self.runtime.state == BotState::Stopping {
                    Some((p.mint, ExitReason::BotStop))
                } else {
                    p.exit_trigger(now, self.risk.max_hold_ms)
                        .map(|r| (p.mint, r))
                }
            })
            .collect();
        for (mint, reason) in exits {
            if let Err(e) = self.exit(mint, reason).await {
                self.error = Some(e.to_string());
            }
        }
        self.maybe_stopped().await?;
        if self.strategy.entry_mode == EntryMode::Confirmed {
            let candidates: Vec<_> = self.candidates.values().cloned().collect();
            for mut e in candidates {
                e.price = *self.prices.get(&e.mint).unwrap_or(&0);
                if e.price > 0 {
                    self.try_entry(e).await?;
                }
            }
        }
        Ok(())
    }
}
