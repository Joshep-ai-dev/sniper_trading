use anyhow::Result;
use async_trait::async_trait;
use sniper_connectors::execution::PaperExecutor;
use sniper_domain::*;
use sniper_engine::{Action, Engine, EngineHandle};
use sniper_store::{mint_key, order_key, Mutation, Store, StoreConfig};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn config(t: &std::path::Path) -> StoreConfig {
    StoreConfig {
        path: t.join("db"),
        wal_dir: t.join("wal"),
        checkpoint_path: t.join("checkpoint"),
        backup_path: t.join("backup"),
        min_free_bytes: 0,
        write_buffer_bytes: 1 << 20,
        block_cache_bytes: 1 << 20,
        ..Default::default()
    }
}
fn paper() -> Arc<dyn TradeExecutor> {
    Arc::new(
        PaperExecutor::new(
            Mode::Replay,
            PaperConfig {
                network_delay_ms: 0,
                landing_delay_ms: 0,
                slippage_bps: 0,
                tip: 0,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}
async fn launch(store: Arc<Store>, executor: Arc<dyn TradeExecutor>) -> EngineHandle {
    Engine::launch(
        store,
        Mode::Replay,
        StrategyConfig::default(),
        RiskConfig::default(),
        5 * SOL,
        HashMap::from([(Mode::Replay, executor)]),
        Arc::new(SystemClock),
    )
    .await
    .unwrap()
}
fn event(mint: u8, price: u64, kind: EventKind) -> MarketEvent {
    MarketEvent {
        signature: format!("test-{mint}-{price}-{:?}", kind),
        slot: 1,
        instruction_index: 0,
        mint: Key([mint; 32]),
        creator: Key([100; 32]),
        venue: Venue::PumpFun,
        kind,
        observed_ms: SystemClock.now_ms(),
        blockchain_ms: None,
        price,
        market_cap: 0,
        trader: None,
        sol_amount: 0,
        token_amount: 0,
        speculative: false,
        facts: Some(TokenFacts {
            verified: true,
            creator_score: 30,
            unique_buyers: 10,
            buys: 10,
            ..Default::default()
        }),
    }
}
async fn until(h: &EngineHandle, p: impl Fn(&sniper_engine::Snapshot) -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if p(h.snapshot().as_ref()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("engine condition timed out");
}
async fn start(h: &EngineHandle) {
    until(h, |s| s.database.is_some()).await;
    h.action(Action::Start { confirmation: None })
        .await
        .unwrap();
    until(h, |s| s.state == BotState::Running).await;
}
async fn shutdown(h: EngineHandle, store: Arc<Store>) {
    h.shutdown().await.unwrap();
    drop(h);
    store.flush().await.unwrap();
}

#[tokio::test]
async fn hundred_concurrent_detections_one_buy() {
    let t = tempfile::tempdir().unwrap();
    let store = Store::open(
        config(t.path()),
        &format!("concurrency-{}", t.path().display()),
    )
    .unwrap();
    let h = launch(store.clone(), paper()).await;
    start(&h).await;
    let mut tasks = vec![];
    for _ in 0..100 {
        let h = h.clone();
        tasks.push(tokio::spawn(async move {
            h.event(event(1, 10, EventKind::Create)).await.unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    until(&h, |s| s.positions.len() == 1 && s.pending.is_empty()).await;
    assert_eq!(
        h.metrics
            .submissions
            .with_label_values(&["Replay", "buy"])
            .get(),
        1
    );
    assert_eq!(
        store
            .load::<PurchasedToken>("purchased_tokens", Mode::Replay)
            .unwrap()
            .len(),
        1
    );
    shutdown(h, store).await;
}
#[tokio::test]
async fn bought_sold_reopen_permanently_rejected() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let wallet = format!("restart-{}", t.path().display());
    let store = Store::open(c.clone(), &wallet).unwrap();
    let h = launch(store.clone(), paper()).await;
    start(&h).await;
    h.event(event(2, 10, EventKind::Create)).await.unwrap();
    until(&h, |s| s.positions.len() == 1).await;
    h.sell(Key([2; 32])).await.unwrap();
    until(&h, |s| s.positions.is_empty() && s.unresolved_orders == 0).await;
    h.shutdown().await.unwrap();
    drop(h);
    // The asynchronous history writer releases its DB handle after the actor closes its channel.
    for _ in 0..100 {
        if Arc::strong_count(&store) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(store);
    let store = Store::open(c, &wallet).unwrap();
    let h = launch(store.clone(), paper()).await;
    start(&h).await;
    h.event(event(2, 10, EventKind::Create)).await.unwrap();
    until(&h, |s| {
        s.feed.iter().any(|d| d.decision == "ALREADY_PURCHASED")
    })
    .await;
    assert_eq!(
        h.metrics
            .submissions
            .with_label_values(&["Replay", "buy"])
            .get(),
        0
    );
    assert!(h.snapshot().positions.is_empty());
    shutdown(h, store).await;
}
#[tokio::test]
async fn stop_waits_for_all_positions() {
    let t = tempfile::tempdir().unwrap();
    let store = Store::open(config(t.path()), &format!("stop-{}", t.path().display())).unwrap();
    let h = launch(store.clone(), paper()).await;
    start(&h).await;
    for mint in 3..6 {
        h.event(event(mint, 10, EventKind::Create)).await.unwrap();
    }
    until(&h, |s| s.positions.len() == 3).await;
    h.action(Action::Stop).await.unwrap();
    h.event(event(7, 10, EventKind::Create)).await.unwrap();
    until(&h, |s| s.state == BotState::Stopped).await;
    assert!(h.snapshot().positions.is_empty());
    assert_eq!(
        h.metrics
            .submissions
            .with_label_values(&["Replay", "sell"])
            .get(),
        3
    );
    assert_eq!(
        h.metrics
            .submissions
            .with_label_values(&["Replay", "buy"])
            .get(),
        3
    );
    let analytics_store = store.clone();
    tokio::task::spawn_blocking(move || {
        assert_eq!(analytics_store.refresh_analytics().unwrap(), 3);
        assert_eq!(analytics_store.refresh_analytics().unwrap(), 0);
        for cf in [
            "analytics_minute",
            "analytics_five_minute",
            "analytics_hourly",
            "analytics_daily",
        ] {
            assert_eq!(
                analytics_store
                    .load::<Aggregate>(cf, Mode::Replay)
                    .unwrap()
                    .iter()
                    .map(|bucket| bucket.trades)
                    .sum::<u64>(),
                3
            );
            assert!(analytics_store
                .load::<Aggregate>(cf, Mode::Live)
                .unwrap()
                .is_empty());
        }
    })
    .await
    .unwrap();
    shutdown(h, store).await;
}
#[tokio::test]
async fn take_profit_and_stop_loss_priority_exit() {
    let t = tempfile::tempdir().unwrap();
    let store = Store::open(
        config(t.path()),
        &format!("threshold-{}", t.path().display()),
    )
    .unwrap();
    let h = launch(store.clone(), paper()).await;
    start(&h).await;
    for mint in [8, 9] {
        h.event(event(mint, 10, EventKind::Create)).await.unwrap();
    }
    until(&h, |s| s.positions.len() == 2).await;
    h.event(event(8, 16, EventKind::Price)).await.unwrap();
    h.event(event(9, 7, EventKind::Price)).await.unwrap();
    until(&h, |s| s.positions.is_empty() && s.aggregate.trades == 2).await;
    assert_eq!(h.snapshot().aggregate.tp, 1);
    assert_eq!(h.snapshot().aggregate.sl, 1);
    shutdown(h, store).await;
}

struct Unknown {
    submissions: AtomicUsize,
}
#[async_trait]
impl TradeExecutor for Unknown {
    fn mode(&self) -> Mode {
        Mode::Replay
    }
    fn prepare(&self, r: OrderRequest) -> Result<PreparedOrder> {
        paper().prepare(r)
    }
    async fn submit(&self, _: &PreparedOrder) -> Result<()> {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn reconcile(&self, _: &PreparedOrder) -> Result<Settlement> {
        Ok(Settlement::Unknown)
    }
    fn ready(&self) -> Vec<PreflightCheck> {
        vec![]
    }
}
#[tokio::test]
async fn unknown_buy_restored_without_resubmission() {
    let t = tempfile::tempdir().unwrap();
    let store = Store::open(config(t.path()), &format!("unknown-{}", t.path().display())).unwrap();
    let unknown = Arc::new(Unknown {
        submissions: AtomicUsize::new(0),
    });
    let e = event(10, 10, EventKind::Create);
    let request = OrderRequest {
        id: 77,
        mode: Mode::Replay,
        mint: e.mint,
        creator: e.creator,
        venue: e.venue,
        buy: true,
        amount: 50_000_000,
        price: 10,
        market_cap: 0,
        score: 100,
        strategy: StrategyConfig::default(),
        slippage_bps: 500,
        reason: None,
        created_ms: SystemClock.now_ms(),
        detected_ms: e.observed_ms,
    };
    let order = unknown.prepare(request).unwrap();
    let reservation = Reservation {
        mint: e.mint,
        mode: Mode::Replay,
        order_id: 77,
        status: "BUY_SUBMITTED".into(),
        created_ms: e.observed_ms,
        signature: Some(order.signature.clone()),
    };
    store
        .write(vec![
            Mutation::put("orders", order_key(Mode::Replay, 77), &order).unwrap(),
            Mutation::put(
                "purchase_reservations",
                mint_key(Mode::Replay, e.mint),
                &reservation,
            )
            .unwrap(),
        ])
        .await
        .unwrap();
    let h = launch(store.clone(), unknown.clone()).await;
    until(&h, |s| {
        s.pending
            .first()
            .is_some_and(|p| p.status == "UNKNOWN_PENDING")
    })
    .await;
    assert!(h
        .action(Action::Start { confirmation: None })
        .await
        .is_err());
    h.event(e).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(unknown.submissions.load(Ordering::SeqCst), 0);
    h.action(Action::Stop).await.unwrap();
    until(&h, |s| s.state == BotState::Stopping).await;
    assert_ne!(h.snapshot().state, BotState::Stopped);
    assert!(h.action(Action::Pause).await.is_err());
    shutdown(h, store.clone()).await;
    assert_eq!(
        store
            .get::<sniper_engine::RuntimeRecord>("bot_state", &[Mode::Replay.byte()])
            .unwrap()
            .unwrap()
            .state,
        BotState::Stopping
    );
}
#[tokio::test]
async fn persistence_loss_disables_buys_protects_positions() {
    let t = tempfile::tempdir().unwrap();
    let store = Store::open(config(t.path()), &format!("io-{}", t.path().display())).unwrap();
    let h = launch(store.clone(), paper()).await;
    start(&h).await;
    h.event(event(11, 10, EventKind::Create)).await.unwrap();
    until(&h, |s| s.positions.len() == 1).await;
    store.disable_writes();
    h.event(event(12, 10, EventKind::Create)).await.unwrap();
    h.event(event(11, 7, EventKind::Price)).await.unwrap();
    until(&h, |s| s.state == BotState::Degraded).await;
    assert_eq!(
        h.metrics
            .submissions
            .with_label_values(&["Replay", "buy"])
            .get(),
        1
    );
    assert_eq!(
        h.metrics
            .submissions
            .with_label_values(&["Replay", "sell"])
            .get(),
        1
    );
    h.shutdown().await.unwrap();
}
