mod config;
mod routes;
use anyhow::{ensure, Context, Result};
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use parking_lot::Mutex;
use sniper_connectors::{
    execution::{NetworkExecutor, PaperExecutor},
    feed,
    rpc::Rpc,
    secrets::{SecretProvider, SecretVault},
};
use sniper_domain::*;
use sniper_engine::{Engine, EngineHandle};
use sniper_store::Store;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

pub struct App {
    engine: EngineHandle,
    store: Arc<Store>,
    vault: Arc<SecretVault>,
    token: String,
    origin: String,
    sessions: Mutex<HashMap<String, Instant>>,
    networks: Vec<Arc<NetworkExecutor>>,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "sniper=info,tower_http=warn",
        ))
        .init();
    let path = std::env::var("SNIPER_CONFIG").unwrap_or_else(|_| "config/default.toml".into());
    let config: config::Config = toml::from_str(&std::fs::read_to_string(path)?)?;
    config.strategy.validate()?;
    config.risk.validate()?;
    let bind: std::net::SocketAddr = config.bind.parse().context("invalid API bind address")?;
    ensure!(bind.ip().is_loopback(), "Rust API must bind to loopback");
    let token = std::env::var("SNIPER_API_TOKEN").context("SNIPER_API_TOKEN required")?;
    ensure!(
        token.len() >= 32,
        "API token must contain at least 32 characters"
    );
    let master = zeroize::Zeroizing::new(
        std::env::var("SNIPER_MASTER_KEY").context("SNIPER_MASTER_KEY required")?,
    );
    let vault = Arc::new(SecretVault::new(config.secret_path.clone(), &master)?);
    let secret_value =
        |name: &str| -> Result<Option<String>> { Ok(vault.get(name)?.map(|s| s.to_string())) };
    let mut executors: HashMap<Mode, Arc<dyn TradeExecutor>> = HashMap::new();
    executors.insert(
        Mode::Paper,
        Arc::new(PaperExecutor::new(Mode::Paper, config.paper.clone())?),
    );
    executors.insert(
        Mode::Replay,
        Arc::new(PaperExecutor::new(Mode::Replay, config.paper.clone())?),
    );
    let main_rpc = secret_value("helius_rpc_url")?.map(Rpc::new).transpose()?;
    let mut networks = vec![];
    if let (Some(secret), Some(rpc), Some(sender), Some(jito)) = (
        vault.get("mainnet_wallet")?,
        main_rpc.clone(),
        secret_value("helius_sender_url")?,
        secret_value("jito_url")?,
    ) {
        let executor = NetworkExecutor::new(
            Mode::Live,
            &secret,
            rpc,
            Rpc::new(sender)?,
            Rpc::new(jito)?,
            config.risk.clone(),
        )?;
        executors.insert(Mode::Live, executor.clone());
        networks.push(executor);
    }
    if let (Some(secret), Some(rpc)) =
        (vault.get("devnet_wallet")?, secret_value("devnet_rpc_url")?)
    {
        let rpc = Rpc::new(rpc)?;
        let executor = NetworkExecutor::new(
            Mode::Devnet,
            &secret,
            rpc.clone(),
            rpc.clone(),
            rpc,
            config.risk.clone(),
        )?;
        executors.insert(Mode::Devnet, executor.clone());
        networks.push(executor);
    }
    // All configured signers get independent machine-wide locks, including the nonselected network.
    let mut signer_locks = Vec::new();
    for network in &networks {
        let lock_path = config
            .store
            .path
            .parent()
            .unwrap_or(std::path::Path::new("data"))
            .join(format!("signer-{}", network.address));
        signer_locks.push(sniper_store::Ownership::acquire(
            &lock_path,
            &network.address.to_string(),
        )?);
    }
    for network in &networks {
        let _ = network.refresh().await;
    }
    let starting_balance = if matches!(config.mode, Mode::Live | Mode::Devnet) {
        executors
            .get(&config.mode)
            .and_then(|e| e.wallet_balance())
            .context("real wallet balance unavailable; refusing recovery")?
    } else {
        config.paper.starting_balance
    };
    let c = config.store.clone();
    let identity = format!(
        "database:{}",
        std::env::current_dir()?.join(&config.store.path).display()
    );
    let store = tokio::task::spawn_blocking(move || Store::open(c, &identity)).await??;
    ensure!(
        !std::fs::canonicalize(&config.secret_path)?
            .starts_with(std::fs::canonicalize(&config.store.path)?),
        "secret storage must be outside RocksDB"
    );
    let analytics_store = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let store = analytics_store.clone();
            if !matches!(
                tokio::task::spawn_blocking(move || store.refresh_analytics()).await,
                Ok(Ok(_))
            ) {
                tracing::warn!("analytics repair failed; durable trade history remains available");
            }
        }
    });
    let replay_clock = Arc::new(ReplayClock(std::sync::atomic::AtomicU64::new(
        SystemClock.now_ms(),
    )));
    let clock: Arc<dyn Clock> = if config.mode == Mode::Replay {
        replay_clock.clone()
    } else {
        Arc::new(SystemClock)
    };
    let engine = Engine::launch(
        store.clone(),
        config.mode,
        config.strategy.clone(),
        config.risk.clone(),
        starting_balance,
        executors,
        clock,
    )
    .await?;
    if config.mode == Mode::Replay {
        let e = engine.clone();
        let s = store.clone();
        let clock = replay_clock.clone();
        let speed = std::env::var("SNIPER_REPLAY_SPEED")
            .unwrap_or_else(|_| "1".into())
            .parse::<u32>()?;
        tokio::spawn(async move {
            match sniper_engine::replay::play(s, e, clock, Mode::Paper, speed).await {
                Ok(count) => tracing::info!(count, "replay completed"),
                Err(_) => tracing::error!("replay failed; inspect local storage and preflight"),
            }
        });
    }
    for network in &networks {
        let network = network.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(2));
            loop {
                timer.tick().await;
                if network.refresh().await.is_err() {
                    network.warm.write().network_verified = false;
                    let _ = engine
                        .service(ServiceStatus {
                            name: format!("{:?} wallet", network.mode()),
                            status: "FAILED".into(),
                            error: Some("Network verification or RPC refresh failed".into()),
                            ..ServiceStatus::missing("Wallet")
                        })
                        .await;
                }
            }
        });
    }
    for network in &networks {
        let network = network.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(2));
            loop {
                timer.tick().await;
                let positions: Vec<_> = engine
                    .snapshot()
                    .positions
                    .iter()
                    .filter(|p| p.mode == network.mode())
                    .cloned()
                    .collect();
                let mut jobs = tokio::task::JoinSet::new();
                let mut failed = false;
                for position in positions {
                    if jobs.len() >= 4 {
                        failed |= !matches!(jobs.join_next().await, Some(Ok(Ok(()))));
                    }
                    let network = network.clone();
                    let engine = engine.clone();
                    jobs.spawn(async move {
                        let cache = network
                            .warm_market(
                                position.mint,
                                position.venue,
                                position.pool.map(sniper_connectors::protocol::pubkey),
                            )
                            .await?;
                        let now = SystemClock.now_ms();
                        engine
                            .event(MarketEvent {
                                signature: format!("rpc-price-{}-{now}", position.mint),
                                slot: 0,
                                instruction_index: 0,
                                mint: position.mint,
                                creator: position.creator,
                                venue: position.venue,
                                kind: EventKind::Price,
                                observed_ms: now,
                                blockchain_ms: None,
                                price: cache.price,
                                market_cap: cache.market_cap,
                                trader: None,
                                sol_amount: 0,
                                token_amount: 0,
                                speculative: false,
                                facts: Some(cache.facts),
                            })
                            .await?;
                        Ok::<(), anyhow::Error>(())
                    });
                }
                while let Some(result) = jobs.join_next().await {
                    failed |= !matches!(result, Ok(Ok(())));
                }
                let _ = engine
                    .service(ServiceStatus {
                        name: format!("{:?} position protection", network.mode()),
                        status: if failed { "FAILED" } else { "CONNECTED" }.into(),
                        last_success_ms: if failed {
                            None
                        } else {
                            Some(SystemClock.now_ms())
                        },
                        error: failed
                            .then(|| "Unable to refresh one or more open position markets".into()),
                        ..ServiceStatus::missing("Position protection")
                    })
                    .await;
            }
        });
    }
    let analyzer = if let Some(rpc) = main_rpc {
        Some(NetworkExecutor::analysis_only(rpc)?)
    } else {
        None
    };
    let (parsed, mut parsed_rx) = mpsc::channel::<feed::ParsedEvent>(2048);
    let mut feed_tasks = vec![];
    for (field, early) in [
        ("helius_preprocessed_url", true),
        ("helius_websocket_url", false),
    ] {
        if config.mode != Mode::Replay {
            if let Some(endpoint) = secret_value(field)? {
                let (status_tx, mut status_rx) =
                    watch::channel(ServiceStatus::missing("Helius Feed"));
                let output = parsed.clone();
                feed_tasks.push(tokio::spawn(feed::run(endpoint, early, output, status_tx)));
                let e = engine.clone();
                tokio::spawn(async move {
                    while status_rx.changed().await.is_ok() {
                        let status = status_rx.borrow_and_update().clone();
                        let _ = e.service(status).await;
                    }
                });
            }
        }
    }
    let checkpoint_store = store.clone();
    let checkpoint_engine = engine.clone();
    let checkpoint_interval = config.store.checkpoint_interval_secs;
    if checkpoint_interval > 0 {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(checkpoint_interval));
            loop {
                interval.tick().await;
                if checkpoint_engine.snapshot().unresolved_orders == 0
                    && checkpoint_store.checkpoint(false).await.is_err()
                {
                    tracing::warn!("checkpoint creation failed");
                }
            }
        });
    }
    let consumer_engine = engine.clone();
    let consumer_networks = networks.clone();
    tokio::spawn(async move {
        let (warm_tx, mut warm_rx) = mpsc::channel::<feed::ParsedEvent>(256);
        let warm_engine = consumer_engine.clone();
        let warm_networks = consumer_networks.clone();
        tokio::spawn(async move {
            while let Some(mut parsed) = warm_rx.recv().await {
                let selected = warm_networks
                    .iter()
                    .find(|n| n.mode() == warm_engine.snapshot().mode)
                    .cloned()
                    .or_else(|| analyzer.clone());
                if let Some(network) = selected {
                    if let Ok(cache) = network
                        .warm_market(parsed.event.mint, parsed.event.venue, parsed.pool)
                        .await
                    {
                        parsed.event.facts = Some(cache.facts);
                        parsed.event.price = cache.price;
                        parsed.event.market_cap = cache.market_cap;
                        parsed.event.speculative = false;
                        parsed.event.kind = EventKind::Price;
                        parsed.event.trader = None;
                        let _ = warm_engine.event(parsed.event).await;
                    }
                }
            }
        });
        let mut warmed = HashMap::<Key, Instant>::new();
        while let Some(parsed) = parsed_rx.recv().await {
            if warmed
                .get(&parsed.event.mint)
                .is_none_or(|t| t.elapsed() > Duration::from_secs(2))
                && warm_tx.try_send(parsed.clone()).is_ok()
            {
                warmed.insert(parsed.event.mint, Instant::now());
            }
            if warmed.len() > 10_000 {
                warmed.retain(|_, t| t.elapsed() < Duration::from_secs(30));
            }
            if consumer_engine.event(parsed.event).await.is_err() {
                break;
            }
        }
    });
    let app = Arc::new(App {
        engine: engine.clone(),
        store: store.clone(),
        vault,
        token,
        origin: config.browser_origin.clone(),
        sessions: Mutex::new(HashMap::new()),
        networks,
    });
    let router = Router::new()
        .route("/api/state", get(routes::state))
        .route("/api/action", post(routes::action))
        .route("/api/sell/{mint}", post(routes::sell))
        .route("/api/session", post(routes::session))
        .route("/api/history", get(routes::history))
        .route("/api/analytics", get(routes::analytics))
        .route("/api/analytics/buckets", get(routes::analytics_buckets))
        .route("/api/database/{operation}", post(routes::database))
        .route(
            "/api/credentials",
            get(routes::credentials).post(routes::save_credentials),
        )
        .route("/api/test/{service}", post(routes::connection_test))
        .route("/api/wallet", get(routes::wallet))
        .route("/api/wallet/balance", get(routes::wallet_balance))
        .route("/metrics", get(routes::metrics))
        .route("/stream", get(routes::websocket))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(app.clone(), routes::auth))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(bind=%config.bind,"trading API ready; mode remains paused until user starts");
    let shutdown_engine = engine.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            if shutdown_engine.snapshot().state != BotState::Stopping {
                let _ = shutdown_engine.action(sniper_engine::Action::Pause).await;
            }
            let snapshot = shutdown_engine.snapshot();
            if !snapshot.positions.is_empty() {
                tracing::warn!(
                    positions = snapshot.positions.len(),
                    "shutdown with open positions; recovery will resume protection"
                );
            }
        })
        .await?;
    engine.shutdown().await?;
    for task in feed_tasks {
        task.abort();
    }
    store.flush().await?;
    let _ = store.checkpoint(false).await;
    drop(signer_locks);
    Ok(())
}
