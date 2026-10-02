use crate::App;
use anyhow::Result;
use axum::{
    extract::Request,
    extract::{
        ws::{Message, WebSocket},
        Path, Query, State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sniper_connectors::{rpc::Rpc, secrets::SecretProvider};
use sniper_domain::*;
use sniper_engine::Action;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;

pub struct ApiError(pub anyhow::Error);
impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::CONFLICT,
            Json(json!({"error":self.0.to_string()})),
        )
            .into_response()
    }
}
type ApiResult<T> = std::result::Result<T, ApiError>;
fn require(condition: bool, message: &str) -> Result<()> {
    if !condition {
        return Err(anyhow::anyhow!(message.to_string()));
    }
    Ok(())
}
pub async fn auth(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    if let Some(origin) = request.headers().get("origin") {
        if origin.to_str().ok() != Some(app.origin.as_str()) {
            return (StatusCode::FORBIDDEN, "Origin rejected").into_response();
        }
    }
    let bearer = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let authorized =
        bearer.is_some_and(|token| bool::from(token.as_bytes().ct_eq(app.token.as_bytes())));
    let cookie = request
        .headers()
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.split(';')
                .find_map(|p| p.trim().strip_prefix("sniper_session="))
        });
    let cookie_authorized = {
        let session = app.sessions.lock();
        cookie.is_some_and(|c| {
            session
                .get(c)
                .is_some_and(|t| t.elapsed() < Duration::from_secs(3600))
        })
    };
    if !authorized && !cookie_authorized {
        return (StatusCode::UNAUTHORIZED, "Authentication required").into_response();
    }
    next.run(request).await
}
pub async fn state(State(app): State<Arc<App>>) -> Json<Value> {
    Json(
        serde_json::to_value(app.engine.snapshot().as_ref())
            .unwrap_or(json!({"error":"serialization failure"})),
    )
}
pub async fn action(
    State(app): State<Arc<App>>,
    Json(action): Json<Action>,
) -> ApiResult<Json<Value>> {
    app.engine.action(action).await?;
    Ok(Json(json!({"ok":true})))
}
pub async fn sell(State(app): State<Arc<App>>, Path(mint): Path<Key>) -> ApiResult<Json<Value>> {
    app.engine.sell(mint).await?;
    Ok(Json(json!({"ok":true})))
}
pub async fn session(State(app): State<Arc<App>>) -> ApiResult<Response> {
    let session = uuid_token();
    let mut sessions = app.sessions.lock();
    sessions.retain(|_, t| t.elapsed() < Duration::from_secs(3600));
    require(sessions.len() < 100, "too many active sessions")?;
    sessions.insert(session.clone(), Instant::now());
    Ok((
        [(
            "set-cookie",
            format!(
                "sniper_session={session}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=3600"
            ),
        )],
        Json(json!({"ok":true})),
    )
        .into_response())
}
fn uuid_token() -> String {
    use std::fmt::Write;
    let mut s = String::new();
    for _ in 0..4 {
        let _ = write!(s, "{:016x}", rand::random::<u64>());
    }
    s
}
#[derive(Deserialize)]
pub struct HistoryQuery {
    pub mode: Option<Mode>,
    pub limit: Option<usize>,
    pub before: Option<String>,
}
pub async fn history(
    State(app): State<Arc<App>>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<Json<Value>> {
    let mode = q.mode.unwrap_or(Mode::Live);
    let store = app.store.clone();
    let before = q.before.map(|s| decode_hex(&s)).transpose()?;
    let page = tokio::task::spawn_blocking(move || {
        store.page::<ClosedTrade>("trade_history", mode, before, q.limit.unwrap_or(100))
    })
    .await??;
    let cursor = page.last().map(|(k, _)| encode_hex(k));
    Ok(Json(
        json!({"trades":page.into_iter().map(|(_,t)|t).collect::<Vec<_>>(),"cursor":cursor}),
    ))
}
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn decode_hex(s: &str) -> Result<Vec<u8>> {
    require(s.len() == 34, "invalid history cursor")?;
    Ok((0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<std::result::Result<_, _>>()?)
}
pub async fn analytics(
    State(app): State<Arc<App>>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<Json<Value>> {
    let store = app.store.clone();
    let mode = q.mode.unwrap_or(Mode::Live);
    let result = tokio::task::spawn_blocking(move || -> Result<_> {
        Ok(store
            .get::<Aggregate>("config", &[mode.byte(), 2])?
            .unwrap_or_default())
    })
    .await??;
    Ok(Json(json!(result)))
}
#[derive(Deserialize)]
pub struct BucketQuery {
    mode: Option<Mode>,
    window: Option<String>,
    limit: Option<usize>,
    before: Option<String>,
}
pub async fn analytics_buckets(
    State(app): State<Arc<App>>,
    Query(q): Query<BucketQuery>,
) -> ApiResult<Json<Value>> {
    let cf = match q.window.as_deref().unwrap_or("day") {
        "minute" => "analytics_minute",
        "five_minute" => "analytics_five_minute",
        "hour" => "analytics_hourly",
        "day" => "analytics_daily",
        _ => return Err(anyhow::anyhow!("unknown analytics window").into()),
    };
    let store = app.store.clone();
    let before = q.before.map(|s| decode_hex(&s)).transpose()?;
    let rows = tokio::task::spawn_blocking(move || {
        store.page::<Aggregate>(
            cf,
            q.mode.unwrap_or(Mode::Live),
            before,
            q.limit.unwrap_or(100),
        )
    })
    .await??;
    let cursor = rows.last().map(|(key, _)| encode_hex(key));
    let buckets: Vec<_> = rows
        .into_iter()
        .map(|(key, aggregate)| -> Result<_> {
            require(key.len() == 9, "invalid analytics bucket key")?;
            Ok(json!({"start_ms":u64::from_be_bytes(key[1..].try_into()?),"aggregate":aggregate}))
        })
        .collect::<Result<_>>()?;
    Ok(Json(json!({"buckets":buckets,"cursor":cursor})))
}
pub async fn database(
    State(app): State<Arc<App>>,
    Path(operation): Path<String>,
) -> ApiResult<Json<Value>> {
    let s = app.engine.snapshot();
    match operation.as_str() {
        "test" => {
            let store = app.store.clone();
            return Ok(Json(json!(
                tokio::task::spawn_blocking(move || store.health()).await??
            )));
        }
        "checkpoint" => {
            return Ok(Json(json!({"path":app.store.checkpoint(false).await?})));
        }
        "backup" => {
            return Ok(Json(json!({"path":app.store.checkpoint(true).await?})));
        }
        "verify" => app.store.verify().await?,
        "compact" => {
            require(
                s.state != BotState::Running && s.positions.is_empty() && s.unresolved_orders == 0,
                "pause and close exposure before compaction",
            )?;
            app.store.compact().await?;
        }
        _ => return Err(anyhow::anyhow!("unknown database operation").into()),
    }
    Ok(Json(json!({"ok":true})))
}
const SECRET_FIELDS: &[&str] = &[
    "helius_api_key",
    "helius_rpc_url",
    "helius_websocket_url",
    "helius_preprocessed_url",
    "helius_laserstream_url",
    "helius_sender_url",
    "jito_url",
    "jupiter_api_key",
    "jupiter_url",
    "devnet_rpc_url",
];
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub field: String,
    pub value: Option<String>,
}
pub async fn credentials(State(app): State<Arc<App>>) -> ApiResult<Json<Value>> {
    let vault = app.vault.clone();
    let configured = tokio::task::spawn_blocking(move || -> Result<_> {
        let mut fields = serde_json::Map::new();
        for field in SECRET_FIELDS {
            fields.insert(
                (*field).into(),
                json!({"configured":vault.get(field)?.is_some(),"masked":"••••••••"}),
            );
        }
        Ok(fields)
    })
    .await??;
    Ok(Json(json!(configured)))
}
pub async fn save_credentials(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(mut credential): Json<Credential>,
) -> ApiResult<Json<Value>> {
    require(
        headers.get("x-sniper-https").and_then(|s| s.to_str().ok()) == Some("true"),
        "credentials require HTTPS",
    )?;
    require(
        SECRET_FIELDS.contains(&credential.field.as_str()),
        "unknown credential field",
    )?;
    if let Some(value) = &credential.value {
        if credential.field.ends_with("url") {
            sniper_connectors::rpc::validate_endpoint(
                value,
                credential.field.contains("websocket") || credential.field.contains("preprocessed"),
            )?;
        }
    }
    let vault = app.vault.clone();
    let field = credential.field.clone();
    let value = zeroize::Zeroizing::new(credential.value.take().unwrap_or_default());
    tokio::task::spawn_blocking(move || -> Result<()> {
        if value.is_empty() {
            vault.clear(&field)
        } else {
            vault.save(&field, &value)
        }
    })
    .await??;
    let now = SystemClock.now_ms();
    app.store
        .write(vec![sniper_store::Mutation::put(
            "audit_history",
            sniper_store::time_key(app.engine.snapshot().mode, now, 0),
            &Audit {
                timestamp_ms: now,
                mode: app.engine.snapshot().mode,
                event: "CREDENTIAL_UPDATED".into(),
                result: format!(
                    "{} updated; restart paused engine to activate",
                    credential.field
                ),
            },
        )?])
        .await?;
    Ok(Json(json!({"ok":true,"restart_required":true})))
}
pub async fn connection_test(
    State(app): State<Arc<App>>,
    Path(service): Path<String>,
) -> ApiResult<Json<Value>> {
    let field = match service.as_str() {
        "helius" => "helius_rpc_url",
        "devnet" => "devnet_rpc_url",
        "jito" => "jito_url",
        "jupiter" => "jupiter_url",
        _ => return Err(anyhow::anyhow!("unknown service").into()),
    };
    let vault = app.vault.clone();
    let endpoint = tokio::task::spawn_blocking(move || vault.get(field))
        .await??
        .ok_or_else(|| anyhow::anyhow!("service not configured"))?;
    let rpc = Rpc::new(endpoint.to_string())?;
    let started = Instant::now();
    let result = if service == "jito" {
        rpc.with_path("/api/v1/bundles")?
            .call("getTipAccounts", json!([]))
            .await?
    } else if service == "helius" || service == "devnet" {
        let genesis = rpc.genesis().await?;
        if service == "devnet" {
            require(
                genesis == sniper_connectors::rpc::DEVNET_GENESIS,
                "wrong Devnet network",
            )?;
        }
        json!({"slot":rpc.slot().await?,"network":if genesis==sniper_connectors::rpc::MAINNET_GENESIS{"Mainnet"}else if genesis==sniper_connectors::rpc::DEVNET_GENESIS{"Devnet"}else{"Unknown"}})
    } else {
        return Err(anyhow::anyhow!(
            "Jupiter optional routing is disabled; no impact on early entries"
        )
        .into());
    };
    Ok(Json(
        json!({"status":"CONNECTED","latency_ms":started.elapsed().as_millis(),"last_success_ms":SystemClock.now_ms(),"details":result}),
    ))
}
pub async fn wallet(State(app): State<Arc<App>>) -> Json<Value> {
    let wallets:Vec<_>=app.networks.iter().map(|n|json!({"mode":n.mode(),"address":n.address,"balance":n.warm.read().balance,"status":if n.ready().iter().all(|c|c.pass){"READY"}else{"LOCKED"},"network":if n.mode()==Mode::Live{"Mainnet"}else{"Devnet"}})).collect();
    Json(json!({"trading_wallets":wallets}))
}
#[derive(Deserialize)]
pub struct WalletQuery {
    pub address: Key,
}
pub async fn wallet_balance(
    State(app): State<Arc<App>>,
    Query(q): Query<WalletQuery>,
) -> ApiResult<Json<Value>> {
    let vault = app.vault.clone();
    let endpoint = tokio::task::spawn_blocking(move || vault.get("helius_rpc_url"))
        .await??
        .ok_or_else(|| anyhow::anyhow!("Helius RPC not configured"))?;
    let rpc = Rpc::new(endpoint.to_string())?;
    require(
        rpc.genesis().await? == sniper_connectors::rpc::MAINNET_GENESIS,
        "wrong network",
    )?;
    Ok(Json(
        json!({"balance":rpc.balance(&q.address.to_string()).await?,"network":"Mainnet","updated_ms":SystemClock.now_ms()}),
    ))
}
pub async fn metrics(State(app): State<Arc<App>>) -> ApiResult<Response> {
    Ok((
        [("content-type", "text/plain; version=0.0.4")],
        app.engine.metrics.render()?,
    )
        .into_response())
}
pub async fn websocket(State(app): State<Arc<App>>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| publish(socket, app))
}
async fn publish(mut socket: WebSocket, app: Arc<App>) {
    let mut rx = app.engine.updates.subscribe();
    let started = Instant::now();
    loop {
        if started.elapsed() > Duration::from_secs(3600) {
            break;
        }
        let snapshot = match rx.recv().await {
            Ok(s) => s,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => app.engine.snapshot(),
            Err(_) => break,
        };
        let Ok(json) = serde_json::to_string(snapshot.as_ref()) else {
            break;
        };
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                socket.send(Message::Text(json.into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, middleware, routing::get, Router};
    use sniper_connectors::execution::PaperExecutor;
    use sniper_engine::Engine;
    use sniper_store::{Store, StoreConfig};
    use std::collections::HashMap;
    use tower::ServiceExt;
    #[tokio::test]
    async fn api_auth_and_origin_enforced() {
        let tmp = tempfile::tempdir().unwrap();
        let c = StoreConfig {
            path: tmp.path().join("db"),
            wal_dir: tmp.path().join("wal"),
            checkpoint_path: tmp.path().join("checkpoint"),
            backup_path: tmp.path().join("backup"),
            min_free_bytes: 0,
            write_buffer_bytes: 1 << 20,
            block_cache_bytes: 1 << 20,
            ..Default::default()
        };
        let store = Store::open(c, &format!("auth-{}", tmp.path().display())).unwrap();
        let executor: Arc<dyn TradeExecutor> =
            Arc::new(PaperExecutor::new(Mode::Replay, PaperConfig::default()).unwrap());
        let engine = Engine::launch(
            store.clone(),
            Mode::Replay,
            StrategyConfig::default(),
            RiskConfig::default(),
            5 * SOL,
            HashMap::from([(Mode::Replay, executor)]),
            Arc::new(SystemClock),
        )
        .await
        .unwrap();
        let app = Arc::new(App {
            engine: engine.clone(),
            store,
            vault: Arc::new(
                sniper_connectors::secrets::SecretVault::new(
                    tmp.path().join("secrets"),
                    &"12".repeat(32),
                )
                .unwrap(),
            ),
            token: "local-test-token-at-least-32-characters".into(),
            origin: "https://terminal.test".into(),
            sessions: parking_lot::Mutex::new(HashMap::new()),
            networks: vec![],
        });
        let router = Router::new()
            .route("/api/state", get(state))
            .layer(middleware::from_fn_with_state(app.clone(), auth))
            .with_state(app.clone());
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/state")
                    .header("authorization", format!("Bearer {}", app.token))
                    .header("origin", "https://attacker.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/state")
                    .header("authorization", format!("Bearer {}", app.token))
                    .header("origin", "https://terminal.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&body).contains(&app.token));
        engine.shutdown().await.unwrap();
    }
}
