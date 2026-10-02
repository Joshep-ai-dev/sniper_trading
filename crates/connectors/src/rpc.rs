use anyhow::{bail, ensure, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use url::Url;

/// This type deliberately has no Debug implementation; RPC URLs can contain credentials.
#[derive(Clone)]
pub struct Rpc {
    client: Client,
    endpoint: Arc<str>,
}
pub fn validate_endpoint(endpoint: &str, websocket: bool) -> Result<()> {
    let url = Url::parse(endpoint)?;
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "URL userinfo is forbidden"
    );
    ensure!(
        url.scheme() == if websocket { "wss" } else { "https" },
        "endpoint requires TLS"
    );
    ensure!(url.fragment().is_none(), "endpoint fragments are forbidden");
    Ok(())
}
impl Rpc {
    pub fn with_path(&self, path: &str) -> Result<Self> {
        let mut url = Url::parse(&self.endpoint)?;
        url.set_path(path);
        Ok(Self {
            client: self.client.clone(),
            endpoint: Arc::from(url.to_string()),
        })
    }
    pub async fn ping(&self) -> Result<()> {
        let mut url = Url::parse(&self.endpoint)?;
        url.set_path("/ping");
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("endpoint ping failed"))?;
        ensure!(response.status().is_success(), "endpoint ping rejected");
        Ok(())
    }
    pub fn new(endpoint: String) -> Result<Self> {
        validate_endpoint(&endpoint, false)?;
        Ok(Self {
            client: Client::builder()
                .connect_timeout(Duration::from_millis(1000))
                .timeout(Duration::from_secs(5))
                .pool_idle_timeout(Duration::from_secs(90))
                .tcp_nodelay(true)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: Arc::from(endpoint),
        })
    }
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        // Never propagate reqwest errors with credential-bearing URLs into logs or API errors.
        let response = self
            .client
            .post(self.endpoint.as_ref())
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("endpoint timeout or connection failure"))?;
        ensure!(
            response.status().is_success(),
            "endpoint returned HTTP {}",
            response.status().as_u16()
        );
        let body: Value = response
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("invalid RPC response"))?;
        if let Some(error) = body.get("error") {
            bail!("RPC rejected request (code {})", error["code"]);
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("RPC response missing result"))
    }
    pub async fn genesis(&self) -> Result<String> {
        Ok(self
            .call("getGenesisHash", json!([]))
            .await?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid genesis"))?
            .into())
    }
    pub async fn slot(&self) -> Result<u64> {
        self.call("getSlot", json!([{"commitment":"confirmed"}]))
            .await?
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("invalid slot"))
    }
    pub async fn balance(&self, address: &str) -> Result<u64> {
        self.call("getBalance", json!([address,{"commitment":"finalized"}]))
            .await?["value"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("invalid balance"))
    }
}
pub const MAINNET_GENESIS: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp";
pub const DEVNET_GENESIS: &str = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1";
