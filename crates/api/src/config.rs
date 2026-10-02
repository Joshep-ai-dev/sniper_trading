use serde::Deserialize;
use sniper_domain::*;
use sniper_store::StoreConfig;
use std::path::PathBuf;
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub mode: Mode,
    pub bind: String,
    pub browser_origin: String,
    pub secret_path: PathBuf,
    pub store: StoreConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub paper: PaperConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Paper,
            bind: "127.0.0.1:8787".into(),
            browser_origin: "https://localhost:3443".into(),
            secret_path: "secrets".into(),
            store: StoreConfig::default(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
            paper: PaperConfig::default(),
        }
    }
}
