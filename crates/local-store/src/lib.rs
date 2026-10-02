//! Synchronous WAL writes run on Tokio's blocking pool behind a bounded semaphore.
use anyhow::{bail, ensure, Context, Result};
use fs2::FileExt;
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType,
    Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sniper_domain::{Aggregate, ClosedTrade, Key, Mode, SCHEMA_VERSION};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::Semaphore;

pub const COLUMN_FAMILIES: &[&str] = &[
    "system",
    "config",
    "secrets_metadata",
    "purchased_tokens",
    "purchase_reservations",
    "detected_tokens",
    "creator_intelligence",
    "wallet_intelligence",
    "token_scores",
    "orders",
    "fills",
    "positions",
    "closed_positions",
    "trade_history",
    "strategy_versions",
    "market_events",
    "price_events",
    "latency_events",
    "analytics_daily",
    "analytics_hourly",
    "analytics_minute",
    "analytics_five_minute",
    "analytics_index",
    "bot_state",
    "recovery_state",
    "audit_history",
    "replay_data",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreConfig {
    pub path: PathBuf,
    pub wal_dir: PathBuf,
    pub checkpoint_path: PathBuf,
    pub backup_path: PathBuf,
    pub max_open_files: i32,
    pub write_buffer_bytes: usize,
    pub write_buffer_count: i32,
    pub block_cache_bytes: usize,
    pub compaction_threads: i32,
    pub flush_threads: i32,
    pub min_free_bytes: u64,
    pub max_size_warning_bytes: u64,
    pub checkpoint_interval_secs: u64,
    pub checkpoint_retention: usize,
}
impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: "data/rocksdb".into(),
            wal_dir: "data/wal".into(),
            checkpoint_path: "data/checkpoints".into(),
            backup_path: "data/backups".into(),
            max_open_files: 512,
            write_buffer_bytes: 64 << 20,
            write_buffer_count: 3,
            block_cache_bytes: 128 << 20,
            compaction_threads: 2,
            flush_threads: 1,
            min_free_bytes: 1 << 30,
            max_size_warning_bytes: 50 << 30,
            checkpoint_interval_secs: 900,
            checkpoint_retention: 5,
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct DbHealth {
    pub status: String,
    pub path: String,
    pub size_bytes: u64,
    pub free_bytes: u64,
    pub wal_enabled: bool,
    pub sync_critical: bool,
    pub write_latency_us: u64,
    pub pending_compaction_bytes: u64,
    pub column_families: usize,
    pub last_write_ms: u64,
    pub last_checkpoint_ms: u64,
    pub last_backup_ms: u64,
}

pub struct Ownership {
    _files: Vec<File>,
}
impl Ownership {
    pub fn acquire(database: &Path, wallet: &str) -> Result<Self> {
        fs::create_dir_all(database)?;
        let root = std::env::temp_dir().join("sniper-trading-wallet-locks");
        fs::create_dir_all(&root)?;
        let identity = format!("{:x}", Sha256::digest(wallet.as_bytes()));
        let mut files = Vec::new();
        for path in [database.join("engine.owner.lock"), root.join(identity)] {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            file.try_lock_exclusive()
                .context("another engine owns this wallet or database")?;
            files.push(file);
        }
        Ok(Self { _files: files })
    }
}
pub fn mint_key(mode: Mode, mint: Key) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.push(mode.byte());
    key.extend_from_slice(&mint.0);
    key
}
pub fn time_key(mode: Mode, time: u64, id: u64) -> Vec<u8> {
    let mut key = vec![mode.byte()];
    key.extend_from_slice(&time.to_be_bytes());
    key.extend_from_slice(&id.to_be_bytes());
    key
}
pub fn order_key(mode: Mode, id: u64) -> Vec<u8> {
    let mut key = vec![mode.byte()];
    key.extend_from_slice(&id.to_be_bytes());
    key
}
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = SCHEMA_VERSION.to_le_bytes().to_vec();
    bytes.extend(bincode::serialize(value)?);
    Ok(bytes)
}
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ensure!(bytes.len() >= 4, "truncated local record");
    ensure!(
        u32::from_le_bytes(bytes[..4].try_into()?) == SCHEMA_VERSION,
        "unsupported record schema"
    );
    Ok(bincode::deserialize(&bytes[4..])?)
}
#[derive(Clone)]
pub enum Mutation {
    Put(&'static str, Vec<u8>, Vec<u8>),
    Delete(&'static str, Vec<u8>),
}
impl Mutation {
    pub fn put<T: Serialize>(cf: &'static str, key: Vec<u8>, value: &T) -> Result<Self> {
        Ok(Self::Put(cf, key, encode(value)?))
    }
}
pub struct Store {
    db: Arc<DB>,
    pub config: StoreConfig,
    healthy: AtomicBool,
    writes_enabled: AtomicBool,
    last_write: AtomicU64,
    write_us: AtomicU64,
    last_checkpoint: AtomicU64,
    last_backup: AtomicU64,
    permits: Arc<Semaphore>,
    checkpoint_guard: std::sync::Mutex<()>,
    analytics_guard: std::sync::Mutex<()>,
    _ownership: Ownership,
}
impl Store {
    pub fn open(config: StoreConfig, wallet: &str) -> Result<Arc<Self>> {
        ensure!(
            config.write_buffer_bytes >= 1 << 20 && config.block_cache_bytes >= 1 << 20,
            "database buffers too small"
        );
        ensure!(
            config.compaction_threads > 0 && config.flush_threads > 0,
            "invalid background thread count"
        );
        ensure!(
            config.checkpoint_retention > 0,
            "checkpoint retention must be positive"
        );
        let ownership = Ownership::acquire(&config.path, wallet)?;
        fs::create_dir_all(&config.wal_dir)?;
        for path in [&config.checkpoint_path, &config.backup_path] {
            fs::create_dir_all(path)?;
            ensure!(
                !fs::canonicalize(path)?.starts_with(fs::canonicalize(&config.path)?),
                "backup must be outside database"
            );
        }
        let cache = Cache::new_lru_cache(config.block_cache_bytes);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(config.max_open_files);
        opts.set_wal_dir(&config.wal_dir);
        opts.set_max_background_jobs(
            config
                .compaction_threads
                .saturating_add(config.flush_threads),
        );
        let cfs = COLUMN_FAMILIES.iter().map(|name| {
            let mut cf = Options::default();
            cf.set_write_buffer_size(config.write_buffer_bytes);
            cf.set_max_write_buffer_number(config.write_buffer_count);
            cf.set_compression_type(DBCompressionType::Lz4);
            cf.set_block_based_table_factory(&table);
            ColumnFamilyDescriptor::new(*name, cf)
        });
        let db = Arc::new(DB::open_cf_descriptors(&opts, &config.path, cfs)?);
        let store = Arc::new(Self {
            db,
            config,
            healthy: AtomicBool::new(true),
            writes_enabled: AtomicBool::new(true),
            last_write: AtomicU64::new(0),
            write_us: AtomicU64::new(0),
            last_checkpoint: AtomicU64::new(0),
            last_backup: AtomicU64::new(0),
            permits: Arc::new(Semaphore::new(32)),
            checkpoint_guard: std::sync::Mutex::new(()),
            analytics_guard: std::sync::Mutex::new(()),
            _ownership: ownership,
        });
        match store.get::<u32>("system", b"schema")? {
            Some(version) => ensure!(
                version == SCHEMA_VERSION,
                "schema migration required; refusing trading"
            ),
            None => store.write_sync(vec![Mutation::put(
                "system",
                b"schema".to_vec(),
                &SCHEMA_VERSION,
            )?])?,
        }
        Ok(store)
    }
    pub fn writable(&self) -> bool {
        self.healthy.load(Ordering::Acquire) && self.writes_enabled.load(Ordering::Acquire)
    }
    pub fn disable_writes(&self) {
        self.writes_enabled.store(false, Ordering::Release);
    }
    fn write_sync(&self, mutations: Vec<Mutation>) -> Result<()> {
        ensure!(self.writable(), "LOCAL_PERSISTENCE_UNAVAILABLE");
        let started = Instant::now();
        let mut batch = WriteBatch::default();
        for m in mutations {
            match m {
                Mutation::Put(cf, k, v) => batch.put_cf(
                    &self.db.cf_handle(cf).context("missing column family")?,
                    k,
                    v,
                ),
                Mutation::Delete(cf, k) => {
                    ensure!(
                        cf != "purchased_tokens",
                        "permanent buy registry cannot be deleted"
                    );
                    batch.delete_cf(&self.db.cf_handle(cf).context("missing column family")?, k);
                }
            }
        }
        let mut options = WriteOptions::default();
        options.set_sync(true);
        options.disable_wal(false);
        if let Err(error) = self.db.write_opt(batch, &options) {
            self.healthy.store(false, Ordering::Release);
            bail!("local persistence failure: {error}");
        }
        self.write_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.last_write.store(now_ms(), Ordering::Relaxed);
        Ok(())
    }
    pub async fn write(self: &Arc<Self>, mutations: Vec<Mutation>) -> Result<()> {
        let permit = self.permits.clone().acquire_owned().await?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store.write_sync(mutations)
        })
        .await?
    }
    pub fn get<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> Result<Option<T>> {
        let handle = self.db.cf_handle(cf).context("missing column family")?;
        self.db
            .get_cf(&handle, key)?
            .map(|b| decode(&b))
            .transpose()
    }
    /// Startup/background only; never call from entry decisions.
    pub fn load<T: DeserializeOwned>(&self, cf: &str, mode: Mode) -> Result<Vec<T>> {
        let h = self.db.cf_handle(cf).context("missing column family")?;
        let prefix = [mode.byte()];
        self.db
            .iterator_cf(&h, IteratorMode::From(&prefix, Direction::Forward))
            .take_while(|r| {
                r.as_ref()
                    .map(|(k, _)| k.starts_with(&prefix))
                    .unwrap_or(true)
            })
            .map(|r| {
                let (_, v) = r?;
                decode(&v)
            })
            .collect()
    }
    pub fn page<T: DeserializeOwned>(
        &self,
        cf: &str,
        mode: Mode,
        before: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, T)>> {
        let h = self.db.cf_handle(cf).context("missing column family")?;
        let start = before.clone().unwrap_or_else(|| vec![mode.byte() + 1]);
        self.db
            .iterator_cf(&h, IteratorMode::From(&start, Direction::Reverse))
            .take_while(|r| {
                r.as_ref()
                    .map(|(k, _)| k.first() == Some(&mode.byte()))
                    .unwrap_or(true)
            })
            .filter(|r| {
                r.as_ref()
                    .map(|(k, _)| before.as_ref().is_none_or(|b| k.as_ref() != b.as_slice()))
                    .unwrap_or(true)
            })
            .take(limit.min(500))
            .map(|r| {
                let (k, v) = r?;
                Ok((k.to_vec(), decode(&v)?))
            })
            .collect()
    }
    pub fn health(&self) -> Result<DbHealth> {
        let mut size = 0;
        let mut pending = 0;
        for name in COLUMN_FAMILIES {
            let cf = self.db.cf_handle(name).context("missing column family")?;
            size += self
                .db
                .property_int_value_cf(&cf, "rocksdb.total-sst-files-size")?
                .unwrap_or(0);
            pending += self
                .db
                .property_int_value_cf(&cf, "rocksdb.estimate-pending-compaction-bytes")?
                .unwrap_or(0);
        }
        let free = fs2::available_space(&self.config.path)?;
        Ok(DbHealth {
            status: if !self.writable() {
                "FAILED"
            } else if free < self.config.min_free_bytes || size > self.config.max_size_warning_bytes
            {
                "DEGRADED"
            } else {
                "READY"
            }
            .into(),
            path: self.config.path.display().to_string(),
            size_bytes: size,
            free_bytes: free,
            wal_enabled: true,
            sync_critical: true,
            write_latency_us: self.write_us.load(Ordering::Relaxed),
            pending_compaction_bytes: pending,
            column_families: COLUMN_FAMILIES.len(),
            last_write_ms: self.last_write.load(Ordering::Relaxed),
            last_checkpoint_ms: self.last_checkpoint.load(Ordering::Relaxed),
            last_backup_ms: self.last_backup.load(Ordering::Relaxed),
        })
    }
    pub async fn checkpoint(self: &Arc<Self>, backup: bool) -> Result<PathBuf> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = store
                .checkpoint_guard
                .lock()
                .map_err(|_| anyhow::anyhow!("checkpoint worker failed"))?;
            let timestamp = now_ms();
            let root = if backup {
                &store.config.backup_path
            } else {
                &store.config.checkpoint_path
            };
            let path = root.join(format!("checkpoint-{timestamp}"));
            Checkpoint::new(&store.db)?.create_checkpoint(&path)?;
            if backup {
                store.last_backup.store(timestamp, Ordering::Relaxed)
            } else {
                store.last_checkpoint.store(timestamp, Ordering::Relaxed)
            };
            if !backup {
                let root = fs::canonicalize(root)?;
                let mut checkpoints = Vec::new();
                for entry in fs::read_dir(&root)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }
                    let name = entry.file_name();
                    let Some(timestamp) = name
                        .to_str()
                        .and_then(|n| n.strip_prefix("checkpoint-"))
                        .and_then(|n| n.parse::<u64>().ok())
                    else {
                        continue;
                    };
                    checkpoints.push((timestamp, entry.path()));
                }
                checkpoints.sort_by_key(|a| std::cmp::Reverse(a.0));
                for (_, old) in checkpoints
                    .into_iter()
                    .skip(store.config.checkpoint_retention)
                {
                    let target = fs::canonicalize(old)?;
                    ensure!(
                        target.parent() == Some(root.as_path()),
                        "checkpoint retention path escaped its directory"
                    );
                    fs::remove_dir_all(target)?;
                }
            }
            Ok(path)
        })
        .await?
    }
    pub async fn verify(self: &Arc<Self>) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            for name in COLUMN_FAMILIES {
                let cf = store.db.cf_handle(name).context("missing column family")?;
                for item in store.db.iterator_cf(&cf, IteratorMode::Start) {
                    let (_, v) = item?;
                    ensure!(v.len() >= 4, "truncated record");
                    ensure!(
                        u32::from_le_bytes(v[..4].try_into()?) == SCHEMA_VERSION,
                        "unsupported record schema"
                    );
                }
            }
            store.db.flush_wal(true)?;
            Ok(())
        })
        .await?
    }
    /// Cold worker only. The marker and all four buckets commit together, so restart repair is idempotent.
    pub fn refresh_analytics(&self) -> Result<usize> {
        let _guard = self
            .analytics_guard
            .lock()
            .map_err(|_| anyhow::anyhow!("analytics worker failed"))?;
        let cf = self
            .db
            .cf_handle("trade_history")
            .context("trade history column family")?;
        let mut count = 0;
        for item in self.db.iterator_cf(&cf, IteratorMode::Start) {
            let (key, value) = item?;
            if self.get::<bool>("analytics_index", &key)?.is_some() {
                continue;
            }
            let trade: ClosedTrade = decode(&value)?;
            let mut batch = Vec::with_capacity(5);
            for (cf, width) in [
                ("analytics_minute", 60_000),
                ("analytics_five_minute", 300_000),
                ("analytics_hourly", 3_600_000),
                ("analytics_daily", 86_400_000),
            ] {
                let mut bucket_key = vec![trade.position.mode.byte()];
                bucket_key
                    .extend_from_slice(&(trade.exit.timestamp_ms / width * width).to_be_bytes());
                let mut aggregate = self.get::<Aggregate>(cf, &bucket_key)?.unwrap_or_default();
                aggregate.add(&trade);
                batch.push(Mutation::put(cf, bucket_key, &aggregate)?);
            }
            batch.push(Mutation::put("analytics_index", key.to_vec(), &true)?);
            self.write_sync(batch)?;
            count += 1;
        }
        Ok(count)
    }
    pub async fn compact(self: &Arc<Self>) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            for name in COLUMN_FAMILIES {
                let cf = store.db.cf_handle(name).context("missing column family")?;
                store.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
            }
            Ok(())
        })
        .await?
    }
    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.db.flush_wal(true)?;
            store.db.flush()?;
            Ok(())
        })
        .await?
    }
}
fn now_ms() -> u64 {
    use sniper_domain::Clock;
    sniper_domain::SystemClock.now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    pub fn config(t: &Path) -> StoreConfig {
        StoreConfig {
            path: t.join("db"),
            wal_dir: t.join("wal"),
            checkpoint_path: t.join("checkpoints"),
            backup_path: t.join("backup"),
            min_free_bytes: 0,
            write_buffer_bytes: 1 << 20,
            block_cache_bytes: 1 << 20,
            ..Default::default()
        }
    }
    #[tokio::test]
    async fn durable_and_mode_isolated() {
        let t = tempfile::tempdir().unwrap();
        let c = config(t.path());
        let s = Store::open(c.clone(), "store-test").unwrap();
        let k = mint_key(Mode::Live, Key([2; 32]));
        s.write(vec![
            Mutation::put("purchased_tokens", k.clone(), &true).unwrap()
        ])
        .await
        .unwrap();
        assert!(s
            .get::<bool>("purchased_tokens", &mint_key(Mode::Paper, Key([2; 32])))
            .unwrap()
            .is_none());
        assert!(s
            .write(vec![Mutation::Delete("purchased_tokens", k.clone())])
            .await
            .is_err());
        drop(s);
        let s = Store::open(c, "store-test").unwrap();
        assert_eq!(s.get::<bool>("purchased_tokens", &k).unwrap(), Some(true));
    }
    #[test]
    fn process_lock_cross_database() {
        let t = tempfile::tempdir().unwrap();
        let a = Ownership::acquire(&t.path().join("a"), "exclusive-test").unwrap();
        assert!(Ownership::acquire(&t.path().join("b"), "exclusive-test").is_err());
        drop(a);
        assert!(Ownership::acquire(&t.path().join("b"), "exclusive-test").is_ok());
    }
    #[test]
    fn invalid_schema_rejected() {
        assert!(decode::<u64>(&[2, 0, 0, 0, 1]).is_err());
        assert!(decode::<u64>(&[1]).is_err());
    }
    #[tokio::test]
    async fn checkpoints_retain_recent_and_preserve_other_directories() {
        let t = tempfile::tempdir().unwrap();
        let mut c = config(t.path());
        c.checkpoint_retention = 2;
        let s = Store::open(c, &format!("checkpoint-{}", t.path().display())).unwrap();
        let unrelated = s.config.checkpoint_path.join("operator-notes");
        fs::create_dir(&unrelated).unwrap();
        fs::write(unrelated.join("preserve.txt"), "keep").unwrap();
        s.write(vec![Mutation::put(
            "purchased_tokens",
            mint_key(Mode::Live, Key([9; 32])),
            &true,
        )
        .unwrap()])
            .await
            .unwrap();
        for _ in 0..3 {
            s.checkpoint(false).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(fs::read_dir(&s.config.checkpoint_path).unwrap().count(), 3);
        assert!(unrelated.join("preserve.txt").exists());
        assert_eq!(
            s.get::<bool>("purchased_tokens", &mint_key(Mode::Live, Key([9; 32])))
                .unwrap(),
            Some(true)
        );
        s.verify().await.unwrap();
    }
}
