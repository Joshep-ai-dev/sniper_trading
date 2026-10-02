use sniper_domain::{Key, Mode};
use sniper_store::{mint_key, Mutation, Store, StoreConfig};
use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};
fn config(path: &std::path::Path) -> StoreConfig {
    StoreConfig {
        path: path.join("db"),
        wal_dir: path.join("wal"),
        checkpoint_path: path.join("checkpoint"),
        backup_path: path.join("backup"),
        min_free_bytes: 0,
        write_buffer_bytes: 1 << 20,
        block_cache_bytes: 1 << 20,
        ..Default::default()
    }
}
#[test]
fn crash_writer_child() {
    let Some(path) = std::env::var_os("SNIPER_CRASH_TEST_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let store = Store::open(config(&path), &format!("crash-{}", path.display())).unwrap();
    runtime
        .block_on(store.write(vec![Mutation::put(
                "purchased_tokens",
                mint_key(Mode::Live, Key([33; 32])),
                &true,
            )
            .unwrap()]))
        .unwrap();
    std::fs::write(path.join("ready"), b"WAL acknowledged").unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
#[test]
fn process_kill_preserves_synced_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_writer_child")
        .arg("--nocapture")
        .env("SNIPER_CRASH_TEST_DIRECTORY", tmp.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !tmp.path().join("ready").exists() {
        if start.elapsed() > Duration::from_secs(15) {
            let _ = child.kill();
            panic!("child did not acknowledge WAL");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let store = Store::open(
        config(tmp.path()),
        &format!("crash-{}", tmp.path().display()),
    )
    .unwrap();
    assert_eq!(
        store
            .get::<bool>("purchased_tokens", &mint_key(Mode::Live, Key([33; 32])))
            .unwrap(),
        Some(true)
    );
}
