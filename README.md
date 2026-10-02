# Sniper trading

Rust / Tokio / Axum Solana trading engine with RocksDB local persistence and a Next.js desktop terminal. The default mode is **PAPER**, paused. There are no remote databases.

Source architecture and integration boundaries: [docs/architecture.md](docs/architecture.md). The 60-section specification in `prompt-advance.txt` is the design target; the current source is not a certified production implementation of every section. See the explicit remaining integrations in that document before enabling real funds.

## Run

Linux deployment requires Rust stable, a C++ toolchain, Clang development headers, OpenSSL development headers, and Node 22+. For Ubuntu:

```sh
sudo apt-get install build-essential clang libclang-dev libssl-dev pkg-config
npm ci
cargo build --locked --release -p sniper-api
```

Generate independent secrets and set them in the service environment. Do not reuse these values:

```sh
export SNIPER_API_TOKEN="$(openssl rand -hex 32)"
export SNIPER_MASTER_KEY="$(openssl rand -hex 32)"
export SNIPER_UI_PASSWORD="$(openssl rand -hex 24)"
export SNIPER_CONFIG=config/default.toml
export SNIPER_BACKEND_URL=http://127.0.0.1:8787
```

Start the backend from the repository root, then start Next.js with the same API token and operator password:

```sh
./target/release/sniper-api
# Another shell with the frontend environment:
npm run build
npm run start -w apps/web
```

Serve both through `deploy/Caddyfile` for HTTPS. Configure a Helius confirmed WebSocket and RPC endpoint in API & Infrastructure. A preprocessed endpoint adds earlier speculative detection. Saved changes require restarting the paused service. Environment credentials take precedence over saved values. The master key must remain available for future vault decryption and is not included in backups.

For recorded playback, set `mode = "REPLAY"` in the TOML configuration and restart. `SNIPER_REPLAY_SPEED=0` runs at maximum speed; 1, 2, 5 and 10 control wall-clock pacing. REPLAY reads recorded PAPER market events and uses a virtual clock. Switching into or out of REPLAY requires a restart. Use a separate replay database for an independent experiment.

Use Strategy & risk to change TP (default **+60%**), SL (default **−30%**) and exposure limits. Start is disabled while any required preflight fails. A browser wallet is separate from the trading signer. The UI never accepts private keys.

DEVNET requires `SNIPER_DEVNET_WALLET` and `SNIPER_DEVNET_RPC_URL`. LIVE additionally requires `SNIPER_MAINNET_WALLET`, `SNIPER_HELIUS_RPC_URL`, `SNIPER_HELIUS_SENDER_URL`, `SNIPER_JITO_URL`, working feeds, fresh caches, recovered balances, and explicit **ENABLE LIVE MAINNET TRADING** confirmation. Sender Max's documented minimum tip is enforced at 0.001 SOL. Wallet key values are Solana keypair JSON arrays or base58 keypairs, supplied only to the Rust process.

## Checks

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked -p sniper-api
python3 scripts/smoke.py target/debug/sniper-api
npm run typecheck
npm test
npm run build
```

On this Windows workspace the isolated Linux toolchain under `.tools` can be used through WSL:

```powershell
wsl -d Ubuntu-24.04 -- bash /mnt/d/KC_data/Project/sniper_trading/scripts/check-rust.sh test --workspace
```

The tests cover deterministic scoring, malformed payloads, authenticated secret storage, API-session integrity, WAL reopen, process ownership, 100 concurrent duplicate detections, permanent protection after buy/sell/reopen, unknown-buy recovery, TP/SL, STOP liquidation, idempotent analytics, checkpoint retention, real-fill metadata accounting and persistence failure. The HTTP smoke test uses an isolated temporary PAPER database and generated test credentials. Verification details: [docs/verification.md](docs/verification.md).

## Persistence and operations

Keep `data/rocksdb` and `data/wal` on local NVMe and preserve them across upgrades. Checkpoints and backups are outside the active database and exclude secret directories. Do not restore an old checkpoint over a current LIVE registry. `/metrics` requires API authentication and exposes Prometheus histograms and queue/failure counters. The terminal shows unconfigured, failed, degraded and unresolved states explicitly.

Reference interfaces: [Helius preprocessed binary WebSocket](https://www.helius.dev/docs/preprocessed-transactions/preprocessed-subscribe), [Helius Sender](https://www.helius.dev/docs/sending-transactions/sender), [official Pump protocol IDLs](https://github.com/pump-fun/pump-public-docs), [Jito transaction submission](https://docs.jito.wtf/lowlatencytxnsend/).
