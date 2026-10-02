# Verification

Verified on 2026-10-02 with Ubuntu 24.04 under WSL, Rust 1.99.0, and Windows Node 22.22.2. Native build prerequisites were installed in the ignored `.tools` directory. The final Linux binaries use `/tmp/sniper-trading-build` for build output.

| Check | Result |
| --- | --- |
| Rust formatting | Passed |
| Rust Clippy, all targets, warnings denied | Passed |
| Rust workspace tests | 21 passed, including the crash-helper test |
| API executable build | Passed |
| Isolated PAPER HTTP smoke test | Passed |
| Frontend TypeScript | Passed |
| Frontend authentication tests | 2 passed |
| Next.js production build | Passed |

The safety tests exercise 100 concurrent duplicate detections, permanent buy-once protection after buy/sell/reopen, unknown signed-order recovery without resubmission, exact default TP/SL triggers, STOP liquidation, STOPPING preservation across shutdown, and database-write failure during position protection. Storage tests kill a child process after the synchronous WAL acknowledgment, then verify the purchase record survives reopening. Analytics repair is checked for idempotence and mode isolation in all four bucket widths. Metadata fixtures check SOL, WSOL, rent, tip and token-balance accounting.

The smoke test creates its own database, config, API token and encryption master key, removes inherited `SNIPER_*` credentials, uses PAPER mode, and shuts down the process afterward. It checks unauthorized and wrong-Origin responses, rejected Start and LIVE selection without required services, HTTPS-only credential writes, history, saved analytics, bucket reads, metrics, checkpoint creation, verification, STOP and graceful shutdown.

No browser surface was available for visual inspection. Frontend verification consists of TypeScript checks, authentication tests and a production build. Network adapters have not been validated against funded wallets or recorded provider fixtures in this environment. No blockchain transactions were submitted. Protocol integration coverage and production limitations are described in [architecture.md](architecture.md).
