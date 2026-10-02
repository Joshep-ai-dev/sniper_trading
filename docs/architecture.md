# Trading architecture

The workspace separates deterministic domain code, local persistence, protocol/network adapters, the trading actor, and the Axum service. Next.js is an authenticated control surface; it never reads RocksDB or receives signer material.

## Entry and transaction recovery

The actor alone mutates purchased mints, reservations, exposure, balances and positions. Commands, sell requests, network completions and market observations use separate bounded queues. Completions and sells are selected before new market analysis. A saturated market queue disables new entries; sends await capacity rather than discarding critical price observations.

Before a buy can reach a transport:

1. Validate mode, running state, facts, strategy and portfolio limits in RAM.
2. Insert the RAM reservation and synchronously write its RocksDB WAL batch.
3. Construct and locally sign from warmed account and blockhash caches.
4. Persist the exact signature and signed bytes together with the reservation.
5. Submit those bytes. Sender failure falls back to Jito with identical bytes.
6. Reconcile finalized transaction metadata. Atomically commit the permanent purchase, position, fill, balance and order removal before clearing the pending index.

Timeouts and missing transactions remain `UNKNOWN_PENDING`; they do not prove a failed fill. There is deliberately no timeout-based reservation release. Unsigned reservations without a corresponding durable order are safe to release on startup: the submission code cannot run before its signed-order write completes.

Confirmed bought mints remain in `purchased_tokens` after exit. The store rejects deletion of this column family. Keys include a mode byte and binary public key, so virtual purchases do not block LIVE purchases. The LIVE registry applies across strategy versions and restarts for the same database. Moving to a new empty database discards that guarantee; production upgrades must preserve the database.

## Exit protection

A position acquires its sell order under actor ownership. Until that order has a proven finalized result it cannot acquire another sell. STOP disables entries, persists `STOPPING`, and schedules every position for liquidation. Filled in-flight buys are also liquidated. STOPPED requires zero positions, zero orders and zero reservations.

Database failure closes the entry gate. Emergency sells can continue from RAM, with the order retained for reconciliation and an explicit degraded status. If the machine also crashes while emergency writes are unavailable, on-chain holdings must be reconciled before entry is permitted; do not assume the database has captured the emergency signature.

Open real positions have a separate bounded RPC refresh task, so cache refresh continues when market feeds disconnect. The signed order records its PumpSwap pool address; filled positions retain that route for restart protection. Database health runs in a separate blocking worker and cannot hold the actor while it inspects RocksDB or disk space.

## Local analytics

Closed-trade totals commit with each exit. A background worker also maintains minute, five-minute, hourly and daily buckets. It commits a trade-processing marker and all four buckets in one WAL batch. Repeating the repair scan after a crash is idempotent. Historical scans stream records through RocksDB iterators outside the actor; the analytics API reads saved totals or cursor-paginated buckets.

## Security boundaries

The Rust API is bound to loopback. It accepts a constant-time-checked bearer token or a short-lived HTTP-only session cookie and validates browser Origins. The Next.js server uses an operator password and signed HTTP-only cookie before forwarding the server token. Secrets have no Debug implementation, RPC errors do not include URLs, and credentials are not persisted in browser storage. Secret writes require HTTPS; deployment uses the supplied Caddy reverse proxy.

Infrastructure secrets use AES-256-GCM with random nonces and name-bound associated data in a directory outside RocksDB. Environment values override the encrypted vault. Trading keypairs are read by the Rust secret-provider interface, never accepted by the frontend API. Local signer and database ownership use OS locks; signer locks span databases on the same machine.

## Protocol and performance boundaries

Official Pump.fun and PumpSwap IDLs are vendored. Account order, privileges, PDA seeds and instruction discriminators come from those files. Hydration runs separately from decision execution. Token extensions, special market variants, missing accounts and stale caches fail closed. Legacy and v0 wire transactions are supported; unresolved ALT instruction indices and v1 transactions require a newer decoder and are not guessed.

Latency measurements use `Instant`, not wall-clock subtraction. Prometheus histograms distinguish local analysis, reservation writes, construction/signing, submission and reconciliation. These are measured stages, not a certified sub-100-ms latency result. Profile on the deployment machine against recorded and actual feeds before claiming a latency target.

## Current integration limits

The project includes the shared engine, paper executor, network executor, direct instruction builders, feed parsers, durable recovery, API and terminal. It has not been certified for trading real funds. Protocol variant coverage, hardware/OS signing providers, raw-shred and LaserStream adapters, comprehensive wallet-cluster intelligence, transaction-v1 decoding, and optional Jupiter execution still need separate integrations. Neither placeholders nor fabricated service health enable them. Network-backed behavior requires credentials, recorded provider fixtures and controlled Devnet validation; the test suite does not submit real transactions.
