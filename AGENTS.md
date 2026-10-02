# Repository Guidelines

## Project Structure & Module Organization

- `crates/domain`: shared types, configuration, scoring, and executor interfaces.
- `crates/local-store`: RocksDB schema, WAL writes, ownership locks, checkpoints, and analytics.
- `crates/connectors`: Solana protocols, feeds, execution, RPC, and encrypted secrets.
- `crates/engine`: trading actor, recovery, replay, and metrics.
- `crates/api`: Axum service, authentication, and HTTP/WebSocket routes.
- `apps/web/src`: Next.js routes, React components, styles, and client utilities.
- `protocol/idl`: vendored protocol assets; `config`, `deploy`, `scripts`, and `docs` contain defaults, deployment files, tooling, and architecture notes.

## Build, Test, and Development Commands

Use stable Rust, C++/Clang and OpenSSL development packages, and Node 22+. Export required variables described in `.env.example`; the Rust service does not automatically load `.env`.

- `npm ci`: install locked frontend dependencies.
- `cargo run --locked -p sniper-api`: start the backend; defaults are PAPER and paused.
- `npm run dev`: start the frontend development server.
- `cargo fmt --all --check`: verify Rust formatting.
- `cargo clippy --locked --workspace --all-targets -- -D warnings`: enforce Rust lint checks.
- `cargo test --locked --workspace`: run backend tests.
- `npm run typecheck`, `npm test`, `npm run build`: check TypeScript, authentication tests, and production compilation.
- After `cargo build --locked -p sniper-api`, run `python3 scripts/smoke.py target/debug/sniper-api` for isolated PAPER HTTP checks.

For Windows/WSL toolchain instructions, see `README.md` and `scripts/check-rust.sh`.

## Coding Style & Naming Conventions

Use four-space Rust indentation and rustfmt; use two-space TypeScript/JSON indentation and Prettier. Rust functions/modules use `snake_case`, types use `PascalCase`. React components use `PascalCase`; retain existing lowercase filenames such as `terminal.tsx`. Follow `apps/web/AGENTS.md` before frontend edits.

## Testing Guidelines

Use Rust unit tests, Tokio async tests, Proptest, and Node's test runner. Integration tests live in `crates/*/tests`; frontend tests use `apps/web/tests/*.test.mjs`. Name tests by behavior, such as `unknown_buy_restored_without_resubmission`. Add regression coverage for changed trading, recovery, persistence, or authentication behavior. Keep tests deterministic and independent of funded wallets.

## Commit & Pull Request Guidelines

This checkout has no Git history to establish conventions. Use concise imperative subjects, such as `Fix STOP recovery`. PRs should describe behavior, relevant issues, validation commands/results, and limitations. Include screenshots for UI changes and explain schema or protocol changes.

## Security & Architecture

Keep secrets out of commits, logs, browser storage, and RocksDB. Preserve permanent purchase records, mode isolation, and LIVE confirmation guards. Keep trading decisions in RAM under actor ownership; perform network hydration and historical scans outside the hot path.
