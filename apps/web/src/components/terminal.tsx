"use client";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useWallet } from "@solana/wallet-adapter-react";
import { api, ApiError, sol, short, date } from "@/lib/api";
import type {
  Aggregate,
  ClosedTrade,
  Mode,
  Position,
  Snapshot,
  Strategy,
  Risk,
} from "@/lib/types";

const WalletButton = dynamic(
  () =>
    import("@solana/wallet-adapter-react-ui").then((m) => m.WalletMultiButton),
  { ssr: false },
);
const nav = [
  ["dashboard", "Overview", "◈"],
  ["feed", "Live feed", "≋"],
  ["positions", "Positions", "▤"],
  ["history", "Trade history", "↗"],
  ["analytics", "Analytics", "⌁"],
  ["strategy", "Strategy & risk", "⚙"],
  ["wallet", "Wallets", "◇"],
  ["infrastructure", "API & infrastructure", "⇄"],
  ["database", "Local database", "▥"],
  ["operations", "Operations", "◎"],
  ["settings", "Settings", "⋯"],
];
type Notice = { text: string; bad: boolean };
const badge = (status: string) => (
  <span
    className={`badge ${["RUNNING", "READY", "CONNECTED", "PASS", "OPEN", "STOPPED"].includes(status) ? "good" : ["FAILED", "ERROR", "DEGRADED", "UNKNOWN_PENDING"].includes(status) ? "bad" : ""}`}
  >
    <i />
    {status.replaceAll("_", " ")}
  </span>
);
function Empty({
  text = "No records yet. Data will appear as the trading engine processes market events.",
}: {
  text?: string;
}) {
  return (
    <div className="empty">
      <span>◇</span>
      <p>{text}</p>
    </div>
  );
}
function Metric({
  label,
  value,
  note,
  positive,
}: {
  label: string;
  value: string;
  note?: string;
  positive?: boolean;
}) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong className={positive ? "green" : ""}>{value}</strong>
      {note && <small>{note}</small>}
    </div>
  );
}
function PositionTable({
  positions,
  onSell,
}: {
  positions: Position[];
  onSell: (mint: string) => void;
}) {
  return positions.length ? (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Token / mint</th>
            <th>Entry · SOL</th>
            <th>Current · SOL</th>
            <th>Net PnL</th>
            <th>TP / SL</th>
            <th>Hold time</th>
            <th>Status</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => {
            const value = (p.quantity * p.current_price) / 1e6,
              pnl = value - p.spent - p.entry_fees,
              percent = (value / p.spent - 1) * 100;
            return (
              <tr key={p.mint}>
                <td>
                  <strong>{short(p.mint)}</strong>
                  <small>
                    {p.venue.replaceAll("_", " ")} · score {p.score}
                  </small>
                </td>
                <td>{sol(p.spent)}</td>
                <td>{sol(value)}</td>
                <td className={pnl >= 0 ? "green" : "red"}>
                  {percent.toFixed(2)}%<small>{sol(pnl)} SOL</small>
                </td>
                <td>
                  +{p.strategy.take_profit_bps / 100}% / −
                  {p.strategy.stop_loss_bps / 100}%
                </td>
                <td>
                  {Math.max(0, Math.floor((Date.now() - p.entry_ms) / 1000))}s
                </td>
                <td>{badge(p.state)}</td>
                <td>
                  <button
                    className="danger subtle"
                    disabled={!!p.exit_order}
                    onClick={() => onSell(p.mint)}
                  >
                    Priority sell ↗
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  ) : (
    <Empty text="No open positions. New positions appear here after a confirmed buy." />
  );
}

export function Terminal({ section }: { section: string }) {
  const queryClient = useQueryClient();
  const [notice, setNotice] = useState<Notice | null>(null);
  const [busy, setBusy] = useState(false);
  const [modal, setModal] = useState<"stop" | "live" | null>(null);
  const [password, setPassword] = useState("");
  const query = useQuery<Snapshot, ApiError>({
    queryKey: ["state"],
    queryFn: () => api("state"),
    refetchInterval: 2000,
    retry: false,
  });
  const s = query.data;
  async function run(
    path: string,
    body: unknown = {},
    success = "Operation completed",
  ) {
    setBusy(true);
    try {
      const result = await api<unknown>(path, body);
      setNotice({ text: success, bad: false });
      await queryClient.invalidateQueries();
      return result;
    } catch (e) {
      setNotice({ text: (e as Error).message, bad: true });
    } finally {
      setBusy(false);
    }
  }
  useEffect(() => {
    if (!s) return;
    let socket: WebSocket | undefined,
      timer: ReturnType<typeof setTimeout> | undefined,
      cancelled = false;
    const connect = async () => {
      try {
        await api("session", {});
        if (cancelled) return;
        socket = new WebSocket(
          `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/stream`,
        );
        socket.onmessage = (e) => {
          try {
            const next: Snapshot = JSON.parse(e.data);
            queryClient.setQueryData(["state"], (old: Snapshot | undefined) =>
              !old || next.sequence > old.sequence ? next : old,
            );
          } catch {
            /* REST polling stays active */
          }
        };
        socket.onclose = () => {
          if (!cancelled) timer = setTimeout(connect, 5000);
        };
      } catch {
        if (!cancelled) timer = setTimeout(connect, 5000);
      }
    };
    void connect();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      socket?.close();
    };
  }, [!!s, queryClient]);
  const page = nav.find((n) => n[0] === section) || nav[0];
  const ready = !!s?.preflight.every((c) => c.pass);
  return (
    <div className="terminal">
      <aside>
        <Link href="/" className="brand">
          <span className="brand-mark">S</span>
          <div>
            SNIPER<small>SOLANA TRADING ENGINE</small>
          </div>
        </Link>
        <div className="workspace">
          <span className="status-dot" />
          LOCAL WORKSPACE <span>01</span>
        </div>
        <nav>
          {nav.map(([id, label, icon]) => (
            <Link
              key={id}
              href={id === "dashboard" ? "/" : `/${id}`}
              className={section === id ? "active" : ""}
            >
              <span>{icon}</span>
              {label}
              {id === "positions" && s && <b>{s.positions.length}</b>}
            </Link>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div>{badge(s?.database?.status || "OFFLINE")}</div>
          <small>Local persistence · RocksDB</small>
          <p>All financial views identify the selected trading mode.</p>
          <span>ENGINE v0.1.0</span>
        </div>
      </aside>
      <main>
        <header>
          <div className="breadcrumb">
            TERMINAL <span>/</span> {page[1]}
          </div>
          <div className="header-right">
            <span className="status-dot" />
            {s ? "ENGINE CONNECTED" : "ENGINE OFFLINE"}
            <span className="divider" />
            <time suppressHydrationWarning>
              {s ? new Date().toLocaleTimeString() : "—"}
            </time>
          </div>
        </header>
        <div className="content">
          <div className="title-row">
            <div>
              <div className="eyebrow">SOLANA · LOCAL EXECUTION</div>
              <h1>{page[1]}</h1>
              <p>
                {section === "dashboard"
                  ? "Your trading engine, at a glance."
                  : section === "strategy"
                    ? "Deterministic decisions. Explicit limits. Every change is versioned."
                    : "Live engine state and durable local records."}
              </p>
            </div>
            <div className="controls">
              <select
                aria-label="Trading mode"
                value={s?.mode || "PAPER"}
                disabled={
                  !s ||
                  busy ||
                  s.state === "RUNNING" ||
                  s.positions.length > 0 ||
                  s.unresolved_orders > 0
                }
                onChange={(e) =>
                  void run(
                    "action",
                    { action: "SET_MODE", mode: e.target.value },
                    "Mode selected. Complete preflight before starting.",
                  )
                }
              >
                <option>PAPER</option>
                <option>REPLAY</option>
                <option>DEVNET</option>
                <option>LIVE</option>
              </select>
              {badge(s?.state || "OFFLINE")}
              <button
                disabled={!s || busy || s.state === "RUNNING" || !ready}
                className="primary"
                onClick={() =>
                  s?.mode === "LIVE"
                    ? setModal("live")
                    : void run("action", { action: "START" }, "Trading started")
                }
              >
                ▶ Start
              </button>
              <button
                disabled={!s || busy || s.state !== "RUNNING"}
                onClick={() =>
                  void run(
                    "action",
                    { action: "PAUSE" },
                    "Paused. Position protection remains active.",
                  )
                }
              >
                Ⅱ Pause
              </button>
              <button
                disabled={!s || busy}
                className="danger"
                onClick={() => setModal("stop")}
              >
                ■ Stop bot
              </button>
            </div>
          </div>
          {s && (
            <div className={`mode-banner ${s.mode === "LIVE" ? "live" : ""}`}>
              <strong>
                {s.mode === "PAPER"
                  ? "PAPER TRADING"
                  : s.mode === "REPLAY"
                    ? "DETERMINISTIC REPLAY"
                    : s.mode === "DEVNET"
                      ? "DEVNET TEST TRADING"
                      : "LIVE MAINNET"}
              </strong>
              <span>
                {s.mode === "PAPER"
                  ? "Mainnet market data · virtual balance · simulated transactions"
                  : s.mode === "REPLAY"
                    ? "Local historical data · virtual execution"
                    : s.mode === "DEVNET"
                      ? "Test SOL · separate signing wallet"
                      : "Real SOL · real transactions · real fees"}
              </span>
            </div>
          )}
          {notice && (
            <div
              className={`notice ${notice.bad ? "error" : ""}`}
              role="status"
            >
              {notice.text}
              <button onClick={() => setNotice(null)}>×</button>
            </div>
          )}
          {s?.error && <div className="notice error">{s.error}</div>}
          {query.error && (
            <div className="panel connection">
              <div className="eyebrow">CONNECTION REQUIRED</div>
              <h2>
                {query.error.status === 401
                  ? "Sign in to your terminal"
                  : "Trading service unavailable"}
              </h2>
              <p>{query.error.message}</p>
              {query.error.status === 401 ? (
                <form
                  onSubmit={async (e) => {
                    e.preventDefault();
                    try {
                      const r = await fetch("/api/login", {
                        method: "POST",
                        headers: { "content-type": "application/json" },
                        body: JSON.stringify({ password }),
                      });
                      const data = await r.json();
                      if (!r.ok) throw new Error(data.error);
                      setPassword("");
                      void query.refetch();
                    } catch (error) {
                      setNotice({ text: (error as Error).message, bad: true });
                    }
                  }}
                >
                  <label>
                    Operator password
                    <input
                      type="password"
                      autoComplete="current-password"
                      required
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                    />
                  </label>
                  <button className="primary">Sign in →</button>
                </form>
              ) : (
                <button onClick={() => void query.refetch()}>
                  Retry connection
                </button>
              )}
            </div>
          )}
          {section === "dashboard" && (
            <>
              <div className="metrics-grid">
                <Metric
                  label={s?.mode === "LIVE" ? "WALLET BALANCE" : "MODE BALANCE"}
                  value={`${sol(s?.balance)} SOL`}
                  note={s?.mode || "Awaiting engine"}
                />
                <Metric
                  label="OPEN EXPOSURE"
                  value={`${sol(s?.exposure)} SOL`}
                  note={`${s?.positions.length ?? 0} active positions`}
                />
                <Metric
                  label="TODAY · NET PNL"
                  value={`${sol(s?.daily_pnl)} SOL`}
                  positive={(s?.daily_pnl ?? 0) > 0}
                  note="Fees and tips included"
                />
                <Metric
                  label="WIN RATE"
                  value={
                    s && s.aggregate.trades
                      ? `${((s.aggregate.wins / s.aggregate.trades) * 100).toFixed(1)}%`
                      : "—"
                  }
                  note={`${s?.aggregate.trades ?? 0} closed trades`}
                />
              </div>
              <div className="dashboard-grid">
                <div className="panel">
                  <div className="panel-title">
                    <h2>Engine preflight</h2>
                    <span>
                      {s?.preflight.filter((c) => c.pass).length ?? 0} /{" "}
                      {s?.preflight.length ?? 0} PASS
                    </span>
                  </div>
                  <Preflight s={s} />
                </div>
                <div className="panel">
                  <div className="panel-title">
                    <h2>Infrastructure</h2>
                    <Link href="/infrastructure">Manage ↗</Link>
                  </div>
                  <Services s={s} />
                </div>
              </div>
              <div className="panel">
                <div className="panel-title">
                  <h2>
                    Active positions <em>{s?.positions.length ?? 0}</em>
                  </h2>
                  <Link href="/positions">View all ↗</Link>
                </div>
                <PositionTable
                  positions={s?.positions || []}
                  onSell={(mint) =>
                    void run(`sell/${mint}`, {}, "Priority sell requested")
                  }
                />
              </div>
              <div className="panel">
                <div className="panel-title">
                  <h2>Latest detections</h2>
                  <Link href="/feed">Live feed ↗</Link>
                </div>
                <Feed s={s} compact />
              </div>
            </>
          )}
          {section === "positions" && (
            <>
              <div className="panel">
                <div className="panel-title">
                  <h2>Position protection</h2>
                  <span>
                    TP +{(s?.strategy.take_profit_bps ?? 6000) / 100}% · SL −
                    {(s?.strategy.stop_loss_bps ?? 3000) / 100}%
                  </span>
                </div>
                <PositionTable
                  positions={s?.positions || []}
                  onSell={(mint) =>
                    void run(`sell/${mint}`, {}, "Priority sell requested")
                  }
                />
              </div>
              {!!s?.pending.length && (
                <div className="panel">
                  <h2>Pending buys</h2>
                  {s.pending.map((p) => (
                    <div className="check-row" key={p.mint}>
                      <span>{short(p.mint)}</span>
                      {badge(p.status)}
                      <code>{p.signature || "Reserved before signing"}</code>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
          {section === "feed" && (
            <div className="panel">
              <Feed s={s} />
            </div>
          )}
          {section === "history" && <History />}
          {section === "analytics" && <Analytics />}
          {section === "strategy" && s && (
            <StrategyPanel s={s} run={run} busy={busy} />
          )}
          {section === "wallet" && <WalletPanel run={run} />}
          {section === "infrastructure" && (
            <Infrastructure run={run} busy={busy} />
          )}
          {section === "database" && <Database s={s} run={run} busy={busy} />}
          {section === "operations" && (
            <>
              <div className="metrics-grid">
                <Metric
                  label="UPTIME"
                  value={s ? `${Math.floor(s.uptime_secs / 60)} min` : "—"}
                />
                <Metric
                  label="MARKET QUEUE"
                  value={String(s?.market_queue ?? "—")}
                  note="Capacity 2,048"
                />
                <Metric
                  label="UNRESOLVED ORDERS"
                  value={String(s?.unresolved_orders ?? "—")}
                />
                <Metric
                  label="LOCAL WAL WRITE"
                  value={
                    s?.database ? `${s.database.write_latency_us} µs` : "—"
                  }
                />
              </div>
              <div className="dashboard-grid">
                <div className="panel">
                  <h2>Operational preflight</h2>
                  <Preflight s={s} />
                </div>
                <div className="panel">
                  <h2>Service health</h2>
                  <Services s={s} />
                </div>
              </div>
            </>
          )}
          {section === "settings" && (
            <div className="panel settings">
              <h2>Workspace settings</h2>
              <p>
                Database tuning, paper simulation, deployment paths, and startup
                network settings are configured in{" "}
                <code>config/default.toml</code>. Strategy and risk limits can
                be changed in the terminal and are persisted with a new version.
              </p>
              <div className="check-row">
                <span>Permanent buy-once registry</span>
                {badge("ENFORCED")}
              </div>
              <div className="check-row">
                <span>Critical writes</span>
                <strong>WAL enabled · synchronous</strong>
              </div>
              <div className="check-row">
                <span>LIVE startup</span>
                <strong>Explicit confirmation required</strong>
              </div>
              <Link className="button" href="/strategy">
                Strategy & risk →
              </Link>
            </div>
          )}
          <footer>
            <span>
              <i className="status-dot" />{" "}
              {s ? "Local engine connected" : "Waiting for Rust service"}
            </span>
            <span>
              RAM hot state <b>·</b> RocksDB source of truth <b>·</b> No remote
              database
            </span>
            <span>
              {s?.mode || "PAPER"} / {s?.state || "OFFLINE"}
            </span>
          </footer>
        </div>
      </main>
      {modal && (
        <div className="modal-shade">
          <div
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="modal-title"
          >
            <div className="eyebrow">
              {modal === "live" ? "REAL FUNDS" : "PRIORITY LIQUIDATION"}
            </div>
            <h2 id="modal-title">
              {modal === "live"
                ? "ENABLE LIVE MAINNET TRADING?"
                : "STOP AND SELL ALL?"}
            </h2>
            <p>
              {modal === "live"
                ? "Real SOL will be used. Real blockchain transactions will be submitted."
                : "New purchases will stop immediately. Every open position will receive a priority sell order. The engine stays STOPPING until all positions and unresolved orders close."}
            </p>
            <div className="modal-details">
              <span>
                Open positions<strong>{s?.positions.length ?? 0}</strong>
              </span>
              <span>
                Exposure<strong>{sol(s?.exposure)} SOL</strong>
              </span>
            </div>
            <div className="modal-buttons">
              <button onClick={() => setModal(null)}>Cancel</button>
              <button
                className="danger"
                disabled={busy}
                onClick={() => {
                  setModal(null);
                  void run(
                    "action",
                    modal === "live"
                      ? {
                          action: "START",
                          confirmation: "ENABLE LIVE MAINNET TRADING",
                        }
                      : { action: "STOP" },
                    modal === "live"
                      ? "LIVE trading enabled"
                      : "STOPPING · Priority liquidation requested",
                  );
                }}
              >
                {modal === "live" ? "ENABLE LIVE" : "STOP AND SELL ALL"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
type Runner = (
  path: string,
  body?: unknown,
  success?: string,
) => Promise<unknown>;
function Preflight({ s }: { s?: Snapshot }) {
  return s ? (
    <div className="preflight">
      {s.preflight.map((c) => (
        <div className="check-row" key={c.name}>
          <span>
            <strong>{c.name}</strong>
            <small>{c.reason}</small>
          </span>
          {badge(c.pass ? "PASS" : "FAILED")}
        </div>
      ))}
    </div>
  ) : (
    <Empty text="Preflight will be available when the engine connects." />
  );
}
function Services({ s }: { s?: Snapshot }) {
  const services = s?.services.length
    ? s.services
    : [
        {
          name: "Helius",
          status: "NOT_CONFIGURED",
          latency_ms: null,
          error: null,
        },
        {
          name: "Sender",
          status: "NOT_CONFIGURED",
          latency_ms: null,
          error: null,
        },
        {
          name: "Jito",
          status: "NOT_CONFIGURED",
          latency_ms: null,
          error: null,
        },
      ];
  return (
    <div>
      {services.map((service) => (
        <div className="check-row" key={service.name}>
          <span>
            <strong>{service.name}</strong>
            <small>{service.error || "Persistent connection"}</small>
          </span>
          <div>
            {badge(service.status)}
            <small className="right">
              {service.latency_ms === null ? "—" : `${service.latency_ms} ms`}
            </small>
          </div>
        </div>
      ))}
      <div className="check-row">
        <strong>RocksDB</strong>
        {badge(s?.database?.status || "NOT_INITIALIZED")}
      </div>
    </div>
  );
}
function Feed({ s, compact = false }: { s?: Snapshot; compact?: boolean }) {
  const [min, setMin] = useState(0);
  const [filter, setFilter] = useState("ALL");
  const feed = (s?.feed || [])
    .filter(
      (f) =>
        (f.score?.total || 0) >= min &&
        (filter === "ALL" || f.decision === filter),
    )
    .slice(0, compact ? 5 : 200);
  return (
    <>
      {!compact && (
        <div className="filter-row">
          <label>
            Minimum score
            <input
              type="number"
              min={0}
              max={100}
              value={min}
              onChange={(e) => setMin(Number(e.target.value))}
            />
          </label>
          <label>
            Decision
            <select value={filter} onChange={(e) => setFilter(e.target.value)}>
              {[
                "ALL",
                "REJECTED",
                "BUY_SUBMITTED",
                "ALREADY_PURCHASED",
                "UNKNOWN_PENDING",
                "RISK_LIMIT",
              ].map((v) => (
                <option key={v}>{v}</option>
              ))}
            </select>
          </label>
          <span>
            {feed.length} observations · {s?.mode || "PAPER"}
          </span>
        </div>
      )}
      {feed.length ? (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Token / mint</th>
                <th>Age</th>
                <th>Creator</th>
                <th>Market cap · SOL</th>
                <th>Score</th>
                <th>Source</th>
                <th>Decision</th>
              </tr>
            </thead>
            <tbody>
              {feed.map((f, i) => (
                <tr key={`${f.event.signature}-${i}`}>
                  <td>
                    <strong>{short(f.event.mint)}</strong>
                    <small>{f.event.venue}</small>
                  </td>
                  <td>
                    {Math.max(
                      0,
                      Math.floor((Date.now() - f.event.observed_ms) / 1000),
                    )}
                    s
                  </td>
                  <td>{short(f.event.creator)}</td>
                  <td>{sol(f.event.market_cap)}</td>
                  <td>
                    <span className="score">{f.score?.total ?? "—"}</span>
                  </td>
                  <td>{f.event.speculative ? "Preprocessed" : "Confirmed"}</td>
                  <td title={f.score?.reasons.join(", ")}>
                    {badge(f.decision)}
                    {f.score?.reasons.length ? (
                      <small>{f.score.reasons.join(" · ")}</small>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty text="No matching detections. Connect the Helius feed to receive Mainnet events." />
      )}
    </>
  );
}
function History() {
  const [mode, setMode] = useState<Mode>("LIVE");
  const [outcome, setOutcome] = useState("ALL");
  const [cursor, setCursor] = useState<string | null>(null);
  const q = useQuery<{ trades: ClosedTrade[]; cursor: string | null }>({
    queryKey: ["history", mode, cursor],
    queryFn: () =>
      api(`history?mode=${mode}&limit=100${cursor ? `&before=${cursor}` : ""}`),
  });
  const trades =
    q.data?.trades.filter(
      (t) =>
        outcome === "ALL" ||
        (outcome === "WIN" ? t.net_pnl > 0 : t.net_pnl <= 0),
    ) || [];
  return (
    <div className="panel">
      <div className="filter-row">
        <label>
          Trading mode
          <ModeSelect
            value={mode}
            onChange={(m) => {
              setMode(m);
              setCursor(null);
            }}
          />
        </label>
        <label>
          Result
          <select value={outcome} onChange={(e) => setOutcome(e.target.value)}>
            <option>ALL</option>
            <option>WIN</option>
            <option>LOSS</option>
          </select>
        </label>
        <button disabled={!cursor} onClick={() => setCursor(null)}>
          Latest
        </button>
        <button
          disabled={!q.data?.cursor || q.data.trades.length < 100}
          onClick={() => setCursor(q.data?.cursor || null)}
        >
          Older →
        </button>
      </div>
      {q.error && <p className="red">{q.error.message}</p>}
      {trades.length ? (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Token</th>
                <th>Mode</th>
                <th>Closed</th>
                <th>Spent</th>
                <th>Received</th>
                <th>Net PnL · SOL</th>
                <th>Score</th>
                <th>Exit</th>
                <th>Hold</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t) => (
                <tr key={t.exit.signature}>
                  <td title={t.position.mint}>{short(t.position.mint)}</td>
                  <td>{badge(t.position.mode)}</td>
                  <td>{date(t.exit.timestamp_ms)}</td>
                  <td>{sol(t.position.spent)}</td>
                  <td>{sol(t.exit.sol_amount)}</td>
                  <td className={t.net_pnl >= 0 ? "green" : "red"}>
                    {sol(t.net_pnl)}
                  </td>
                  <td>{t.position.score}</td>
                  <td>{t.reason}</td>
                  <td>{(t.hold_ms / 1000).toFixed(1)}s</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty text={`No closed ${mode} trades for this view.`} />
      )}
    </div>
  );
}
function ModeSelect({
  value,
  onChange,
}: {
  value: Mode;
  onChange: (m: Mode) => void;
}) {
  return (
    <select value={value} onChange={(e) => onChange(e.target.value as Mode)}>
      {["LIVE", "PAPER", "DEVNET", "REPLAY"].map((m) => (
        <option key={m}>{m}</option>
      ))}
    </select>
  );
}
function Analytics() {
  const [mode, setMode] = useState<Mode>("LIVE");
  const q = useQuery<Aggregate>({
    queryKey: ["analytics", mode],
    queryFn: () => api(`analytics?mode=${mode}`),
  });
  const a = q.data;
  return (
    <>
      <div className="filter-row">
        <label>
          Financial view
          <ModeSelect value={mode} onChange={setMode} />
        </label>
        <span>
          {mode === "LIVE" ? "Real trading results" : "Virtual or test results"}
        </span>
      </div>
      {q.error && <p className="red">{q.error.message}</p>}
      <div className="metrics-grid">
        <Metric
          label="NET PNL"
          value={`${sol(a?.net_pnl)} SOL`}
          positive={(a?.net_pnl || 0) > 0}
        />
        <Metric
          label="WIN RATE"
          value={a?.trades ? `${((100 * a.wins) / a.trades).toFixed(1)}%` : "—"}
        />
        <Metric label="CLOSED TRADES" value={String(a?.trades ?? "—")} />
        <Metric label="FEES & TIPS" value={`${sol(a?.fees)} SOL`} />
      </div>
      <div className="panel">
        <h2>Performance breakdown</h2>
        {a ? (
          <div className="details-grid">
            {[
              ["Wins", a.wins],
              ["Losses", a.losses],
              ["Gross PnL", `${sol(a.gross_pnl)} SOL`],
              [
                "Average hold time",
                a.trades
                  ? `${(a.hold_ms / a.trades / 1000).toFixed(1)} s`
                  : "—",
              ],
              [
                "Average score",
                a.trades ? (a.score_sum / a.trades).toFixed(1) : "—",
              ],
              ["Take profit exits", a.tp],
              ["Stop loss exits", a.sl],
            ].map(([name, value]) => (
              <div key={name}>
                <small>{name}</small>
                <strong>{value}</strong>
              </div>
            ))}
          </div>
        ) : (
          <Empty />
        )}
      </div>
    </>
  );
}
function StrategyPanel({
  s,
  run,
  busy,
}: {
  s: Snapshot;
  run: Runner;
  busy: boolean;
}) {
  const [strategy, setStrategy] = useState<Strategy>(s.strategy);
  const [risk, setRisk] = useState<Risk>(s.risk);
  useEffect(() => setStrategy(s.strategy), [s.strategy.version]);
  return (
    <div className="dashboard-grid">
      <form
        className="panel"
        onSubmit={(e) => {
          e.preventDefault();
          void run(
            "action",
            { action: "STRATEGY", config: strategy },
            "New strategy version activated",
          );
        }}
      >
        <div className="panel-title">
          <h2>Entry & exit strategy</h2>
          {badge(`VERSION ${s.strategy.version}`)}
        </div>
        <div className="form-grid">
          <label>
            Entry mode
            <select
              value={strategy.entry_mode}
              onChange={(e) =>
                setStrategy({
                  ...strategy,
                  entry_mode: e.target.value as "FAST" | "CONFIRMED",
                })
              }
            >
              <option>FAST</option>
              <option>CONFIRMED</option>
            </select>
          </label>
          <label>
            Confirmation window · ms
            <input
              type="number"
              min={100}
              max={300}
              value={strategy.confirmation_ms}
              onChange={(e) =>
                setStrategy({
                  ...strategy,
                  confirmation_ms: Number(e.target.value),
                })
              }
            />
          </label>
          {(
            [
              ["take_profit_bps", "Take profit · %", 100],
              ["stop_loss_bps", "Stop loss · %", 100],
              ["min_score", "Minimum token score", 1],
              ["min_creator_score", "Minimum creator score", 1],
              ["max_dev_allocation_bps", "Maximum dev allocation · %", 100],
              ["max_concentration_bps", "Maximum concentration · %", 100],
            ] as const
          ).map(([key, label, scale]) => (
            <label key={key}>
              {label}
              <input
                type="number"
                min={0}
                step={scale === 100 ? 0.01 : 1}
                required
                value={strategy[key] / scale}
                onChange={(e) =>
                  setStrategy({
                    ...strategy,
                    [key]: Math.round(Number(e.target.value) * scale),
                  })
                }
              />
            </label>
          ))}
        </div>
        <label>
          Version notes
          <textarea
            value={strategy.notes}
            maxLength={1024}
            onChange={(e) =>
              setStrategy({ ...strategy, notes: e.target.value })
            }
          />
        </label>
        <p className="muted">
          Each position keeps the strategy version used for its entry.
        </p>
        <button className="primary" disabled={busy}>
          Activate new version →
        </button>
      </form>
      <form
        className="panel"
        onSubmit={(e) => {
          e.preventDefault();
          void run(
            "action",
            { action: "RISK", config: risk },
            "Risk configuration activated",
          );
        }}
      >
        <div className="panel-title">
          <h2>Risk limits</h2>
          <span>ENFORCED IN RUST</span>
        </div>
        <div className="form-grid">
          {Object.keys(risk).map((key) => {
            const k = key as keyof Risk,
              scale = [
                "sol_per_trade",
                "max_exposure",
                "max_daily_loss",
                "max_priority_fee",
                "max_jito_tip",
                "min_balance",
              ].includes(k)
                ? 1e9
                : k === "slippage_bps"
                  ? 100
                  : k === "max_hold_ms"
                    ? 1000
                    : 1;
            return (
              <label key={key}>
                {key.replaceAll("_", " ")}
                {scale === 1e9
                  ? " · SOL"
                  : scale === 100
                    ? " · %"
                    : scale === 1000
                      ? " · seconds"
                      : ""}
                <input
                  required
                  type="number"
                  min={0}
                  step={scale === 1 ? 1 : 0.000001}
                  value={risk[k] / scale}
                  onChange={(e) =>
                    setRisk({
                      ...risk,
                      [k]: Math.round(Number(e.target.value) * scale),
                    })
                  }
                />
              </label>
            );
          })}
        </div>
        <button className="primary" disabled={busy || s.unresolved_orders > 0}>
          Save risk limits →
        </button>
      </form>
    </div>
  );
}
function WalletPanel({ run }: { run: Runner }) {
  const wallet = useWallet();
  const [balance, setBalance] = useState<{
    balance: number;
    network: string;
    updated_ms: number;
  } | null>(null);
  const [error, setError] = useState("");
  const q = useQuery<{
    trading_wallets: {
      mode: Mode;
      address: string;
      balance: number;
      status: string;
      network: string;
    }[];
  }>({ queryKey: ["wallet"], queryFn: () => api("wallet") });
  useEffect(() => setBalance(null), [wallet.publicKey?.toBase58()]);
  return (
    <div className="dashboard-grid">
      <div className="panel wallet-panel">
        <div className="panel-title">
          <h2>Browser wallet</h2>
          {badge(
            wallet.connected
              ? "CONNECTED"
              : wallet.connecting
                ? "CONNECTING"
                : "DISCONNECTED",
          )}
        </div>
        <div className="wallet-symbol">◇</div>
        <p>Connect a Solana wallet to view its address and balance.</p>
        <WalletButton />
        {wallet.publicKey && (
          <>
            <code className="wallet-address">
              {wallet.publicKey.toBase58()}
            </code>
            <div className="details-grid">
              <div>
                <small>SOL balance</small>
                <strong>{sol(balance?.balance)}</strong>
              </div>
              <div>
                <small>Network</small>
                <strong>{balance?.network || "Unverified"}</strong>
              </div>
            </div>
            <div className="button-row">
              <button
                onClick={() =>
                  void navigator.clipboard.writeText(
                    wallet.publicKey!.toBase58(),
                  )
                }
              >
                Copy address
              </button>
              <button
                onClick={async () => {
                  try {
                    setBalance(
                      await api(
                        `wallet/balance?address=${wallet.publicKey!.toBase58()}`,
                      ),
                    );
                    setError("");
                  } catch (e) {
                    setError((e as Error).message);
                  }
                }}
              >
                Refresh balance
              </button>
              <button onClick={() => void wallet.disconnect()}>
                Disconnect
              </button>
            </div>
            {error && <p className="red">{error}</p>}
          </>
        )}
        <p className="muted">
          The automated signer is configured separately in the Rust service.
        </p>
      </div>
      <div className="panel">
        <h2>Dedicated trading wallets</h2>
        {q.error && <p className="red">{q.error.message}</p>}
        {q.data?.trading_wallets.length ? (
          q.data.trading_wallets.map((w) => (
            <div key={w.address} className="wallet-record">
              <div className="panel-title">
                <strong>{w.mode}</strong>
                {badge(w.status)}
              </div>
              <code>{w.address}</code>
              <p>
                {sol(w.balance)} SOL · {w.network}
              </p>
            </div>
          ))
        ) : (
          <Empty text="No trading signer configured. Load an encrypted local keyfile through the Rust secret provider, or configure SNIPER_MAINNET_WALLET / SNIPER_DEVNET_WALLET in the service environment." />
        )}
      </div>
    </div>
  );
}
const infrastructureGroups = [
  {
    name: "Helius",
    service: "helius",
    fields: [
      ["helius_api_key", "API key"],
      ["helius_rpc_url", "RPC URL"],
      ["helius_websocket_url", "Confirmed WebSocket URL"],
      ["helius_preprocessed_url", "Preprocessed WebSocket URL"],
      ["helius_laserstream_url", "LaserStream endpoint"],
      ["helius_sender_url", "Sender Max endpoint"],
    ],
  },
  {
    name: "Jito",
    service: "jito",
    fields: [["jito_url", "Block Engine transaction endpoint"]],
  },
  {
    name: "Jupiter",
    service: "jupiter",
    fields: [
      ["jupiter_api_key", "API key"],
      ["jupiter_url", "Optional post-migration endpoint"],
    ],
  },
  {
    name: "Devnet",
    service: "devnet",
    fields: [["devnet_rpc_url", "Devnet RPC URL"]],
  },
];
function Infrastructure({ run, busy }: { run: Runner; busy: boolean }) {
  const q = useQuery<Record<string, { configured: boolean; masked: string }>>({
    queryKey: ["credentials"],
    queryFn: () => api("credentials"),
  });
  const [values, setValues] = useState<Record<string, string>>({});
  const [tests, setTests] = useState<
    Record<
      string,
      {
        status: string;
        latency_ms: number;
        last_success_ms: number;
        details?: { network?: string; slot?: number };
      }
    >
  >({});
  return (
    <>
      <div className="notice">
        Saved credentials are encrypted on the local server. HTTPS is required
        to save. Restart the paused engine to activate infrastructure changes.
      </div>
      {q.error && <p className="red">{q.error.message}</p>}
      <div className="dashboard-grid">
        {infrastructureGroups.map((group) => (
          <div className="panel" key={group.name}>
            <div className="panel-title">
              <h2>{group.name}</h2>
              {badge(
                group.fields.some(([k]) => q.data?.[k]?.configured)
                  ? "CONFIGURED"
                  : "NOT_CONFIGURED",
              )}
            </div>
            {group.fields.map(([field, label]) => (
              <div className="secret-row" key={field}>
                <label>
                  {label}
                  <input
                    type="password"
                    autoComplete="off"
                    placeholder={
                      q.data?.[field]?.configured
                        ? "•••••••• (saved)"
                        : "Not configured"
                    }
                    value={values[field] || ""}
                    onChange={(e) =>
                      setValues({ ...values, [field]: e.target.value })
                    }
                  />
                </label>
                <button
                  disabled={busy || !values[field]}
                  onClick={async () => {
                    const saved = await run(
                      "credentials",
                      { field, value: values[field] },
                      `${label} encrypted and saved`,
                    );
                    if (saved) setValues((old) => ({ ...old, [field]: "" }));
                  }}
                >
                  Save
                </button>
                <button
                  className="subtle"
                  disabled={busy || !q.data?.[field]?.configured}
                  onClick={() =>
                    void run(
                      "credentials",
                      { field, value: null },
                      `${label} cleared`,
                    )
                  }
                >
                  Clear
                </button>
              </div>
            ))}
            {
              <button
                disabled={busy}
                onClick={async () => {
                  const result = await run(
                    `test/${group.service}`,
                    {},
                    `${group.name} connection test completed`,
                  );
                  if (result && typeof result === "object")
                    setTests((previous) => ({
                      ...previous,
                      [group.service]: result as (typeof tests)[string],
                    }));
                }}
              >
                Test connection ↗
              </button>
            }
            {tests[group.service] && (
              <p className="muted">
                {tests[group.service].status} ·{" "}
                {tests[group.service].latency_ms} ms ·{" "}
                {tests[group.service].details?.network || group.name}{" "}
                {tests[group.service].details?.slot
                  ? `· slot ${tests[group.service].details?.slot}`
                  : ""}{" "}
                · {date(tests[group.service].last_success_ms)}
              </p>
            )}
            {group.service === "jupiter" && (
              <p className="muted">
                Optional fallback is disabled. Early Pump.fun entry runs
                independently.
              </p>
            )}
          </div>
        ))}
      </div>
    </>
  );
}
function Database({
  s,
  run,
  busy,
}: {
  s?: Snapshot;
  run: Runner;
  busy: boolean;
}) {
  const d = s?.database;
  return (
    <>
      <div className="metrics-grid">
        <Metric label="LOCAL DATABASE" value={d?.status || "NOT INITIALIZED"} />
        <Metric
          label="DATABASE SIZE"
          value={d ? `${(d.size_bytes / 2 ** 20).toFixed(1)} MB` : "—"}
        />
        <Metric
          label="FREE DISK"
          value={d ? `${(d.free_bytes / 2 ** 30).toFixed(1)} GB` : "—"}
        />
        <Metric
          label="CRITICAL WRITE LATENCY"
          value={d ? `${d.write_latency_us} µs` : "—"}
        />
      </div>
      <div className="panel">
        <div className="panel-title">
          <h2>RocksDB / local source of truth</h2>
          {badge(d?.status || "OFFLINE")}
        </div>
        <div className="details-grid">
          {[
            ["Database path", d?.path || "—"],
            ["WAL", d?.wal_enabled ? "ENABLED" : "—"],
            ["Critical writes", d?.sync_critical ? "SYNCHRONOUS" : "—"],
            ["Column families", d?.column_families ?? "—"],
            [
              "Pending compaction",
              d
                ? `${(d.pending_compaction_bytes / 2 ** 20).toFixed(1)} MB`
                : "—",
            ],
            ["Last successful write", date(d?.last_write_ms || 0)],
            ["Last checkpoint", date(d?.last_checkpoint_ms || 0)],
            ["Last backup", date(d?.last_backup_ms || 0)],
          ].map(([name, value]) => (
            <div key={name}>
              <small>{name}</small>
              <strong>{value}</strong>
            </div>
          ))}
        </div>
        <div className="button-row">
          {[
            ["test", "Test database"],
            ["checkpoint", "Create checkpoint"],
            ["verify", "Verify database"],
            ["backup", "Backup database"],
            ["compact", "Compact database"],
          ].map(([operation, label]) => (
            <button
              key={operation}
              disabled={
                busy ||
                !s ||
                (operation === "compact" &&
                  (s.state === "RUNNING" || !!s.positions.length))
              }
              onClick={() =>
                void run(`database/${operation}`, {}, `${label} completed`)
              }
            >
              {label}
            </button>
          ))}
        </div>
        <p className="muted">
          Checkpoints exclude signer keys and infrastructure secrets. Restoring
          production state requires a separate, explicit recovery operation.
        </p>
      </div>
    </>
  );
}
