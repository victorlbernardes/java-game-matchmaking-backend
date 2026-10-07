# 0005 — Why JDK System.Logger (post-W5)

## Status
Accepted (2026-10-06)

## Context
After W5 the services were functionally correct but operationally blind:
three `System.out.println` startup lines and 20+ `Throwable::printStackTrace`
calls with no context. No request logs, no pairing/confirmation/dissolution
events. When anything failed, the operator had zero visibility into which
stage broke — a blocker for debugging W6's race conditions and for any
future production use.

## Options
1. **Keep `printStackTrace`** — zero effort, but stack traces without
   context (which matchId, which player, which operation) are useless
   in a distributed async system.
2. **SLF4J + Logback** — the industry standard, but adds two dependency
   trees to every module for a capability the JDK already provides;
   W15 will introduce OTel anyway, so a heavy logging framework now is
   a premature commitment.
3. **`java.lang.System.Logger` (JDK)** — zero new dependencies, levels,
   parameterized messages (`{0}`), pluggable backend (JUL today, and
   the API is stable enough to swap the backend later). Single-line
   structured output via `java.util.logging.SimpleFormatter.format`.

## Decision
Use `java.lang.System.Logger` now, one named logger per layer
(`matchmaking.api`, `matchmaking.api.http`, `matchmaking.api.ws`,
`matchmaking.engine`, `matchmaking.auth`, `matchmaking.redis`), with the
JUL SimpleFormatter set to an ISO single-line format at each service's
composition root:

```
%1$tF %1$tT %4$s %5$s%n   →   2026-10-06 16:39:29 INFO pair.created matchId=match-abc123 players=[p1, p2]
```

## How it works
- **matchmaking-api**: a global handler logs every request
  (`http METHOD PATH -> STATUS in Nms sub=playerId`); `fail()` logs a
  WARN for every 4xx/5xx with the reason; `WsHub` logs
  connect/disconnect, client frames (CONFIRM/DECLINE) and pushed
  notifications.
- **matchmaking-engine**: logs `pair.created`, `confirm.received`
  (count vs required players), `match.confirmed` (persisted to
  PostgreSQL), `match.dissolved` (reason), and every async failure with
  the stage name instead of a bare stack trace.
- **auth-service**: `login.issued` / `login.rejected` with playerId.
- **common-redis**: subscription lifecycle with channel names.
- All `Throwable::printStackTrace` replaced by
  `LOG.log(Level.ERROR, "context...", throwable)`.

## How to test
- `StructuredLoggingTest` captures JUL records and fails if the request
  log line disappears.
- Manual: run the W5 E2E scenario — every stage produces visible
  structured lines; killing Redis mid-match shows the error with stage
  context.

## Trade-offs
- JUL's default ConsoleHandler writes to stderr (acceptable for now;
  W15/OTel will route properly).
- The logger name is not in the formatted line (JDK SimpleFormatter
  limitation in this JDK version); services are separated by process
  until W15 aggregates logs.
- Parameterized messages are formatted by JUL at INFO+ only, so hot
  paths pay negligible cost.

## Consequences
- W6 race debugging becomes possible (pair/confirm/dissolve timeline).
- W15 (OTel + Prometheus + Grafana) inherits named loggers as the
  correlation surface; migrating to SLF4J later is a mechanical,
  call-site-level change.
