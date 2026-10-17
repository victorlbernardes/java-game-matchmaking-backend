# ADR-0020 — Observability: domain KPIs on a Prometheus scrape, traces deferred (W15)

## Status
Accepted (2026-10-17)

## Context
Until this sprint the system could answer "is it alive?" and nothing else. `/health` returns
`{"status":"UP"}` as long as the process is listening, the structured logs say what each request
did, and the W13/W14 work made the event path verifiable by hand. None of that answers the
question the operation actually has:

- are players waiting longer than they should, and **where** (mode, region)?
- is the queue deep because players are arriving or because pairing stopped?
- how many matches form and how many fall apart before confirmation, and why?
- is the event log keeping up with the facts, or is the outbox accumulating?
- is the process itself healthy (event loop not blocked, memory not climbing, HTTP error rate)?

A liveness probe cannot tell a healthy matchmaker from one whose engine stopped pairing ten
minutes ago: both are "UP". Logs can, but only by reading them, and only after someone suspects
something. The sprint asks for domain KPIs with a Prometheus/Grafana pipeline (the plan's
"OTel + Prometheus + Grafana + KPIs de domínio") so the answer is a number on a panel.

Constraints that shape the decision:

- the two services are Vert.x applications with a non-blocking event loop: reading a metric must
  never block it;
- the truth lives in Redis (queues, tickets) and Postgres (matches, outbox). A number that is
  counted in memory can disagree with the truth; the plan's rule is to measure the system, not
  our bookkeeping;
- cardinality is a budget: player ids, match ids and free-form reasons must never become labels;
- there is no dashboard to click into: the local stack has to come up provisioned from the
  repository, or nobody will look at the panels.

## Options
1. **OpenTelemetry SDK (traces + metrics) with a collector.** The textbook answer, and the one
   the plan names. It brings distributed traces across services for free, at the cost of an
   agent/collector to run, OTLP configuration in every service, and context propagation that has
   to survive Redis pub/sub and Kafka (which needs manual work - there is no HTTP header to carry
   it). For metrics alone it is also heavier: the SDK keeps per-process aggregation state.
2. **Micrometer + Prometheus exposition, scraped.** The registry is already what Vert.x's own
   metrics use (`vertx-micrometer-metrics`), Prometheus is a pull model (no agent in the service,
   failures are visible as gaps), and Grafana reads from it directly. Chosen for metrics.
3. **Derive metrics from logs (Loki/ELK).** Cheapest to add, worst to trust: every number becomes
   a log-parsing contract, latency is minutes, and an alert on a log pattern is a regex nobody
   dares change.
4. **Keep `/health` and read the logs when something feels wrong.** Rejected: it is what the
   sprints before this one did, and it cannot answer any of the questions above.

## Decision
**Metrics yes, traces not yet.** Each service exposes a Prometheus scrape on a **port of its
own** (API 9090, engine 9091), backed by one Micrometer registry per process that Vert.x also
writes its own meters into. OpenTelemetry stays a deferred decision with a criterion (below) -
the sprint's value is in the KPIs, and a trace pipeline without a real latency question to answer
would be configuration without an owner.

### One registry, two writers
`MatchmakingMetrics` creates the Prometheus registry **before** the `Vertx` instance and hands it
to `BackendRegistries.setupBackend`, so the meters Vert.x adds (HTTP server, event loop, JVM) and
the ones the service adds (`matchmaking.*`) end up in the same scrape, under one name contract
(`MetricNames`). The embedded Prometheus server of Vert.x is switched off: the exposition is
served by `MetricsEndpoint`, so it never shares the public port. In this local stack that endpoint
binds every interface (the Prometheus container reaches the host through
`host.docker.internal`); in a deployment it belongs on the management network, and that is a
one-line change because the port and the route are already separate.

### Two kinds of number, on purpose
- **Counted where the decision is made.** Arrivals (`matchmaking_tickets_enqueued_total`, at the
  API's enqueue route, only on a real creation - an idempotent replay must not inflate arrivals),
  matches formed (`matchmaking_matches_formed_total`), confirmed
  (`matchmaking_matches_confirmed_total`), dissolved (`matchmaking_matches_dissolved_total`,
  tagged by `reason`), the wait of each matched ticket, and the relay's own report
  (`matchmaking_outbox_published_total` / `_retried_` / `_parked_`). Nobody else can count these,
  and a counter that resets on restart is what Prometheus expects: `rate()` handles the reset and
  the durable truth stays in Postgres.
- **Derived from state.** Queue depth and the head's wait are read from Redis, the outbox backlog
  from Postgres, every `KPI_POLL_INTERVAL_MS` (2 s), by a poller that publishes into a holder the
  gauge reads. A Micrometer gauge is read synchronously at scrape time and these stores are
  asynchronous: querying them from the scrape would block the event loop, so the scrape reads the
  last poll instead. The cost is bounded staleness (≤ 2 s) and it is written down here; the gain
  is that these numbers cannot disagree with Redis or Postgres, because they *are* Redis and
  Postgres.

### The KPIs, and what each one answers

| Metric (Prometheus) | Kind | Question it answers |
| --- | --- | --- |
| `matchmaking_tickets_enqueued_total{mode,region}` | counter | How many players arrived, where. Denominator of every rate. |
| `matchmaking_queue_depth{mode,region}` | gauge (Redis) | How many are waiting right now. |
| `matchmaking_queue_oldest_wait_seconds{mode,region}` | gauge (Redis) | How long the player at the head has been waiting - the depth alone lies, this one does not. |
| `matchmaking_match_wait_seconds{...}` (histogram) | timer | Wait from enqueue to match, p50/p95/p99. The product metric. |
| `matchmaking_matches_formed_total{mode,region}` | counter | Throughput of pairing. |
| `matchmaking_matches_confirmed_total{mode,region}` | counter | Matches that actually happened. |
| `matchmaking_matches_dissolved_total{reason}` | counter | Matches that fell apart, by `timeout` / `declined`. |
| `matchmaking_outbox_backlog{status}` | gauge (Postgres) | PENDING = the backlog, PARKED = what needs a human. |
| `matchmaking_outbox_oldest_pending_seconds` | gauge (Postgres) | How long the oldest unpublished event has been waiting: a stopped relay, not a busy one. |
| `matchmaking_outbox_publish_lag_seconds` | gauge (Postgres) | Fact to broker, for the newest published row. |
| `matchmaking_outbox_published_total` / `_retried_total` / `_parked_total` | counters | What the relay is doing. |
| `vertx_http_server_requests_total{code,method}`, `vertx_http_server_response_time_seconds`, `vertx_pool_*`, `vertx_redis_*`, `vertx_sql_*`, `jvm_*` | Vert.x/JVM | Transport and runtime health (event loop, pool, memory, HTTP codes). Names as Vert.x 5.2 exposes them. |

Every tag is a closed set: `mode` (1v1/5v5), `region` (SA/NA/EU), `status` (PENDING/PUBLISHED/
PARKED), `reason` (timeout/declined). A player id, a match id or a free-form message as a label
would create unbounded series - the free-form detail stays in the log line next to the metric
(`match.dissolved matchId=… reason=timeout detail=declined by p1`).

One consequence is documented rather than hidden: the dissolution counter is tagged by reason
only. The pending match in Redis carries its roster and its deadline, not the mode and region, and
looking the tickets up to tag a counter would put reads on the dissolution path. Per mode/region
the answer is the difference between `matches_formed` and `matches_confirmed` - the same answer
from numbers already collected.

### The local stack is code
`docker-compose.yml` runs Prometheus (host port 9095) and Grafana (3000); `ops/prometheus/`
holds the scrape config and `ops/grafana/` the provisioned datasource, provider and dashboard, so
a fresh machine gets panels without clicking. Prometheus scrapes two jobs,
`host.docker.internal:9090` and `:9091`.

### Alerts and SLOs: next sprint, deliberately
Panels answer "what is happening", not "should someone wake up". Thresholds without a budget are
noise, so the alerting rules are W16's SLO work (ADR 0021); this sprint builds the numbers they
will be written against.

## How it works
1. the process creates the registry, hands it to Vert.x, registers its KPIs;
2. `MetricsEndpoint` serves `/metrics` (Prometheus text) and `/health` on 9090 (API) and 9091
   (engine);
3. Prometheus scrapes both every 5 s; Grafana reads Prometheus;
4. the derived gauges are refreshed every 2 s from Redis/Postgres; the counters move with the
   events themselves;
5. `requests/observability/check_metrics.sh` drives one real match and asserts the numbers moved
   and that they agree with Postgres.

## How to test
```bash
# the module's own contract (naming, exposition)
mvn -B -pl common/common-metrics test

# the whole story by hand (needs the stack + both services running)
bash requests/observability/check_metrics.sh
```
The scenario reads `http://localhost:9090/metrics` (API) and `:9091` (engine) directly, then the
same series through Prometheus (`:9095/api/v1/query`) and the provisioned dashboard through
Grafana's API (`:3000/api/search`), so it fails if the service stops exposing, if Prometheus stops
scraping, or if the dashboard is dropped from the provisioning.

## Measured (2026-10-17)

Environment: the four services running on the host (`mvn exec:java`), Redis/Postgres/Kafka from
docker compose, Prometheus 3.13.4 and Grafana 12.2.0 running from their binaries on the host
(9095 and 3000) because this machine could not pull the container images - the compose service
definitions and the provisioning files are the ones in this repository, and the only difference
is the scrape target host (`ops/prometheus/prometheus.host.yml` points at `localhost`,
`prometheus.yml` at `host.docker.internal`).

- `bash requests/observability/check_metrics.sh`: **35 checks, 0 failures** in ~25 s
  (25-27 s across runs; exit 0).
- `mvn -pl common/common-metrics test`: 1 test - the naming contract and the exposition. The
  three suites the change touches are green together (`common-metrics` 1, `matchmaking-api` 17,
  `matchmaking-engine` 46 = **64 tests, 0 failures**) with the services stopped and Redis flushed,
  as the runbook requires.
- The derived gauges tracked reality: the head's wait read **4.0 s** four seconds after a player
  enqueued, and depth and wait both went back to **0** the moment the ticket was cancelled.
- The counters moved on the events: `tickets_enqueued_total` +1 at the boundary,
  `matches_formed_total` +1, `match_wait_seconds_count` +2 (one per player),
  `matches_confirmed_total` +1.
- The outbox gauges summed to the table (5 rows, `PARKED` 0), and the newest publish lag was
  **0.035 s** in Postgres and **0.0 s** in the gauge on a steady state; a run started right after
  a `flushall` (nothing published yet) measured **1 s**, which is the gauge's whole-second unit,
  not drift.
- The pipeline: Prometheus scraped both jobs (`up == 1`) and answered
  `sum(matchmaking_queue_depth)`; Grafana answered the same query through the provisioned
  datasource (`/api/ds/query`), with the dashboard from `ops/grafana/dashboards/` loaded.

### What the wiring taught (kept for the next person)

- **A ZSET score comes back as a double, and nested.** `ZRANGE key 0 0 WITHSCORES` returns
  `[[member, score]]` - one pair per member - and the score renders as `1.792232380054E12`, so
  `Long.parseLong` fails on it. The poller parses it as a double and rounds (epoch millis fit in
  a double exactly, 2^53 >> 1.8e12), accepts both the nested and the flat shape, and logs a
  warning when a non-empty queue has no readable score instead of publishing a silent zero.
- **Vert.x 5's metric names are `vertx_*`.** HTTP is
  `vertx_http_server_requests_total{code,method}` and
  `vertx_http_server_response_time_seconds{...}`, a summary (count/sum/max): the mean is
  sum/count and there is no p95 without a histogram. Pools are `vertx_pool_*`, the datastores
  `vertx_redis_*` / `vertx_sql_*`. There is no `http_server_requests_seconds` family.
- **`BackendRegistries.setupBackend(options, registry)` must run before the `Vertx` instance
  exists**, or Vert.x installs a backend of its own first and the domain meters end up in a
  registry nobody scrapes.
- Vert.x logs `This Gauge has been already registered (vertx.net.client.active.connections)`
  once at startup when the backend is set up this way: the client meters are bound twice. It is
  noise; the meter is exposed.

## Trade-offs
- **Bounded staleness on the derived gauges (≤ 2 s).** Accepted: the alternative is blocking a
  scrape on the event loop, which is worse than a number that is two seconds old.
- **Counters are per process.** A restart resets them; `rate()` and `increase()` handle that, and
  the durable totals stay in Postgres. The counter also only counts what this process saw - with
  two engines, `matches_confirmed_total` is summed across instances, which is exactly what
  Prometheus is for, as long as no one reads a single instance's counter as the total.
- **The metrics port is open in the local stack.** Documented, with the production shape (bind to
  the management network, or scrape through a sidecar) named above.
- **Percentile histograms carry extra series** (`matchmaking_match_wait_seconds_bucket`). Accepted
  for the one metric whose distribution is the product quality; the rest are counters and gauges.
- **Metrics are not logs and not traces.** A KPI says *that* wait times doubled, never *why*; the
  structured log line and the event's `correlationId` are still the way to find out which match.
- **No alerts yet** (W16), and no trace pipeline (deferred).

### Why not OpenTelemetry now
What OTel would add tomorrow is **traces**: one request's path across API, engine and orchestrator
with per-hop timings. The pieces it needs are not free of work here - context has to be propagated
through Redis pub/sub and Kafka, and there is no collector in this stack - and the questions this
sprint had to answer are metric questions ("how many, how long, how deep, how far behind"). The
decision is deferred, not dropped, with a criterion: revisit when a latency problem spans more
than one service and cannot be localised from the KPIs and the logs. The event contract already
carries `correlationId` (= the match id today), which is the seam a trace id would use
(ADR 0016).

## Consequences
- The engine now has a reason to expose an HTTP surface (metrics only) and it does so on its own
  port, so the "no public HTTP in the engine" property still holds.
- The API counts arrivals at the boundary, where every arrival is visible; the engine counts
  what happened to them. Neither duplicates the other's numbers.
- `common-metrics` is a new shared module: the metric contract (names, tags, tag values) is now a
  reviewed artifact, not a string typed twice in two services.
- The outbox gained read-only queries (`countByStatus`, `oldestPendingAge`, `newestPublishLag`)
  that are also the operator's view of the queue (ADR 0018).
- A dashboard in `ops/grafana/dashboards/` is now part of the review surface:
  `requests/observability/check_metrics.sh` asserts that the dashboard is provisioned, that
  Prometheus can answer a KPI query and that Grafana can answer one through the provisioned
  datasource - so a renamed metric or a dropped dashboard turns the scenario red.
