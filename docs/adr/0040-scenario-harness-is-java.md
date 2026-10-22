# ADR-0040 — The scenario harness is Java, and the shell only orchestrates (W12–W24)

## Status
Accepted (2026-10-21)

## Context

Every sprint since W1 shipped its proof as a runnable scenario under `requests/`: an E2E with 52
checks, a load measurement, the queue reaper, the degradation matrix, two-instance routing, queue
partitioning, the multi-region stack, the game server pool, the allocation lifecycle, the Kafka
contract gate, the proto evolution check, the dead letter queue. Each of them is a `bash` script and,
by W23, a growing pile of **Python** helpers behind it.

The Python arrived for a good reason, one requirement at a time:

- the shell cannot parse JSON (`curl` returns text, `jq` is another dependency to install);
- the shell cannot read **bytes** — a dead letter's payload starts with the Protobuf wire format's
  `0x00` and is not UTF-8, and `grep`/`cut` answer `Binary file … matches`;
- a probe needs concurrency (400 logins, 400 enqueues) and a stopwatch;
- a probe needs a Redis client, a WebSocket client, a JDBC client, and an HTTP client;
- a load measurement needs percentiles from a cumulative histogram.

By the end of W23 that was **12 files**: `run_e2e.py`, `run_load.py`, `degradation_probe.py`,
`routing_probe.py`, `multiregion_probe.py`, `delay_proxy.py`, `partition_probe.py`,
`capacity_probe.py`, `db_backpressure.py`, `pending_match.py`, `slo_ratios.py`,
`check_confirmation_sweep.py`, plus four heredocs the Kafka scripts wrote into a temp directory at
run time and ran (`compat.py`, `mutate.py` and two binary parsers inside `verify_dlq.sh`).

Three problems came with them, and each one was paid for in a sprint:

1. **A second language in a Java repository is a second thing that is never compiled.** Nothing
   checks a probe until the scenario runs it, three minutes in. A rename in the product turns into a
   `TypeError` at a `json.loads` in line 300 of a script nobody looks at.
2. **The probe reimplements the product, and then disagrees with it.** `run_load.py` carried its own
   copy of the skill function: `abs(h) % 101`. The product had moved to `Math.floorMod`, which is the
   same number for a positive hash and a *different* one for a negative hash — so the load run
   picked ids that shared a skill by one function and did not share it by the other, and the
   "0 matches formed" looked like an engine defect. `partition_probe.py` reimplemented jump
   consistent hash for the same reason. A harness with its own copy of a rule is a harness that can
   blame the system for its own arithmetic.
3. **The harness's own plumbing became part of the measurement.** With
   `docker exec redis-cli SUBSCRIBE` standing in for the player's client, 24 of 1024 confirmed
   matches were announced and never arrived; each one dissolved at its 30 s deadline. The engine had
   published them correctly. A generator whose transport drops 2 % of the input cannot measure a
   99 % confirmation objective.

And one smaller one: the scenarios needed a Python that is not the system one (this machine runs
`pyenv`; a container image would need the same pinning), for code that does not ship.

## Options

### A. Keep Python and document it

Cheapest in the short run: nothing to rewrite. Rejected because it keeps all four problems and adds
one: the project's story is "one language, one artifact, one build", and a second runtime in the
repo is exactly the kind of drift an interviewer notices.

### B. Move the probes into a service module

Rejected: a probe is a **client** of the stack, not part of it. A scenario that depends on an
internal application class measures the wiring instead of the contract, and the harness would become
deployable.

### C. A Maven module that is not deployable, plus a launcher

Chosen. `tools/scenarios` compiles with the rest of the reactor (`mvn -B install`), is in no
`docker-compose`, is depended on by nothing, does not depend on Vert.x, and produces a fat jar that
`requests/mm.sh` runs.

### D. Bash only, no helpers

Rejected before it started: parsing a `+Inf` bucket, a Protobuf payload and a WebSocket frame is
logic, and logic in `awk` is the same second language with worse syntax.

## Decision

**The logic is Java, the orchestration stays bash.**

`tools/scenarios` is a Maven module with 22 classes and one entry point, `ScenarioCli`, dispatching
**41 subcommands** by name. Its fields are the scenario's, not a framework's: `pairing` picks ids
with the product's own `DeterministicSkill`, `partitions` calls the product's own `QueuePartitions`,
`queue`/`resp` speaks RESP over a socket, `scrape` reads a Prometheus exposition, `postgres` is the
JDBC driver, `e2e`/`routing`/`resilience`/`multiregion`/`backpressure`/`observability`/`lifecycle`
drive their own scenarios, `kafka verify-log|dlq-offset|dlq-checks|compat|mutate` are the four
heredocs and the two parsers, and `load` is the load harness.

`requests/mm.sh` is the only way in. It rebuilds the jar when any `.java` under `common/` or `tools/`
is newer than it, then `exec`s `java -jar`. That is the same trap as starting a service before
`mvn install`: a probe running against a stale jar answers about a product that no longer exists.

The division is the one the scripts already had:

| Stays in bash | Moved to Java |
| --- | --- |
| starting and stopping services (`mvn exec:java`, `pkill`) | parsing anything |
| `docker exec`, `docker start/stop` | comparing numbers (with a tolerance) |
| `psql`, `redis-cli`, `kafka-topics`, `kafka-consumer-groups` | reading a Redis key over a socket |
| `curl` for the setup calls a human would make | driving the scenario (login, enqueue, confirm) |
| `trap`, `EXIT`, the report of pass/fail | the percentages, the percentiles, the histograms |
| env vars, ports, `KEEP=1` | the JSON in `results-*.json` |

## How it works

- **One artifact.** `mvn -o -B install -DskipTests` builds `tools/scenarios/target/*-shaded.jar`.
  `mm.sh` calls it with `--flags` (a 60-line `Args`, not a framework) and reads one line of stdout,
  or an exit code for a comparison (`mm.sh le 1.092 1.15`).
- **Failures are loud.** A missing flag, an unreadable file and a non-JSON answer exit non-zero with
  the reason on stderr — never an empty string that a check then reads as a verdict. That rule came
  from a scenario that "failed" for an hour because a probe printed nothing and the shell compared
  nothing to nothing.
- **The harness links against the product's vocabulary.** `DeterministicSkill`, `QueuePartitions`,
  `GameMode`, `Region` come from `common-domain`; the numbers the scenarios assert are the numbers
  the services compute, by construction.
- **Clients are the JDK's.** `java.net.http` for HTTP, `java.net.http.WebSocket` for the WS
  scenarios, a plain socket and a RESP parser for Redis, JDBC for Postgres, Jackson for JSON. No
  Vert.x: a scenario must be able to exercise the API without sharing its runtime.

## How to test

```bash
mvn -B install -DskipTests
requests/mm.sh e2e                                  # 52 checks, 11 scenarios
requests/mm.sh load --players 400 --concurrency 64 --label after
bash requests/lifecycle/verify_lifecycle.sh         # 75 checks
bash requests/gameservers/verify_capacity.sh        # 58 checks
bash requests/multiregion/verify_multiregion.sh     # 54 checks
bash requests/kafka/check_proto_evolution.sh        # 12 checks, no Kafka needed
bash requests/kafka/check_contract_gate.sh          # the registry verdict, with negative controls
bash requests/kafka/verify_dlq.sh                   # the two failure shapes, by hand
```

And the property that this ADR exists for:

```bash
find . -name '*.py' -not -path '*/target/*' | wc -l    # 0
grep -r python3 requests | wc -l                       # 0
```

## Measured numbers (local stack, 2026-10-21)

| What | Before | After |
| --- | --- | --- |
| Python files in the repository | 12 | **0** |
| `python3` call sites in `requests/` | 6 (plus 12 scripts) | **0** |
| Languages a scenario needs | bash + Python + Java | **bash + Java** |
| Harness source | 3 900 lines of Python + inline heredocs | 22 classes, 6 233 lines of Java |
| Subcommands | — | 41 |
| Scenario call sites (`$MM …`) | — | 171 |
| `mm.sh` cold start (rebuild + JVM) | — | ~9 s once, then ~0.2 s per call |
| Scenarios green after the migration | — | e2e 52/52, lifecycle 75/75, gameservers 58/58, multiregion 54/54, partitioning 27/27, routing 35/35, resilience 45/45, backpressure 35/35, reaper 10/10, proto evolution 12/12, contract gate 12/12 |
| Load run after the migration (40 players, 16 concurrent) | — | 40/40 logins, 20 matches formed, **20 confirmed, 0 dissolved, 0 duplicate events** |

## Trade-offs

- **A JVM per call.** `mm.sh le 1.092 1.15` starts a JVM for a float comparison (~0.2 s). A batched
  protocol would be faster and much worse to read; the scripts make ~170 calls, so the cost is a few
  seconds per scenario, against scenarios that take minutes.
- **The harness must be built before it runs.** `mm.sh` rebuilds when a source is newer, so the
  failure mode is a slow first call rather than a stale answer — but a `java` on the PATH is now a
  precondition of `requests/`, where before it was `python3`. Same class of dependency, one fewer.
- **Bash is still the top layer, and bash has no types.** The flags of 41 subcommands are strings
  until `Args` parses them. The alternative (a Java DSL per scenario) becomes a second product.
- **The harness links against `common-domain`,** so it moves when the product moves — which is the
  point (one definition of skill, one of partitioning) and also a coupling: an old jar and a new
  service disagree, and the disagreement shows up as a scenario failure that has to be read
  carefully.
- **A faster harness finds product bugs that a slower one hides.** `curl` + `docker exec` read state
  ~80 ms after the reply; the JDK reads it in ~1 ms, and the party-cancel path was not finished by
  then — a second cancel could still see the first one in flight. That was a real defect (fixed in
  `c4b4297`), found by removing the latency that was covering it. A migration is not a refactor of
  the tests; it changes what the tests can see.

## Consequences

- `requests/` is bash + Java. A new scenario is a subcommand (or a small class) plus a `.sh` that
  orchestrates it — and the scenario logic is compiled, navigable and reachable from a unit test.
- The harness can no longer disagree with the product about a rule it does not own: skill,
  partitioning and the enums are imported, not re-implemented.
- The measurement stopped measuring its own transport: the load client is a socket, and the 24 lost
  notifications are gone by construction.
- The workbench for the remaining sprints is one language: W24's skill recalculation scenario is
  written in the same module, with the same `mm.sh`, and there is no Python anywhere to keep in step.
- Recorded as one ADR rather than one per script, because the decision is the boundary — 18 scripts
  and 12 probes are the same choice applied 30 times.
