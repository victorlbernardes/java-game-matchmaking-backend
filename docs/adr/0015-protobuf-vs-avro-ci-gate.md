# ADR-0015 — Protobuf over Avro, and the compatibility gate running in the build (W12)

## Status
Accepted (2026-10-17)

## Context
ADR 0016 fixed the shape of the first event (one topic per event, envelope as a
message) and how a producer obtains its schema (`auto.register.schemas=false`, the
schema is registered outside the running service). Two questions were left open, and
both are about trust in the contract rather than about the event itself:

1. **Which schema format.** The contract has to be checkable at compile time, shared
   with the IDL the project already uses for internal calls, and readable by a
   consumer that is not this codebase (a telemetry pipeline, another service, another
   language).
2. **Where the compatibility verdict is enforced.** The Schema Registry can be asked
   to decide compatibility on every `POST`, but *when* that `POST` happens is a
   choice — in the build, or from a running pod.

There is a third question the first two hide: a gate nobody has seen fail is
decoration. Whatever was built had to be shown refusing a change, and the limits of
the registry's `FULL` level had to be measured rather than assumed, because that
level is the whole basis of the verdict.

## Options
**Schema format**

1. **Avro** — self-describing (the schema travels with the data), supported by every
   registry and consumer tool, no codegen required. But it would be a *second* IDL
   next to the Protobuf already used for gRPC, its records are not checked at compile
   time, and an Avro field is identified by name (aliases bridge renames), which
   moves compatibility arguments from the wire format to convention.
2. **JSON Schema** — no codegen and human-readable, but it versions nothing on the
   wire, checks nothing at compile time, and pushes the contract back into maps and
   strings — the shape rule 11 exists to keep out of the layers.
3. **Protobuf** — the same IDL the internal RPC already uses, generated at build
   time, so a producer that no longer compiles is caught before anything is
   published. Fields have explicit numbers, which is what makes wire compatibility a
   property of the contract instead of an accident. Cost: it is *not*
   self-describing, so a consumer needs the registry and the schema id to decode —
   accepted, and paid in ADR 0016 by pinning the registry as part of the wire.

**Where the gate runs**

1. **Runtime auto-registration** (`auto.register.schemas=true`) — the producer
   registers its schema on the first record. Convenient in development, unusable as a
   gate: any pod running a new `.proto` creates a version with no review, the version
   on the topic depends on which pod started first, and an incompatible change is
   discovered by a *consumer*, in production, as a decoding failure.
2. **Build time, in CI** — the version is created by a reviewed merge, and the same
   step fails the build when the change is incompatible. The registry becomes the
   arbiter of a decision made in a pull request, and the failure lands on the author.

## Decision
- **Protobuf**, for the single-IDL and compile-time reasons above. The
  non-self-describing cost is accepted and covered by ADR 0016's pinned registry.
- **The compatibility verdict is enforced in the build.** Services run with
  `auto.register.schemas=false`; the contract is registered by `SchemaRegistrar` and
  the verdict is produced by the registry's `FULL` level.
- **The gate is `requests/kafka/check_contract_gate.sh`**, run by the `contracts` CI
  job. It registers the contracts of the **merge base** and then the contracts of the
  working tree; the registry accepts a compatible change and refuses an incompatible
  one, and the registrar turns that refusal into a non-zero exit.
- **Two negative controls run on every invocation**, because a gate that has never
  been red is an assertion, not evidence: the registry must refuse a mutated
  contract, and the registrar must exit non-zero when registration cannot happen.

## How it works

### The baseline is the merge base, not the tip of the base branch
`baseline = git merge-base <base ref> HEAD`. Comparing against the tip of `main`
would make any pull request that is merely *behind* look like a downgrade of the
contract and fail for a reason that has nothing to do with the change. A gate that
cries wolf is worse than no gate: people learn to ignore it and then to disable it.
With the merge base, the comparison has exactly one meaning — *what does this change
do to the contract it started from?*

### The baseline is registered by the baseline's own registrar
In a worktree of the merge base, the contracts are compiled and registered by that
revision's own `SchemaRegistrar`; then the same registrar runs in the working tree.
Registering the baseline by replaying the `.proto` text through the REST API was
tried first and discarded: the registry stores the **canonical** form the serializer
produces (`.matchmaking.common.EventMetadata` where the file says
`matchmaking.common.EventMetadata`), so a hand-made registration of the identical
contract looks like a different schema and bumps a version for nothing. One
mechanism, one canonical form, no drift.

### Reading the outcome
| Baseline | This change | Result |
|----------|-------------|--------|
| v1 | v1 (same contract) | pass — "the contract is unchanged" |
| v1 | v2 (compatible evolution) | pass — "v1 → v2" |
| v1 | refused by the registry | **fail** — the registrar exits 1, the job is red |
| no contract (bootstrap) | v1 | pass, with the notice — a first version cannot be incompatible with anything |

The bootstrap line matters: the first contract of a new event has no baseline, and
failing there would block the only change that can create one. It is reported
explicitly in the output (`bootstrap case: … This is not a bypass`), and from the
next change on the gate compares against the merge base.

### The negative controls
- **The registry must refuse.** A mutated contract is submitted to the
  **read-only** compatibility endpoint (`POST /compatibility/subjects/<subject>/versions/latest`),
  which answers `{"is_compatible": false}`. Read-only on purpose: a wrong expectation
  in the control cannot leave a broken schema registered.
- **The registrar must fail the build.** The registrar is run with
  `SCHEMA_REGISTRY_URL` pointing at a port nothing listens on and must exit non-zero.
  A rejected registration and an unreachable registry take the same path in
  `SchemaRegistrar` (catch → non-zero exit), which is what turns a verdict into a red
  CI step.

### What `FULL` actually catches (measured on registry 7.7.1)
| Contract mutation | Verdict |
|-------------------|---------|
| `string` → `int32` on a field (scalar kind) | **rejected** |
| `int32` → `sint32` on a field (scalar kind) | **rejected** |
| enum field → `string` | **rejected** |
| renumbering a field (`= 2` → `= 9`) | accepted |
| removing a field | accepted |
| renaming a field (same number) | accepted |
| adding a field | accepted (compatible by definition) |

This is the most important measurement in this ADR, and it is uncomfortable: the
registry's Protobuf checker tracks fields **by name**, so a renumbering or a removal
— both real wire breaks for a reader built on the older contract — pass `FULL`
silently. `FULL` is therefore necessary but not sufficient, and two things cover the
rest: our own producer stops compiling (the mapper is compiled against the generated
classes, so a kind change breaks the build even before the registry sees it), and
explicit evolution rules (reserved numbers, deprecation instead of removal) belong to
ADR 0017 in W13 rather than to a level the registry cannot enforce.

### The CI job
`contracts` runs in parallel with `build` and uses the **same `docker-compose.yml`**
as development — a hand-written services block in the workflow would duplicate the
Kafka topology (the two listeners, the registry's `FULL` setting) and the two would
drift, leaving CI validating a stack nobody runs. The project name is fixed
(`docker compose -p game-matchmaking-backend …`) so the container names the scripts
already know stay stable, and the checkout uses `fetch-depth: 0` because the gate
needs the merge base.

Order inside the job: start the stack → wait for `healthy` → `check_kafka.sh` (the
task-1 smoke check, which also verifies the registry is still `FULL` — the level the
whole gate rests on) → `check_contract_gate.sh`.

### Measured (dev machine, warm Maven repository)
| Metric | Value |
|--------|-------|
| `check_contract_gate.sh`, bootstrap case (one build + registrar + both controls) | 23 s |
| build of the contract modules (`-pl common/common-events -am install -DskipTests`) | 10 s |
| one registrar run | 6 s |
| `check_kafka.sh` (9 checks) | 25 s |
| stack up → healthy (task 1) | 54 s |
| compatible change (demo, git clone) | `v1 → v2`, exit 0 |
| incompatible change that still compiles (demo) | exit 1, registry reason `FIELD_SCALAR_KIND_CHANGED` at `#/MatchPlayerState/2` |
| gate on a merge base without contracts (this PR) | exit 0, bootstrap notice |

The registry reason is the point of the whole exercise: the red build says which
field the registry refused, in a pull request, in seconds — instead of a consumer
failing to decode in production. Getting there required printing the exception's
cause chain in `SchemaRegistrar`; without it the log said "registration failed" and
hid the verdict.

## How to test
```bash
bash requests/kafka/check_contract_gate.sh             # vs origin/main
BASE_REF=HEAD bash requests/kafka/check_contract_gate.sh   # vs the last commit
```
Both must end in `result: N passed, 0 failed` and exit 0. The three paths of the
table above were exercised in a throwaway `git clone` (so the real working tree was
never mutated): unchanged, a compatible field addition, and a kind change that
compiles — the last one must produce the `FAIL`, the registry's
`FIELD_SCALAR_KIND_CHANGED` and exit 1.

In CI, the `contracts` job is the same script; a red job is the property this ADR
buys.

## Trade-offs
- **CI now needs Docker and a broker.** The job adds roughly two minutes (stack up,
  smoke check, gate) in parallel with `build`, so wall-clock time is close to
  unchanged but runner minutes are not.
- **The registry in CI is ephemeral.** It starts empty every run, which is precisely
  why the baseline comes from git. A persistent staging registry would also catch
  "the version that is deployed", which is a stronger guarantee this project does not
  have infrastructure for.
- **The gate protects against the base branch, not against production.** A change
  already merged and deployed is out of reach; reverting is a contract change of its
  own.
- **`FULL` does not cover renumbering, removal or renaming** (measured above). Covered
  by the compiler for our own producer and to be covered by rules in ADR 0017.
- **Deleting an event is invisible to the gate**: no schema is submitted, so there is
  nothing to reject. The convention is to deprecate rather than delete inside the
  hour-long horizon of this project; a check would have to compare the set of subjects
  to the set of contracts.
- **The gate needs a baseline to be meaningful**, hence the explicit bootstrap notice
  instead of a silent pass. A gate that passes because it had nothing to compare must
  say so in the log.
- **The negative control uses the compatibility endpoint, not a real registration**
  (so it cannot pollute the subject), and the registrar's failure path is proven
  separately with an unreachable registry. It is evidence by construction, not a
  rehearsal of the exact production call.

## Consequences
- An incompatible contract change fails in the pull request, with the registry's
  reason in the log, instead of failing a consumer in production — the property this
  task exists for.
- `FULL` is now load-bearing configuration: `check_kafka.sh` asserts it and the
  negative control fails if someone lowers it, so the level cannot be quietly
  weakened.
- Adding an event is: a `.proto`, a mapper, a topic constant, a line in the
  registrar — and its first pull request takes the documented bootstrap path.
- The limits measured here are an input to ADR 0017 (W13): schema evolution rules
  cannot be delegated to `FULL` for Protobuf, and reserved field numbers have to be
  part of the contract review.
