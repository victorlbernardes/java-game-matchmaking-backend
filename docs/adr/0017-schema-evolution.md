# ADR-0017 — Contract evolution rules: a field number is permanent (W13)

## Status
Accepted (2026-10-17)

## Context
ADR 0015 put the Schema Registry in CI and measured what its `FULL` level actually refuses:
a *kind* change of a field is rejected, and **renumbering, removing and renaming a field
are all accepted** — because the registry tracks a Protobuf field by *name*, while the wire
is read by *number*. So the two changes that break a running consumer most often were the
two the gate let through.

That is not a corner case, it is the normal shape of a deploy. A producer and a consumer
of the same topic are almost never the same artifact: the engine publishes, the
orchestrator, the telemetry consumer and the skill service read. Between two deployments
at least one old writer lives next to one new reader, so for a window of time the contract
that is being written is the *previous* one. Under that window:

- **renumbering** `players = 5` to `= 6` compiles, passes the registry, passes code review
  (`git diff` shows a digit), and makes every reader of the previous contract read field 6
  as `players` — or read nothing, silently;
- **removing** a field compiles when the producer's own code stops using it, passes the
  registry, and makes the old writer's bytes land on a number the reader no longer knows;
- **renaming** a field keeping its number is a renumbering seen from the other side;
- **reusing a number** for a new field with a different type is the same break again, and it
  is what a removal without `reserved` invites the next person to do.

`common/common-events/README.md` stated the rule from W12 on ("field numbers are permanent").
Nothing enforced it, and a rule in prose is a hope.

## Options
1. **Trust `FULL` plus the Java compiler** — for *our* producer the compiler catches a lot:
   remove a field and the mapper stops compiling. It does not catch a renumbering that keeps
   the field name (the generated Java moves the number and everything still compiles) and it
   cannot catch anything about a consumer deployed from another artifact, which is exactly
   who the contract is for. Rejected: this is the hole that was measured.
2. **`buf breaking`** — the tool that exists for precisely this problem: it compares a
   checkout against a baseline over git and its `WIRE` policy covers renumbering, removals,
   `reserved`, presence and enum values. It would work, and it is *the* answer once the
   contracts are consumed by a team we do not deploy with. Not chosen *now*: it is a second
   toolchain in the build (a `buf.yaml`, a version to pin, a module cache in CI and a new
   failure mode) for a check over two `.proto` files in one repository; and its policy is
   not ours to read — ours is in this ADR, next to the code. Recorded as a deferred
   decision (`MelhoriasFutura.md`), not as a rejection.
3. **Compare the descriptor sets of the baseline and the change with a checker of our own**
   — chosen. `protoc` already produces the wire view (`FileDescriptorSet`); the comparison is
   a pure function over descriptors; the rules live in one file, in the same language as the
   rest of the build, and the failure message can say what to do instead.
4. **Never break a contract: version the topic** — a new topic and a new subject per
   breaking change is the real answer when a break cannot be avoided, and it is exactly why
   this gate can afford to be strict. Kept as the documented way out (see Consequences),
   not as a substitute for the rules: paying a new topic for what is usually an accident is
   a bad trade.
5. **Review discipline** — "the reviewer will notice". The W12 gate exists because that
   argument already failed once; a review sees a digit in a diff, and the person who wrote
   the diff has no signal that anything is wrong. Rejected.

## Decision
The evolution rules of an event contract are enforced in CI, from the descriptors, by
`ProtoContractDiff` — and the only breaks that are refused are the ones that break a
deployed peer:

| # | Rule | What it protects | Enforced by |
|---|------|------------------|-------------|
| 1 | a published **field number** is never reused for another meaning | a reader of the old contract | `FIELD_RENUMBERED`, `FIELD_NUMBER_REUSED` |
| 2 | a **field that goes away reserves its number and its name** | the next person to pick a number | `FIELD_REMOVED_WITHOUT_RESERVED` (fails), `FIELD_NAME_NOT_RESERVED` (advisory) |
| 3 | a field's **kind** (type + label) never changes under the same number | the value decoded by a reader | `FIELD_KIND_CHANGED` |
| 4 | an **enum value number** is permanent, and a value that goes away is reserved | a writer of the old contract | `ENUM_VALUE_RENUMBERED`, `ENUM_VALUE_NUMBER_REUSED`, `ENUM_VALUE_REMOVED_WITHOUT_RESERVED` |
| 5 | **growth is additive**: a new field takes a new number | everything above | nothing to enforce — it is the accepted case and is tested as such |
| 6 | making a field explicitly `optional` is visible, not silent | a reader that starts receiving the default value | `FIELD_PRESENCE_CHANGED` (advisory) |

Growing a contract is therefore: **add** a field with a number never used before; to retire
one, **stop writing it** and `reserved` both its number and its name; to change its type,
add a **new** field and stop writing the old one.

## How it works

### The comparison is over the wire view, not over the text
Both sides are compiled by `protoc` into one `FileDescriptorSet` each
(`--include_imports`), and the checker walks them in parallel: files by name, messages and
enums by fully-qualified name (nested types included), fields by name and by number. A file
or a message that exists only in the new contract is an addition: nothing a peer sent
before can break against it. Fields are compared by *both* keys on purpose — by name, to see
a renumbering; by number, to see a removal whose number was taken over.

Comparing descriptors instead of `.proto` text is what makes the verdict about the contract
and not about the file: a comment, a re-indentation and a reordered `message` block are
invisible here, exactly as they are on the wire. It also removes the temptation to
pattern-match `sed` on the text, which is how a checker starts reporting things nobody can
act on.

### Two severities, and only one of them fails the build
A **violation** is a break: it fails the build with a message that names the field and says
what to do instead (`… Add a new field and stop writing the old one instead.`). An
**advisory** is legal on the wire but worth a human look, and it is printed without failing:
`FIELD_NAME_NOT_RESERVED` (the number is `reserved`, the name is not — a future field could
be read as this one) and `FIELD_PRESENCE_CHANGED` (a field became explicitly `optional`, so
a reader that used to see "absent" can start seeing `0`). A gate that refuses everything is
a gate people learn to route around; a gate that explains the three cases where it is right
gets argued with only when it is wrong.

### The checker says what it compared
Every run ends with one line that the shell scripts read:

```text
contract.diff files=3 messages=4 fields=15 enums=2 enumValues=7 violations=0 advisories=0 verdict=compatible
```

The unit tests assert the shape of that line, and the script asserts that `fields` is
greater than zero: "compatible" has to mean *compared and found equal*, never *compared
nothing*.

### The CI step runs the same script a human runs
`requests/kafka/check_proto_evolution.sh` is the whole check, and the `contracts` CI job
runs that file — a gate duplicated in workflow YAML drifts from the one you can run by hand.
The script:

1. builds `common/common-events` (which is also what makes the protobuf plugin download
   `protoc`, pinned by the same version property the build uses — 3.25.4, the version the
   Confluent serializer declares) and extracts the well-known types from the `protobuf-java`
   jar of that version, since a standalone `protoc` has no include directory of its own;
2. materializes the contracts of the merge base (`git ls-tree` + `git show`, never the
   repository's working tree) and of the change, and compiles both;
3. compares the change against its baseline — **and fails when the baseline has contracts
   and the comparison breaks a rule**;
4. feeds six mutations of *this* tree to the checker: four it must **refuse**
   (renumbering, removal without `reserved`, kind change, renumbered enum value in the
   imported types) and two it must **accept** (a field added with a free number, a removal
   done with `reserved 5;` + `reserved "players";`). A mutation whose anchor text has moved
   makes the mutation script fail, so a control cannot silently stop applying.

It needs neither Kafka nor the registry: this check is about the contracts alone.

### Measured (dev machine, warm Maven repository)
- the whole script: **57.4 s** for **12 checks** (one build 12.3 s, four `protoc` runs,
  seven checker runs at ~4.75 s each — Maven startup included);
- the comparison reads the real contracts: `files=3 messages=4 fields=15 enums=2
  enumValues=7` (the event file, the shared types it imports and `timestamp.proto`);
- **4 of 6 mutations refused** with exit 1 and the rule named, **2 accepted** with
  `verdict=compatible`;
- mutation proof, run by hand: renumbering `players = 5` to `= 6` **in the working tree**
  turned step 1 red — `violations=1 … verdict=breaking` with
  `FIELD_RENUMBERED MatchConfirmed.players` — and the tree was restored with
  `git checkout --`;
- unit: **21 tests** in `ProtoContractDiffTest` (one per rule, plus the real contract
  against itself and against a renumbering of itself), 0.5 s.

## How to test
```bash
# the rules, in isolation (no protoc, no registry, no Kafka)
mvn -B -pl common/common-events test -Dtest=ProtoContractDiffTest

# the whole check, the way CI runs it
bash requests/kafka/check_proto_evolution.sh

# the mutation proof by hand
sed -i '' 's/repeated MatchPlayerState players = 5;/repeated MatchPlayerState players = 6;/' \
  common/common-events/src/main/proto/matchmaking/events/match_confirmed.proto
bash requests/kafka/check_proto_evolution.sh   # step 1 must be red (FIELD_RENUMBERED)
git checkout -- common/common-events/src/main/proto/matchmaking/events/match_confirmed.proto
```

## Trade-offs
- **A checker of our own** rather than `buf breaking`: ~250 lines to maintain, and the
  rules are ours to get wrong. In exchange it adds no toolchain to the build, it is
  readable in the same language as the rest of the repository, and its messages say what to
  do instead of naming a policy level. The day a second team consumes these contracts, `buf`
  (or an equivalent) is the right replacement — recorded in `MelhoriasFutura.md`.
- **`reserved` ranges** are read with their two different conventions (`DescriptorProto`
  end-exclusive, `EnumDescriptorProto` end-inclusive) — a place to get it wrong that is
  pinned by a test on each convention.
- **Advisories can be ignored.** That is deliberate: they are the cases where the gate would
  be wrong to block (a reserved name is hygiene; an explicit `optional` is legal). The cost
  is that a tree full of advisories would still be green.
- **Not covered, on purpose** (documented rather than enforced): a removed *message* (a
  removal that leaves no number behind), a `oneof` restructure, field options such as
  `deprecated`, `json_name` changes (a JSON mapping concern, not the binary wire), and
  `EventMetadata.schemaVersion` bumps, whose source of truth is the registry version derived
  from the schema itself.

## Consequences
- A contract change is now either **additive** or has to be written as a *deliberate*
  retirement (`reserved` number + name). A break with no way out is a new topic: a new
  subject, a new contract, and this gate never has to be argued with.
- The producer-facing summary of the rules lives in `common/common-events/README.md`; the
  ADR is the decision and `ProtoContractDiff` is the enforcement — one place each.
- The registry gate (ADR 0015) and this checker now cover the two halves: the registry
  decides what is compatible *by its rules* and versions the schema; the checker decides
  what breaks a *peer that is already deployed*. Neither is complete alone, and both run in
  the same CI job.
- Any future event type inherits the rules for free: the script discovers the contracts of
  the two proto roots, so nothing here is specific to `MATCH_CONFIRMED`.
