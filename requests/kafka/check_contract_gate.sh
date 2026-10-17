#!/usr/bin/env bash
#
# W12 task 3 - the event-contract compatibility gate.
#
# What it proves: a change to an event contract that the Schema Registry rates as
# incompatible fails the build, in CI, instead of failing a consumer in
# production.
#
# How: the contracts of the MERGE BASE are registered in the registry by the same
# registrar, renamed, registered again by the registrar of the working tree. The
# registry is the arbiter - it accepts a compatible change and rejects an
# incompatible one - and the registrar turns that rejection into a non-zero exit.
# Registering the baseline with the baseline's own code (not by replaying the
# .proto text) matters: the registry stores the *canonical* form the serializer
# produces, so a hand-made registration of the same contract would look like a
# different schema and bump a version for nothing.
#
# Why the merge base and not the tip of the base branch: a pull request that is
# merely behind main would otherwise look like a downgrade of the contract and
# fail for the wrong reason. A gate that cries wolf is a gate people learn to
# bypass.
#
# The last step is the negative control: a contract change the registry must
# refuse is fed to it, and the registrar is shown to fail the build when
# registration cannot happen. A gate that was never seen red is not a gate.
#
# Preconditions: docker-compose stack up (zookeeper, kafka, schema-registry; see
# requests/README.md) and the base ref fetched (`git fetch origin`). The merge
# base must already contain the contracts - it does as soon as the first contract
# is merged, which is also the only situation in which a gate is meaningful.
#
# Usage:
#   bash requests/kafka/check_contract_gate.sh                    # vs origin/main
#   BASE_REF=HEAD bash requests/kafka/check_contract_gate.sh      # vs the last commit
#
# Exit code 0 = this change is compatible with the baseline and the registry
# refuses the change it was fed on purpose.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REGISTRY_URL="${REGISTRY_URL:-http://localhost:8082}"
BASE_REF="${BASE_REF:-origin/main}"
SUBJECT="${SUBJECT:-matchmaking.events.match-confirmed-value}"
EVENT_PROTO="common/common-events/src/main/proto/matchmaking/events/match_confirmed.proto"
# An imported .proto is registered under a subject named after its import path.
TYPES_PROTO="matchmaking/common/types.proto"
# Modules that own the contracts; -am also builds their dependencies.
MODULES="common/common-events"
REGISTRAR="com.victor.matchmaking.events.SchemaRegistrar"
# A FIELD KIND CHANGE is what the registry's FULL level refuses for Protobuf.
# Measured against this registry (7.7.1): "int32 -> sint32" and "string -> int32"
# are rejected, while renumbering, removing and renaming a field are all
# accepted - a field is tracked by name, so the wire break of a renumbering slips
# through. That limit is recorded in the ADR; the mutation below is one that
# really fails, and it still compiles on the Java side, so the red in CI comes
# from the registry and not from the compiler.
INCOMPATIBLE_SED="${INCOMPATIBLE_SED:-s/int32 skill_at_match = 3;/sint32 skill_at_match = 3;/}"
# A port nothing listens on: proves the registrar fails the build when the
# registration cannot happen, which is what turns a rejection into a red job.
DEAD_REGISTRY_URL="${DEAD_REGISTRY_URL:-http://127.0.0.1:60999}"

TMP_DIR="$(mktemp -d)"
BASE_DIR="$TMP_DIR/baseline-worktree"
trap 'git -C "$REPO_ROOT" worktree remove --force "$BASE_DIR" >/dev/null 2>&1; rm -rf "$TMP_DIR"' EXIT

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
step() { printf '\n== %s\n' "$1"; }

# Registers the contracts of a checkout with the real registrar and records the
# outcome in OUTCOME (build-failed / registrar-failed / ok), so the caller can
# tell a contract the registry refused from one that no longer compiles. For our
# own producer the compiler is the first line of defence and the registry the
# second; the gate is about the consumers that do not share this source.
register_contracts() {
  local dir="$1" label="$2"
  if ! mvn -B -q -f "$dir/pom.xml" -pl "$MODULES" -am install -DskipTests >"$TMP_DIR/$label-build.log" 2>&1; then
    OUTCOME="build-failed"
    return 1
  fi
  if ! mvn -B -f "$dir/pom.xml" -pl "$MODULES" exec:java -Dexec.mainClass="$REGISTRAR" \
      >"$TMP_DIR/$label-registrar.log" 2>&1; then
    OUTCOME="registrar-failed"
    return 1
  fi
  OUTCOME="ok"
  return 0
}

# ---------------------------------------------------------------------------
step "1. preconditions"
# ---------------------------------------------------------------------------
config="$(curl -fsS "$REGISTRY_URL/config" 2>/dev/null || true)"
case "$config" in
  *'"compatibilityLevel":"FULL"'*) ok "registry at $REGISTRY_URL enforces $config";;
  *) bad "registry at $REGISTRY_URL is not answering FULL (${config:-no answer}); run: docker compose up -d zookeeper kafka schema-registry";;
esac

base_sha="$(git -C "$REPO_ROOT" merge-base "$BASE_REF" HEAD 2>/dev/null || true)"
if [ -z "$base_sha" ]; then
  bad "cannot resolve $BASE_REF (fetch it first: git fetch origin), so there is no baseline to compare against"
elif ! git -C "$REPO_ROOT" cat-file -e "$base_sha:$EVENT_PROTO" 2>/dev/null; then
  bootstrapping=1
  ok "no contract at the merge base ($(git -C "$REPO_ROOT" rev-parse --short "$base_sha")): this change brings the first one"
  printf '        bootstrap case: a first version cannot be incompatible with anything, so there\n'
  printf '        is nothing to compare. From the next change on, the gate compares against the\n'
  printf '        merge base. This is not a bypass - it is the only way the first contract can land.\n'
else
  ok "baseline is merge-base($BASE_REF, HEAD) = $(git -C "$REPO_ROOT" rev-parse --short "$base_sha")"
fi

if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (the gate cannot run)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "2. register the baseline contracts (the state this change started from)"
# ---------------------------------------------------------------------------
if [ "${bootstrapping:-0}" -eq 1 ]; then
  printf '  SKIP  nothing to register: the merge base has no contracts\n'
else
  git -C "$REPO_ROOT" worktree add --detach --quiet "$BASE_DIR" "$base_sha"
  if register_contracts "$BASE_DIR" baseline; then
    ok "baseline contracts registered by the baseline's own registrar"
    grep -E "schema\.registered|schema\.registration" "$TMP_DIR/baseline-registrar.log" | sed 's/^/        /'
    base_version="$(grep -o "subject=$SUBJECT version=[0-9]*" "$TMP_DIR/baseline-registrar.log" | sed 's/.*version=//')"
    if [ -z "$base_version" ]; then
      bad "the baseline registrar did not report a version for $SUBJECT"
    fi
  elif [ "$OUTCOME" = "build-failed" ]; then
    bad "the contracts of the merge base $(git -C "$REPO_ROOT" rev-parse --short "$base_sha") do not compile"
    tail -3 "$TMP_DIR/baseline-build.log" | sed 's/^/        /'
  else
    bad "the baseline's registrar failed to register the contracts of $(git -C "$REPO_ROOT" rev-parse --short "$base_sha")"
    tail -3 "$TMP_DIR/baseline-registrar.log" | sed 's/^/        /'
  fi

  if [ -z "${base_version:-}" ]; then
    printf '\n== result: %d passed, %d failed (no baseline in the registry, so an incompatible\n   change would look like a first version and pass)\n' "$PASS" "$FAIL"
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
step "3. register this change (the gate itself)"
# ---------------------------------------------------------------------------
register_contracts "$REPO_ROOT" pr
pr_outcome="$OUTCOME"
if [ "$pr_outcome" = "build-failed" ]; then
  bad "the contracts of this change do not compile: an incompatible contract change usually breaks the producer's own code as well (the compiler is the first line of defence, the registry the second)"
  tail -3 "$TMP_DIR/pr-build.log" | sed 's/^/        /'
elif [ "$pr_outcome" = "registrar-failed" ]; then
  bad "the registrar refused this change: it is incompatible with the baseline (version $base_version of $SUBJECT)"
  grep -iE "incompatible|error_code|schema\.registration\.failed" "$TMP_DIR/pr-registrar.log" | head -3 | sed 's/^/        /'
else
  grep -E "schema\.registered" "$TMP_DIR/pr-registrar.log" | sed 's/^/        /'
  version="$(grep -o "subject=$SUBJECT version=[0-9]*" "$TMP_DIR/pr-registrar.log" | sed 's/.*version=//')"
  if [ -z "${base_version:-}" ]; then
    ok "the first version of $SUBJECT was registered as version $version (nothing to be compatible with)"
  elif [ "$version" = "$base_version" ]; then
    ok "the contract is unchanged: $SUBJECT stayed at version $base_version (the two responses agree on the contract)"
  elif [ "$version" -eq "$((base_version + 1))" ] 2>/dev/null; then
    ok "the change is compatible: $SUBJECT went from version $base_version to $version"
  else
    bad "unexpected version for $SUBJECT: baseline=$base_version now=${version:-<none>}"
  fi
  subjects="$(curl -fsS "$REGISTRY_URL/subjects" 2>/dev/null || echo '')"
  case "$subjects" in
    *"$SUBJECT"*) ok "the subject the registrar publishes to is the one that holds the baseline";;
    *) bad "subject $SUBJECT is missing from the registry ($subjects)";;
  esac
fi

# ---------------------------------------------------------------------------
step "4. negative control: a change the registry must refuse"
# ---------------------------------------------------------------------------
# The check is done through the compatibility endpoint, so the control cannot
# leave a broken contract behind if the expectation is ever wrong.
sed "$INCOMPATIBLE_SED" "$REPO_ROOT/$EVENT_PROTO" >"$TMP_DIR/incompatible.proto"
if ! cmp -s "$TMP_DIR/incompatible.proto" "$REPO_ROOT/$EVENT_PROTO"; then
  cat >"$TMP_DIR/compat.py" <<'PY'
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

registry_url, subject, schema_path, types_proto = sys.argv[1:]
# Subjects hold a path (an imported .proto); slashes must travel encoded.
quote = lambda value: urllib.parse.quote(value, safe="")
# The reference version is resolved from the registry, not assumed: an import that
# has moved on would otherwise make the check compare against the wrong schema.
with urllib.request.urlopen(
        f"{registry_url}/subjects/{quote(types_proto)}/versions/latest", timeout=30) as response:
    types_version = json.loads(response.read() or b"{}").get("version")
body = {
    "schemaType": "PROTOBUF",
    "schema": open(schema_path).read(),
    "references": [{"name": types_proto, "subject": types_proto, "version": types_version}],
}
request = urllib.request.Request(
    f"{registry_url}/compatibility/subjects/{quote(subject)}/versions/latest",
    data=json.dumps(body).encode(),
    method="POST",
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        print(json.loads(response.read() or b"{}"))
except urllib.error.HTTPError as error:
    print(f"HTTP {error.code}: {error.read().decode(errors='replace')[:200]}")
PY
  verdict="$(python3 "$TMP_DIR/compat.py" "$REGISTRY_URL" "$SUBJECT" "$TMP_DIR/incompatible.proto" "$TYPES_PROTO")"
  case "$verdict" in
    *"'is_compatible': False"*) ok "the registry refuses the mutated contract (a field kind change): $verdict";;
    *) bad "the registry did not refuse an incompatible contract: $verdict";;
  esac
else
  bad "the mutation did not apply (INCOMPATIBLE_SED no longer matches the contract)"
fi

if SCHEMA_REGISTRY_URL="$DEAD_REGISTRY_URL" mvn -B -q -f "$REPO_ROOT/pom.xml" -pl "$MODULES" \
    exec:java -Dexec.mainClass="$REGISTRAR" >"$TMP_DIR/dead-registry.log" 2>&1; then
  bad "the registrar exited 0 with no registry reachable at $DEAD_REGISTRY_URL, so a rejected contract would leave CI green"
else
  ok "the registrar exits non-zero when registration fails, so the CI step fails with it"
fi

# ---------------------------------------------------------------------------
printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
