#!/usr/bin/env bash
#
# W13 task C - the contract evolution rules that the Schema Registry does not check
# (ADR 0017).
#
# What it proves:
#   1. the contracts of THIS change follow the rules (numbers are permanent, a removal
#      is `reserved`, the kind of a field never changes under the same number);
#   2. the checker really refuses each break - every rule gets a mutated contract fed
#      to it, and the run fails if the checker accepts one of them;
#   3. the checker is not a blanket "refuse every change": an added field and a removal
#      done with `reserved` are accepted.
#
# Why a checker of our own: the registry rates a Protobuf change by field NAME, so a
# renumbering, a removal and a rename all pass `FULL` (measured in ADR 0015). Those are
# exactly the changes that break a consumer still running the previous code.
#
# How the contracts are read: both sides are compiled with protoc into a
# FileDescriptorSet (`--include_imports`), and ProtoContractDiff compares the
# descriptors - the wire view. Comments, formatting and field order are invisible to
# both, which is the point: the field order in the file is not what a reader sees.
#
# Preconditions: a warm Maven repository and the local branches fetched (`git fetch
# origin`). No service has to be running - this check is about the contracts alone, so
# unlike the registry gate it needs neither Kafka nor the Schema Registry.
#
# Usage:
#   bash requests/kafka/check_proto_evolution.sh                  # vs origin/main
#   BASE_REF=HEAD bash requests/kafka/check_proto_evolution.sh    # vs the last commit
#
# Exit code 0 = this change follows the rules and the checker was seen refusing each
# break it exists for.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_REF="${BASE_REF:-origin/main}"
EVENT_PROTO="common/common-events/src/main/proto/matchmaking/events/match_confirmed.proto"
TYPES_PROTO="common/common-proto/src/main/proto/matchmaking/common/types.proto"
EVENT_PROTO_ROOT="common/common-events/src/main/proto"
TYPES_PROTO_ROOT="common/common-proto/src/main/proto"
MODULES="common/common-events"
DIFFER="com.victor.matchmaking.events.ProtoContractDiff"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }

# The contracts a checkout owns, copied into a private tree laid out so that the import
# path protoc resolves (`matchmaking/common/types.proto`) is the one the .proto files
# write down. Mutations happen on these copies, never in the repository.
materialize() { # <destination-root> <source: git ref | worktree>
  local destination="$1" source="$2" path relative
  if [ "$source" = "worktree" ]; then
    while IFS= read -r path; do
      relative="${path#"$REPO_ROOT"/}"
      mkdir -p "$destination/$(dirname "$relative")"
      cp "$path" "$destination/$relative"
    done < <(find "$REPO_ROOT/$EVENT_PROTO_ROOT" "$REPO_ROOT/$TYPES_PROTO_ROOT" -name '*.proto' | sort)
  else
    while IFS= read -r path; do
      mkdir -p "$destination/$(dirname "$path")"
      git -C "$REPO_ROOT" show "$source:$path" >"$destination/$path"
    done < <(git -C "$REPO_ROOT" ls-tree -r --name-only "$source" -- "$EVENT_PROTO_ROOT" "$TYPES_PROTO_ROOT")
  fi
}

# Compiles a tree of contracts into one descriptor set, imports included, so a change in
# a shared type is compared too and not only in the event file.
descriptor_set() { # <tree> <output.desc>
  local tree="$1" output="$2" files=()
  while IFS= read -r file; do
    files+=("$file")
  done < <(find "$tree/$EVENT_PROTO_ROOT" "$tree/$TYPES_PROTO_ROOT" -name '*.proto' 2>/dev/null | sort)
  if [ "${#files[@]}" -eq 0 ]; then
    return 1
  fi
  "$PROTOC" \
    --proto_path="$tree/$EVENT_PROTO_ROOT" \
    --proto_path="$tree/$TYPES_PROTO_ROOT" \
    --proto_path="$WELL_KNOWN_TYPES" \
    --include_imports \
    --descriptor_set_out="$output" \
    "${files[@]}"
}

# Runs the checker the way CI does. DIFF_STATUS is the verdict: 0 compatible, 1 breaking.
run_diff() { # <baseline.desc> <current.desc> <log>
  mvn -B -f "$REPO_ROOT/pom.xml" -pl "$MODULES" exec:java \
    -Dexec.mainClass="$DIFFER" -Dexec.args="$1 $2" >"$3" 2>&1
  DIFF_STATUS=$?
}

summary_of() { grep -o 'contract.diff files=.*' "$1" | tail -1; }

expect_compatible() { # <label> <log>
  local summary
  summary="$(summary_of "$2")"
  if [ "$DIFF_STATUS" -ne 0 ]; then
    bad "$1: the checker rejected a change the rules allow (exit $DIFF_STATUS): ${summary:-no verdict line}"
    sed 's/^/        /' <(grep -E '^(FIELD|ENUM)_' "$2" || true)
  else
    ok "$1: ${summary:-compatible}"
  fi
}

expect_code() { # <label> <CODE> <log>
  if [ "$DIFF_STATUS" -eq 0 ]; then
    bad "$1: the checker accepted a break it exists for - the rule $2 is not enforced"
    sed 's/^/        /' <(grep 'contract.diff' "$3" || true)
  elif grep -q "^$2 " "$3"; then
    ok "$1: refused with $2 - $(grep -m1 "^$2 " "$3")"
  else
    bad "$1: rejected with the wrong reason (expected $2, got: $(summary_of "$3"))"
  fi
}

# ---------------------------------------------------------------------------
step "0. build the module and locate protoc"
# ---------------------------------------------------------------------------
# The checker runs from the module's classpath, so the module (and its siblings) have to
# be in ~/.m2 first; the same build is what makes the protobuf plugin download protoc.
if ! mvn -B -q -f "$REPO_ROOT/pom.xml" -pl "$MODULES" -am install -DskipTests \
    >"$TMP_DIR/build.log" 2>&1; then
  bad "the contracts module does not build"
  tail -5 "$TMP_DIR/build.log" | sed 's/^/        /'
  printf '\n== result: %d passed, %d failed (the checker cannot run)\n' "$PASS" "$FAIL"
  exit 1
fi
ok "$MODULES builds and is installed"

PROTOC="$(find "$REPO_ROOT/$MODULES/target/protoc-plugins" -maxdepth 1 -name 'protoc-*' -type f -perm -u+x 2>/dev/null | head -1)"
if [ -z "$PROTOC" ]; then
  bad "no protoc under $MODULES/target/protoc-plugins (the protobuf plugin should have downloaded it)"
else
  ok "protoc $("$PROTOC" --version) from the build"
fi

# `types.proto` imports `google/protobuf/timestamp.proto`. A standalone protoc has no
# include directory of its own, and the plugin's extracted copies live in a hashed
# directory; the well-known types come from the protobuf-java jar of the same version
# instead, which is a path the build already guarantees.
protobuf_version="$(sed -n 's/.*<protobuf\.version>\([^<]*\)<\/protobuf\.version>.*/\1/p' "$REPO_ROOT/pom.xml" | head -1)"
protobuf_jar="$HOME/.m2/repository/com/google/protobuf/protobuf-java/$protobuf_version/protobuf-java-$protobuf_version.jar"
WELL_KNOWN_TYPES="$TMP_DIR/well-known-types"
mkdir -p "$WELL_KNOWN_TYPES"
if unzip -oq "$protobuf_jar" 'google/protobuf/*.proto' -d "$WELL_KNOWN_TYPES"; then
  ok "well-known types extracted from protobuf-java $protobuf_version"
else
  bad "cannot read the well-known types out of $protobuf_jar"
fi

materialize "$TMP_DIR/current" worktree
if ! descriptor_set "$TMP_DIR/current" "$TMP_DIR/current.desc"; then
  bad "the contracts of this change do not compile with protoc"
  printf '\n== result: %d passed, %d failed (the checker cannot run)\n' "$PASS" "$FAIL"
  exit 1
fi
ok "the contracts of this change compile into a descriptor set"

# ---------------------------------------------------------------------------
step "1. this change against its merge base"
# ---------------------------------------------------------------------------
base_sha="$(git -C "$REPO_ROOT" merge-base "$BASE_REF" HEAD 2>/dev/null || true)"
if [ -z "$base_sha" ]; then
  bad "cannot resolve $BASE_REF (fetch it first: git fetch origin)"
elif [ -z "$(git -C "$REPO_ROOT" ls-tree -r --name-only "$base_sha" -- "$EVENT_PROTO_ROOT" "$TYPES_PROTO_ROOT")" ]; then
  ok "no contract at the merge base: this change brings the first one, nothing to compare"
else
  materialize "$TMP_DIR/baseline" "$base_sha"
  if descriptor_set "$TMP_DIR/baseline" "$TMP_DIR/baseline.desc" >"$TMP_DIR/baseline-protoc.log" 2>&1; then
    run_diff "$TMP_DIR/baseline.desc" "$TMP_DIR/current.desc" "$TMP_DIR/real-diff.log"
    expect_compatible "the contracts of this change keep the rules of the merge base ($(git -C "$REPO_ROOT" rev-parse --short "$base_sha"))" \
      "$TMP_DIR/real-diff.log"
    fields="$(sed -n 's/.*fields=\([0-9]*\).*/\1/p' <<<"$(summary_of "$TMP_DIR/real-diff.log")" | head -1)"
    if [ -n "$fields" ] && [ "$fields" -gt 0 ]; then
      ok "the comparison really read the contracts: $fields field number(s) checked"
    else
      bad "the checker compared 0 fields, so 'compatible' would mean nothing"
    fi
  else
    bad "the contracts of the merge base do not compile with protoc"
    tail -3 "$TMP_DIR/baseline-protoc.log" | sed 's/^/        /'
  fi
fi

# ---------------------------------------------------------------------------
step "2. the checker refuses each break it exists for"
# ---------------------------------------------------------------------------
# Each case is a copy of THIS tree with one edit: what a future pull request would look
# like if it broke a rule. The mutation lives in $TMP_DIR/mutate.py, which fails if the
# text it edits moved - a mutation that silently stopped applying would turn the control
# into a green lie.
cat >"$TMP_DIR/mutate.py" <<'PY'
import sys

case, event_proto, types_proto = sys.argv[1], sys.argv[2], sys.argv[3]


def edit(path, old, new):
    text = open(path).read()
    if old not in text:
        sys.exit("the mutation no longer matches %s: %r" % (path, old))
    open(path, "w").write(text.replace(old, new, 1))


if case == "additive":
    # A new field with a number that was never used: the supported way to grow a contract.
    edit(event_proto, "\n}\n\nmessage MatchPlayerState {",
         "\n  string correlation_note = 6;\n}\n\nmessage MatchPlayerState {")
elif case == "renumbered":
    edit(event_proto, "repeated MatchPlayerState players = 5;", "repeated MatchPlayerState players = 6;")
elif case == "removed":
    edit(event_proto, "  repeated MatchPlayerState players = 5;\n", "")
elif case == "reserved":
    # The same removal, done the documented way.
    edit(event_proto, "  repeated MatchPlayerState players = 5;\n",
         "  reserved 5;\n  reserved \"players\";\n")
elif case == "kind":
    edit(event_proto, "int32 skill_at_match = 3;", "sint32 skill_at_match = 3;")
elif case == "enum":
    # An imported enum: the shared types are compared because the descriptor set
    # carries the imports.
    edit(types_proto, "GAME_MODE_5V5 = 2;", "GAME_MODE_5V5 = 6;")
else:
    sys.exit("unknown case: %s" % case)
PY

for case in additive renumbered removed reserved kind enum; do
  tree="$TMP_DIR/case-$case"
  cp -R "$TMP_DIR/current" "$tree"
  if ! python3 "$TMP_DIR/mutate.py" "$case" "$tree/$EVENT_PROTO" "$tree/$TYPES_PROTO" \
      >"$TMP_DIR/$case-mutation.log" 2>&1; then
    bad "$case: the mutation no longer applies to the contract"
    sed 's/^/        /' "$TMP_DIR/$case-mutation.log"
    continue
  fi
  if ! descriptor_set "$tree" "$TMP_DIR/$case.desc" >"$TMP_DIR/$case-protoc.log" 2>&1; then
    bad "$case: protoc refused the mutated contract"
    tail -3 "$TMP_DIR/$case-protoc.log" | sed 's/^/        /'
    continue
  fi
  run_diff "$TMP_DIR/current.desc" "$TMP_DIR/$case.desc" "$TMP_DIR/$case-diff.log"
  case "$case" in
    additive)   expect_compatible "a field added with a free number is accepted" "$TMP_DIR/$case-diff.log";;
    reserved)   expect_compatible "a removal done with reserved + the name is accepted" "$TMP_DIR/$case-diff.log";;
    renumbered) expect_code "a renumbered field is refused" FIELD_RENUMBERED "$TMP_DIR/$case-diff.log";;
    removed)    expect_code "a field removed without reserved is refused" FIELD_REMOVED_WITHOUT_RESERVED "$TMP_DIR/$case-diff.log";;
    kind)       expect_code "a field kind change is refused" FIELD_KIND_CHANGED "$TMP_DIR/$case-diff.log";;
    enum)       expect_code "a renumbered enum value in the imported types is refused" ENUM_VALUE_RENUMBERED "$TMP_DIR/$case-diff.log";;
  esac
done

note "the registry accepts renumbering, removal and rename under FULL (ADR 0015) - the first"
note "three breaks above. A kind change is the registry's rule and this checker's; the enum rules"
note "are this checker's alone. A break has to be caught here, not in production."

# ---------------------------------------------------------------------------
printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
