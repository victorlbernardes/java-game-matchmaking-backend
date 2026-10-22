#!/usr/bin/env bash
# The scenario harness launcher.
#
# One command per thing a request script needs, implemented in Java (tools/scenarios) so the
# checks that describe the product can use the product's own types instead of reimplementing them.
#
#   requests/mm.sh metric http://localhost:9102/metrics 'matchmaking_queue_depth{'
#   requests/mm.sh queue players --count 16 --prefix w23-
#   requests/mm.sh gt 3 1 && echo "3 is greater"
#
# The jar is built on first use by the normal Maven build (`mvn -o -B install -DskipTests` builds it
# too). Nothing here needs the network: the launcher only runs `java -jar`.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
JAR="$REPO_ROOT/tools/scenarios/target/matchmaking-scenarios.jar"

if [ ! -f "$JAR" ] || [ -n "$(find "$REPO_ROOT/common" "$REPO_ROOT/tools" -name '*.java' -newer "$JAR" -print -quit 2>/dev/null)" ]; then
  printf 'mm.sh: building the harness (tools/scenarios)...\n' >&2
  ( cd "$REPO_ROOT" && mvn -o -B -q -pl tools/scenarios -am install -DskipTests ) >&2
fi

exec java -jar "$JAR" "$@"
