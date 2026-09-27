#!/usr/bin/env bash
# Thirty-second end-to-end check: render a short loop with ffmpeg, run the split origin
# (publish-server + edge-server) against two Redis instances, publish from two pipelines with
# pipeline A dropping 20% of its segments, and require every simulated cache to receive every
# segment with no gaps and nothing corrupt. Used by CI and runnable locally.
set -euo pipefail
cd "$(dirname "$0")/.."
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
OUT=run/e2e
mkdir -p "$OUT"

[ -f publish-server/build/libs/publish-server.jar ] || ./gradlew -q build -x test
[ -d data/segments/480p ] || "$JAVA" -jar packager/build/libs/packager.jar render --out data/segments --seconds 20

docker compose up -d redis-write redis-read >/dev/null
for p in 27379 27380; do
  for i in $(seq 1 50); do (echo PING | nc -w 1 localhost $p | grep -q PONG) && break; sleep 0.2; done
done

NOW=$(( $(date +%s) * 1000 ))
EPOCH=$(( NOW - NOW % 2000 - 20000 ))
sed "s/^    epoch: .*/    epoch: $EPOCH/" config/events.yaml > "$OUT/events.yaml"

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

"$JAVA" -Xmx512m -jar publish-server/build/libs/publish-server.jar --origin.events-file="$OUT/events.yaml" > "$OUT/publish.log" 2>&1 & pids+=($!)
"$JAVA" -Xmx1g -jar edge-server/build/libs/edge-server.jar --origin.events-file="$OUT/events.yaml" > "$OUT/edge.log" 2>&1 & pids+=($!)
for port in 27080 27081; do
  for i in $(seq 1 120); do curl -sf "localhost:$port/actuator/health" >/dev/null && break; sleep 0.5; done
done

"$JAVA" -jar packager/build/libs/packager.jar publish --epoch-ms "$EPOCH" --pipeline A --drop-rate 0.2 --duration-s 45 --stats-out "$OUT/packager-A.json" > "$OUT/packager-A.log" 2>&1 & pids+=($!)
"$JAVA" -jar packager/build/libs/packager.jar publish --epoch-ms "$EPOCH" --pipeline B --duration-s 45 --stats-out "$OUT/packager-B.json" > "$OUT/packager-B.log" 2>&1 & pids+=($!)
sleep 5
"$JAVA" -jar edge-sim/build/libs/edge-sim.jar --caches 5 --duration-s 30 --out "$OUT/fleet.json" > "$OUT/fleet.log" 2>&1

python3 - "$OUT/fleet.json" <<'PY'
import json, sys
f = json.load(open(sys.argv[1]))
c = f["counters"]
expected, delivered = f["expected_live_deliveries"], c.get("live.delivered", 0)
missing, corrupt = c.get("live.missing", 0), c.get("live.delivered_corrupt", 0)
print(f"expected={expected} delivered={delivered} missing={missing} corrupt={corrupt} failover={c.get('live.delivered.failover', 0)}")
if expected == 0 or delivered != expected or missing or corrupt:
    sys.exit("e2e FAILED")
print("e2e OK")
PY
