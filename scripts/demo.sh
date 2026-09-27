#!/usr/bin/env bash
# Live demo: two real-time ffmpeg pipelines publishing to the split origin, and a browser player.
#   scripts/demo.sh            then open http://localhost:27081/demo/index.html
# Options (environment): DROP_A=0.2 makes pipeline A drop 20% of its segments, so you can watch
# the origin serve B's copies with no gap in playback. Ctrl-C stops everything.
set -euo pipefail
cd "$(dirname "$0")/.."
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
OUT=run/demo
mkdir -p "$OUT"
[ -f edge-server/build/libs/edge-server.jar ] || ./gradlew -q build -x test

docker compose up -d redis-write redis-read >/dev/null
# A new run of the same event id must never serve the previous run's segments (bug 10).
for svc in redis-write redis-read; do docker compose exec -T "$svc" redis-cli FLUSHALL >/dev/null; done
NOW=$(( $(date +%s) * 1000 ))
EPOCH=$(( NOW - NOW % 2000 ))
sed "s/^    epoch: .*/    epoch: $EPOCH/; s/encodeDelayMs: 600 }$/encodeDelayMs: 1500 }/; s/{ id: B, encodeDelayMs: 1500 }/{ id: B, encodeDelayMs: 1700 }/" \
  config/events.yaml > "$OUT/events.yaml"

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

"$JAVA" -Xmx512m -jar publish-server/build/libs/publish-server.jar --origin.events-file="$OUT/events.yaml" > "$OUT/publish.log" 2>&1 & pids+=($!)
"$JAVA" -Xmx1g -jar edge-server/build/libs/edge-server.jar --origin.events-file="$OUT/events.yaml" > "$OUT/edge.log" 2>&1 & pids+=($!)
for port in 27080 27081; do
  for i in $(seq 1 120); do curl -sf "localhost:$port/actuator/health" >/dev/null && break; sleep 0.5; done
done
"$JAVA" -jar packager/build/libs/packager.jar live --epoch-ms "$EPOCH" --pipeline A --drop-rate "${DROP_A:-0}" > "$OUT/packager-A.log" 2>&1 & pids+=($!)
"$JAVA" -jar packager/build/libs/packager.jar live --epoch-ms "$EPOCH" --pipeline B > "$OUT/packager-B.log" 2>&1 & pids+=($!)
echo "Live. Player and origin counters: http://localhost:27081/demo/index.html"
echo "DASH manifest: http://localhost:27081/live/demo/manifest.mpd   HLS: http://localhost:27081/live/demo/master.m3u8"
wait
