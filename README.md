# StreamOrigin

StreamOrigin is a live video origin server in Java 21 and Spring WebFlux. It takes two-second
CMAF segments from two independent encoding pipelines over HTTP PUT and serves them to a fleet of
edge caches over HTTP GET. It picks the first valid copy across pipelines, holds a live-edge
request open until the segment lands instead of answering 404, caches misses until the segment
is due, rejects impossible requests before touching storage, keeps the publish path isolated from
read storms, and sheds replay traffic before live traffic under overload. **What it does not
claim:** everything runs on one laptop. The edge caches are simulated by a load generator in this
repo, the video is ffmpeg's synthetic test pattern, the "replicated" store is two Redis
instances, and there are no real viewers or CDN. The design follows a published description of a
production live origin, credited in [`DESIGN.md`](DESIGN.md); the point was to build it and
measure the trade-offs, not to reproduce anyone's scale.

<!-- RESULTS -->

## Run it

Needs JDK 21, Docker, ffmpeg and Python 3.

```bash
./gradlew build                      # unit, property and Testcontainers tests
scripts/e2e.sh                       # 30 s end to end: two pipelines, A drops 20%, no gaps allowed
scripts/demo.sh                      # live ffmpeg pipelines; open http://localhost:27081/demo/index.html
python3 bench/experiments.py exp2    # one experiment, 3 repeats, rows appended to results/exp2.jsonl
python3 bench/ledger.py              # regenerate NUMBERS.md from results/*.jsonl
docker compose --profile obs up -d   # Prometheus on :27090, Grafana dashboard on :27030
```

## What is where

| Path | What |
|---|---|
| `core/` | Everything both processes share: the event template and schedule (`schedule/`), control plane (`control/`), the chunked key-value store with Redis, RocksDB and in-memory backends (`store/`), the serve path: first-valid resolution, write-through cache, coalescing, live-edge holds, admission (`serve/`), the publish path (`publish/`), DASH and HLS manifests (`manifest/`) |
| `publish-server/` | Spring Boot app: packagers PUT here (port 27080) |
| `edge-server/` | Spring Boot app: edge caches GET here (port 27081); also runs the naive single-process origin; serves the demo page |
| `packager/` | Renders the test stream with ffmpeg and publishes it on schedule for one pipeline, with drop, corrupt, lag and fail injection; `live` mode runs ffmpeg in real time |
| `edge-sim/` | The simulated edge fleet: live caches that follow the template, DVR readers, and a junk-request generator |
| `bench/` | Experiment harness (`experiments.py`), ledger generator (`ledger.py`) |
| `results/` | Every measured run, one JSON row each, with machine and load |
| `DESIGN.md`, `NUMBERS.md`, `BUG_LOG.md` | Why it is built this way, every figure with its source, every bug with what found it |
