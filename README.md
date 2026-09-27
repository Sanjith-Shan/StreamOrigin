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

(Work in progress. Figures land in `NUMBERS.md` from `results/*.jsonl`.)
