# StreamOrigin design

## Sources, credited first

StreamOrigin was built to understand and measure trade-offs that other people designed and
published. It is not a clone of any company's system, and nothing in it is named after one.

- **Netflix Technology Blog, "Netflix Live Origin"** (Xiaomei Liu, Joseph Lynch, Chris Newton,
  December 2025), read through its two most complete secondary accounts: ByteByteGo, "How Netflix
  Live Streams to 100 Million Devices in 60 Seconds" (2026-03-24), and InfoQ, "From On-Demand to
  Live" (2025-12-05). Every serve-path idea here comes from that post: two independent pipelines
  with first-valid selection, a fixed segment template that lets the origin predict when a segment
  should exist, holding a live-edge request open instead of answering 404, caching misses until
  just before the expected publish time, a control plane that rejects impossible requests before
  storage, a chunked key-value abstraction in place of object storage, write-through caching on the
  publish path, separate stacks and stores for publishing and serving, and priority rate limiting
  of live traffic over DVR with a 503 and `max-age` under stress.
- **Netflix Technology Blog, "Behind the Streams" Parts 1 and 2** (2025, 2025-09-15), via the
  Tokyo Video Tech report on dev.to and the Streaming Learning Center write-up: pipeline
  redundancy, the shared epoch, and why a template beats a manifest that changes every segment.
- **Netflix, `concurrency-limits`** (open source, Java): the gradient algorithm behind
  `GradientLimit` (their `Gradient2Limit`). Reimplemented here in about 80 lines, not imported.
- **DASH-IF Interoperability Points**, `SegmentTemplate` with `$Number$` and
  `availabilityStartTime`; **RFC 8216** (HLS) for playlist semantics.
- **ISO/IEC 14496-12 (ISO BMFF) and CMAF** basics for `init.mp4` and `.m4s` fragments. Nothing
  here parses media beyond the first box header.
- **nginx** `proxy_cache_lock` and `proxy_cache_use_stale`: request coalescing at a cache tier.
- **Martin Kleppmann, *Designing Data-Intensive Applications***, chapter 5 (replication,
  read-your-writes) and chapter 11 (write-through caches and derived data).

## The system in one picture

```
  packager A (ffmpeg, region 1) ──PUT──┐                         ┌──GET── edge cache 1
  packager B (ffmpeg, region 2) ──PUT──┤                         ├──GET── edge cache 2
                                       ▼                         │        ...
                              ┌─────────────────┐   notify +     ┌┴────────────────┐
                              │ publish-server  │── bytes ──────▶│  edge-server    │◀─GET── edge cache N
                              │ :27080          │ (write-through)│  :27081         │◀─GET── DVR readers
                              └──┬───────────┬──┘                └──┬──────────────┘
                     ack after   │           │ replicate            │ reads (misses,
                     this write  ▼           ▼ after the ack        ▼ DVR, restarts)
                          ┌────────────┐  ┌────────────┐
                          │ redis-write│  │ redis-read │◀─────────────┘
                          └────────────┘  └────────────┘
```

Two processes, two stores. The publish path acknowledges a segment once it is in the write
store; replication to the read store and the push to the serve side happen after the
acknowledgement, so nothing the serve side does can slow an encoder's write. In naive mode the
same code runs as one process with one store connection and every serve-path feature off.

## The stream model

An event has renditions (1080p at 4.5 Mbps, 720p at 2.5 Mbps, 480p at 1 Mbps), a shared epoch,
a fixed segment duration of two seconds, and two pipelines listed in preference order. Segment
`k` covers media time `[2k, 2k+2)` seconds after the epoch, so its media is complete at
`epoch + 2(k+1)` and a pipeline with encode delay `e` should publish it at `epoch + 2(k+1) + e`.
Pipeline A's delay is 600 ms and B's is 800 ms (B stands for a second region).

Because of that arithmetic the origin never needs a manifest that changes. The DASH manifest is
generated once from a `SegmentTemplate` with `startNumber="0"` and `availabilityStartTime` set
to the epoch; a player computes segment numbers from the clock. HLS has no equivalent, so its
media playlists are computed from the clock on every request, still without reading storage.
URLs follow the template: `/live/{event}/{rendition}/{k}.m4s` and `.../init.mp4`.

## The serve path, in order, and what each step protects against

1. **Control plane.** Events, renditions and the schedule live in memory (loaded from YAML and
   refreshed on a timer). A request for an unknown event or rendition, a segment older than the
   DVR window, or one more than two segments past the live edge is answered 404 from memory.
   Protects storage from 404 storms: misbehaving clients, bots, stale players.
2. **Class and admission.** A request within three segments of the live edge is `LIVE_EDGE`;
   anything older is `DVR`. Each class has a token bucket. DVR requests are admitted only while
   the in-flight count is under half of an adaptive concurrency limit; live requests up to the
   larger of that limit and a fixed floor. A refused request gets `503` with
   `Cache-Control: max-age=5`, so a cache backs off for five seconds instead of retrying.
   Protects live viewers from replay viewers when the origin is short of capacity.
3. **Join an open hold.** If other requests are already waiting for this segment, join them
   without touching storage.
4. **First valid copy.** The write-through cache first, then one store read. Concurrent misses
   for the same segment are coalesced into one read (the `proxy_cache_lock` idea). Pipelines are
   tried in order and the first copy that passes validation wins. Protects against a failing
   pipeline and against a storm of identical reads.
5. **Hold at the live edge.** If the segment is due within two seconds, the request is held
   until it is published or until 1.5 s past its expected time. One publish answers every held
   request. A sweeper re-reads the store for overdue holds in case a notification was lost.
   Protects against the 404-then-poll loop at the live edge.
6. **Negative caching.** A 404 for a segment that is expected later carries `max-age` equal to
   the whole seconds until it is due, so a cache does not ask again before it could exist.

The publish path in order: validate against the control plane, read the body, chunk it into
1 MB pieces in the write store (chunks first, the metadata record last, so a visible record
implies complete chunks), acknowledge, and then replicate to the read store and push the bytes to
every edge-server.

## Decisions and their alternatives

- **WebFlux on Netty instead of servlet threads.** Holding thousands of requests open at the live
  edge is the normal case, not the exception. A held request here is a subscription on a
  `Sinks.One`, not a blocked thread.
- **A chunked key-value abstraction instead of object storage.** Segments are 250 KB to 1.1 MB.
  Chunking at 1 MB keeps each value small enough for any KV store; the store is an interface
  (`KvBackend`) with Redis, RocksDB and in-memory implementations.
- **Two Redis instances instead of Cassandra.** Cassandra's footprint does not fit next to
  everything else on an 18 GB laptop. The write path writes to both instances, which simulates
  replication and keeps the read store separate. What that leaves unmeasured: quorum writes,
  cross-zone replication lag, and read repair.
- **First valid, not first arrived, on the store path; first arrived on the write-through path.**
  The pipelines share an epoch and produce interchangeable segments, so waiting for the preferred
  pipeline when the other one's valid copy is already here would only add latency.
- **Validation is the packager's own flag plus a size and box-header check.** No media parsing.
  A segment whose bytes are garbage but whose first box looks right would be served.
- **Adaptive limit instead of a hand-tuned capacity.** The gradient algorithm compares a
  short-window service time with a long-term baseline and shrinks the limit when queueing
  starts. Only DVR follows it; live traffic has a floor (see `BUG_LOG.md`, bug 3, for why).
- **DVR reads do not fill the cache.** The cache holds the live edge. An earlier version cached
  every store read, and DVR traffic both hid the store's real cost and could evict the live edge.

## What is not measured, and why

- Everything shares one laptop: origin, fleet, packagers, and Redis inside Docker Desktop. Fleet
  latency includes the fleet's own scheduling on the same cores, and Redis traffic crosses Docker
  Desktop's network stack. A second project was benchmarking on the same machine the same night;
  both used a lock file so their runs never overlapped, and every ledger row records load.
- No real CDN, no real players at scale, no Cassandra, no cross-region network.
- The overload experiment runs the origin on the embedded RocksDB store in one process, so no
  store traffic crosses Docker (bug 4). Its capacity figure is the highest rate the sweep tried
  that the origin passed, 1,600 DVR requests per second; the sweep stopped there, so the true
  knee is at or above it. Priority off was run at 1.5x only: it already collapses the origin
  there, and a collapsed origin plus the fleet starved the whole laptop (bug 8).
