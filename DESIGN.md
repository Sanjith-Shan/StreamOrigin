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

## What it is

(Written at M4 with measured figures; see `NUMBERS.md`.)
