# Capacity note

Derived from the measurements in [`NUMBERS.md`](../NUMBERS.md). Everything here was measured on
one laptop over loopback, with the load generator sharing the cores. Read it as a statement about
where the bottlenecks sit and in what ratio, not as production sizing.

## What one edge-server process served

| Measured | Figure | Source |
|---|---|---|
| Replay reads with one Netty I/O thread, before any live segment was missed | at least 1,600 DVR requests/s (the sweep's top; the knee is above it) | exp5 capacity sweep |
| Mean replay response size across the three renditions | about 0.66 MB (1.12, 0.62, 0.25 MB) | `data/segments/sizes.json` |
| So replay bytes per I/O thread | at least about 1 GB/s | the two rows above |
| Same process at 2x (3,200/s) with priority off | collapsed: a median 1,523 of 2,250 live deliveries missed | exp5 |
| Same process at 2x with priority on | 0 missed; replay held at 1,600/s, half refused with 503 | exp5 |
| Live storm, 100 caches x 3 renditions every 2 s | 0 missed, 0.5 store reads per published segment, write p99 45 ms | exp1 |

## What the publish path needs

Two pipelines times three renditions is six PUTs every two seconds, about 2 MB each way. At 100
caches of storm the isolated write p99 was 45 ms, against a 500 ms budget, so the publish path's
headroom is large and its risk is not volume but interference: in one process with shared event
loops, a replay flood took it from tens of milliseconds to 5.6 s (bug 9), and a slow shared store
took it from 62 to 466 ms (exp6).

## What that implies, with the caveats attached

- **The live edge is nearly free per cache.** Each held request costs a subscription, not a thread,
  and one publish answers every cache waiting for that segment from memory. In exp1, 300 responses
  per segment cost about half a store read. The live-edge cost scales with bytes out, not requests.
- **Replay is what needs capacity planning.** DVR reads miss the live-edge cache by design and go
  to the read store. They are also what overloads the process first, which is why they are the
  class that is shed.
- **Bytes out per process is the unit.** At roughly 1 GB/s of responses per I/O thread, and an 8 Mb/s
  ladder (all three renditions), one I/O thread could in principle feed on the order of a thousand
  caches pulling every rendition of the live edge. That number assumes loopback, ignores TLS, NIC
  limits and the cache tier's own behaviour, and has not been measured. It is here to show how
  one would size it, not what the answer is.
- **Isolation is not optional at any size.** Every write-latency failure we saw came from sharing
  something with the serve path (an event loop, a store, a store connection), never from write
  volume.
