# Bug log

Every bug found while building StreamOrigin, with the tool or check that found it. Newest last.

| # | Date | Component | Symptom | Found by | Cause | Fix |
|---|---|---|---|---|---|---|
| 1 | 2026-09-27 | packager, edge-sim | Every packager PUT failed with HTTP 501 while `curl` to the same URL got 201 | Packager's first-error line, then `lsof -iTCP:8080` | An unrelated local Python server held IPv4 `*:8080`; the JDK client resolved `localhost` to 127.0.0.1 and reached it, while curl reached the Java server on IPv6 | All StreamOrigin ports moved to the 27xxx block |
| 2 | 2026-09-27 | edge-sim | M1 play-through lost exactly one segment (k=44) on all 30 cache-renditions, with 718 bare `IOException`s | Fleet's `missing_segments` report plus the printed cause chain: `RejectedExecutionException` | The fleet shut down its virtual-thread executor to wait for in-flight fetches, but the `HttpClient` ran on that same executor, so every request still open for the last segment was rejected | `HttpClient` gets its own executor that is never shut down (same fix applied to the packager) |
