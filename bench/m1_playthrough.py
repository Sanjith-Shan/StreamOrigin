"""M1 done-when: simulated caches play through 60 s of segments with no gaps (one pipeline)."""
import sys
import time

import harness as h

caches = int(sys.argv[1]) if len(sys.argv) > 1 else 10
with h.run("m1", pipelines=(("A", 600),)) as r:
    r.start_origin("split")
    r.start_packager("A", duration_s=80)
    time.sleep(6)
    r.reset_stats()
    load0 = h.load()
    fleet = r.run_fleet(caches=caches, duration_s=60, jitter_ms=100)
    stats = r.server_stats()
    pkg = r.packager_stats("A", wait_s=40)
    row = {"exp": "m1_playthrough", "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "machine": h.machine(),
           "load_before": load0, "load_after": h.load(), "caches": caches, "fleet": fleet,
           "server": stats, "packager": pkg}
    h.append("m1_playthrough", row)
    c = fleet["counters"]
    print("expected", fleet["expected_live_deliveries"], "delivered", c.get("live.delivered"),
          "missing", c.get("live.missing", 0), "404", c.get("live.status.404", 0),
          "corrupt", c.get("live.delivered_corrupt", 0))
    print("publish->first byte us", fleet["latency"].get("live.publish_to_first_byte_us"))
    print("server write us", stats["publish"]["latency_us"].get("write"))
