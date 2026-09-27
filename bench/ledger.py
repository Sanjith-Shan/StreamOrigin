"""Builds NUMBERS.md from results/*.jsonl. Every figure is the median across repeats with the
range in brackets, and every table names the file its rows came from."""
import json
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results"


def rows(exp):
    p = RES / f"{exp}.jsonl"
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def group(rs, key=lambda r: r["label"]):
    g = defaultdict(list)
    for r in rs:
        g[key(r)].append(r)
    return g


def med(values, fmt="{:.0f}"):
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    m = statistics.median(values)
    if len(values) == 1 or min(values) == max(values):
        return fmt.format(m)
    return f"{fmt.format(m)} [{fmt.format(min(values))} to {fmt.format(max(values))}]"


def fc(r, k):
    return r["fleet"]["counters"].get(k, 0)


def fl(r, k, p):
    v = r["fleet"]["latency"].get(k, {}).get(p)
    return None if v is None else v / 1000  # microseconds to ms


def srv(r, role_pref=("publish", "combined")):
    for role in role_pref:
        if role in r["server"]:
            return r["server"][role]
    return {}


def edge(r):
    return srv(r, ("edge", "combined"))


def write_lat(r, p):
    v = srv(r).get("latency_us", {}).get("write", {}).get(p)
    return None if v is None else v / 1000


def table(headers, body):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in body]
    return "\n".join(out)


def load_note(rs):
    loads = [r["load_before"][0] for r in rs]
    return f"load average at start {min(loads):.1f} to {max(loads):.1f}" if loads else ""


def machine_line():
    for exp in ("exp1", "exp2", "exp3", "m1_playthrough"):
        rs = rows(exp)
        if rs:
            m = rs[0]["machine"]
            return (f"{m['host_model']}, {m['cpu']}, {m['cores']} cores, {m['ram_gb']} GB, {m['os']}, "
                    f"{m['java']}. {m['note']}.")
    return "n/a"


def sections():
    out = []

    rs = rows("m1_playthrough")
    if rs:
        r = rs[-1]
        out.append("## M1 play-through (`results/m1_playthrough.jsonl`)\n")
        out.append(f"{r['caches']} simulated caches, 3 renditions, 60 s, one pipeline: "
                   f"{fc(r, 'live.delivered')} of {r['fleet']['expected_live_deliveries']} deliveries, "
                   f"{fc(r, 'live.missing')} missing, {fc(r, 'live.status.404')} 404s.\n")

    rs = rows("exp1")
    if rs:
        g = group(rs, lambda r: (r["config"], r["caches"]))
        body = []
        for (cfg, n) in sorted(g, key=lambda k: (k[1], ["naive", "designed-shared", "designed"].index(k[0]))):
            x = g[(cfg, n)]
            publishes = [srv(r)["counters"].get("publish.ok", 0) for r in x]
            reads = [edge(r)["counters"].get("store.meta_reads", 0) + edge(r)["counters"].get("store.data_reads", 0) for r in x]
            body.append([cfg, n, med([write_lat(r, "p50") for r in x]), med([write_lat(r, "p99") for r in x]),
                         med([srv(r)["latency_us"].get("write", {}).get("max", 0) / 1000 for r in x]),
                         med([(r["packagers"].get("A") or {}).get("client_over_500ms", 0) + (r["packagers"].get("B") or {}).get("client_over_500ms", 0) for r in x]),
                         med([fc(r, "live.bytes") / r["measure_s"] / 1e6 for r in x], "{:.1f}"),
                         med([rd / max(1, p) for rd, p in zip(reads, publishes)], "{:.2f}"),
                         med([fc(r, "live.missing") for r in x]), med([fc(r, "live.delivered_corrupt") for r in x]),
                         med([fl(r, "live.publish_to_first_byte_us", "p99") for r in x])])
        out.append("## exp1 storm: write latency while N caches request the newest segment within 100 ms (`results/exp1.jsonl`)\n")
        out.append("naive: one process, one store connection, every serve feature off. designed-shared: every serve "
                   "feature on, but publish and serve share one process and one store. designed: separate "
                   "publish-server and edge-server processes on separate stores. Write latency is measured in the "
                   "publish handler from request start to acknowledgement. Two pipelines publish every segment of "
                   "3 renditions, so 3 PUTs per pipeline every 2 s.\n")
        out.append(table(["config", "caches", "write p50 ms", "write p99 ms", "write max ms", "writes over 500 ms (client)",
                          "served MB/s", "store reads per publish", "live missing", "corrupt delivered", "publish to first byte p99 ms"], body))
        out.append(f"\n{len(rs)} runs, 45 s each, {load_note(rs)}.\n")

    rs = rows("exp1_diag")
    if rs:
        out.append("### exp1 re-check of designed-shared at 100 caches (`results/exp1_diag.jsonl`)\n")
        out.append("Two of the three exp1 runs above reported corrupt deliveries (4 and 34) with no fault injected; see "
                   "`BUG_LOG.md` bug 6. Re-run with a fleet that separates truncated transfers from wrong bytes: "
                   + "; ".join(f"repeat {r['repeat']}: {fc(r, 'live.delivered')} delivered, {fc(r, 'live.truncated')} truncated, "
                               f"{fc(r, 'live.delivered_corrupt')} corrupt, write p99 {write_lat(r, 'p99'):.0f} ms" for r in rs) + ".\n")

    rs = rows("exp2")
    if rs:
        g = group(rs)
        body = []
        for lab in ("naive", "designed-no-hold", "designed"):
            if lab not in g:
                continue
            x = g[lab]
            body.append([lab, med([fc(r, "live.status.404") / (r["measure_s"] / 60) for r in x]),
                         med([fc(r, "live.requests") / max(1, fc(r, "live.delivered")) for r in x], "{:.2f}"),
                         med([fl(r, "live.publish_to_first_byte_us", "p50") for r in x]),
                         med([fl(r, "live.publish_to_first_byte_us", "p99") for r in x]),
                         med([fl(r, "live.available_to_delivered_us", "p50") for r in x]),
                         med([fl(r, "live.available_to_delivered_us", "p99") for r in x]),
                         med([edge(r)["counters"].get("hold.woken", 0) / max(1, edge(r)["counters"].get("hold.wakeups", 0)) for r in x], "{:.1f}"),
                         med([fc(r, "live.missing") for r in x])])
        out.append("## exp2 live edge: hold-open versus polling (`results/exp2.jsonl`)\n")
        out.append("50 caches, 3 renditions, both pipelines healthy, 60 s. A cache asks for segment k the moment its "
                   "media is complete, before any pipeline has published it. Without a hold it gets 404 and retries "
                   "after `max-age` if one is given, otherwise after 250 ms.\n")
        out.append(table(["config", "live-edge 404s per minute", "requests per delivered segment",
                          "publish to first byte p50 ms", "p99 ms", "request to delivery p50 ms", "p99 ms",
                          "held requests answered per publish", "missing"], body))
        out.append(f"\n{len(rs)} runs, {load_note(rs)}.\n")

    rs = rows("exp3")
    if rs:
        g = group(rs)
        order = ["single-drop10", "designed-drop10", "single-drop30", "designed-drop30", "naive-corrupt10",
                 "designed-corrupt10", "naive-silent10", "designed-silent10", "designed-dies-at-20s"]
        body = []
        for lab in order:
            if lab not in g:
                continue
            x = g[lab]
            a_bad = [((r["packagers"].get("A") or {}).get("dropped", 0) + (r["packagers"].get("A") or {}).get("corrupted_flagged", 0)
                      + (r["packagers"].get("A") or {}).get("corrupted_silent", 0)) for r in x]
            body.append([lab, med([r["fleet"]["expected_live_deliveries"] for r in x]), med(a_bad),
                         med([fc(r, "live.delivered.pipeline.B") for r in x]), med([fc(r, "live.missing") for r in x]),
                         med([fc(r, "live.delivered_corrupt") for r in x]),
                         med([fl(r, "live.available_to_delivered_us", "p50") for r in x]),
                         med([fl(r, "live.available_to_delivered_us", "p99") for r in x])])
        out.append("## exp3 failover: pipeline A drops or corrupts segments, B healthy (`results/exp3.jsonl`)\n")
        out.append("25 caches, 3 renditions, 60 s. `single` has only pipeline A. `naive` has both pipelines but serves "
                   "the first copy present without validating it. `corrupt` is flagged by the packager; `silent` is "
                   "garbage with no flag, caught (or not) by the size and box-header check. `dies-at-20s` stops "
                   "pipeline A entirely. \"A copies bad\" counts rendition-segments A dropped or corrupted.\n")
        out.append(table(["case", "deliveries expected", "A copies bad", "served from B", "missing at caches",
                          "corrupt delivered", "request to delivery p50 ms", "p99 ms"], body))
        out.append(f"\n{len(rs)} runs, {load_note(rs)}.\n")

    rs = rows("exp4")
    if rs:
        g = group(rs, lambda r: (r["config"], r["junk_rate"]))
        body = []
        for (cfg, m) in sorted(g, key=lambda k: (k[1], ["naive", "designed-no-cp", "designed"].index(k[0]))):
            x = g[(cfg, m)]
            junk = [max(1, fc(r, "junk.requests")) for r in x]
            body.append([cfg, m, med([fc(r, "junk.requests") / r["measure_s"] for r in x]),
                         med([edge(r)["counters"].get("cp.reject", 0) / j * 100 for r, j in zip(x, junk)], "{:.1f}"),
                         med([edge(r)["counters"].get("store.meta_reads", 0) / r["measure_s"] for r in x]),
                         med([edge(r)["cpu_ms"] / edge(r)["window_ms"] * 100 for r in x]),
                         med([fl(r, "junk.request_us", "p99") for r in x], "{:.1f}"),
                         med([fl(r, "live.available_to_delivered_us", "p99") for r in x]),
                         med([fc(r, "live.missing") for r in x])])
        out.append("## exp4 404 storm: requests for segments that cannot exist (`results/exp4.jsonl`)\n")
        out.append("M requests per second spread over four kinds (far future, unknown rendition, unknown event, "
                   "older than the DVR window), alongside 50 live caches, 45 s. Edge CPU is the edge process's CPU "
                   "time over wall time (100% is one core).\n")
        out.append(table(["config", "M (req/s asked)", "junk req/s achieved", "junk rejected at control plane %",
                          "store metadata reads per s", "edge CPU %", "junk p99 ms",
                          "live request to delivery p99 ms", "live missing"], body))
        out.append(f"\n{len(rs)} runs, {load_note(rs)}.\n")

    cap = RES / "exp5_capacity_value.json"
    rs = rows("exp5")
    if rs and cap.exists():
        c = json.loads(cap.read_text())
        g = group(rs)
        body = []
        for lab in sorted(g, key=lambda l: (l.split("-x")[1], "priority" in l and "no-" not in l)):
            x = g[lab]
            dvr_req = [max(1, fc(r, "dvr.requests")) for r in x]
            body.append([lab, x[0]["dvr_rate"], med([fl(r, "live.publish_to_first_byte_us", "p99") for r in x]),
                         med([fl(r, "live.available_to_delivered_us", "p99") for r in x]),
                         med([fc(r, "live.missing") for r in x]),
                         med([fc(r, "dvr.status.503") / d * 100 for r, d in zip(x, dvr_req)], "{:.1f}"),
                         med([fc(r, "dvr.status.200") / r["measure_s"] for r in x]),
                         med([fl(r, "dvr.request_us.200", "p99") for r in x])])
        out.append("## exp5 overload: live traffic versus replay traffic (`results/exp5.jsonl`)\n")
        out.append(f"Capacity: **{c['capacity_rps']} DVR requests/s**, the {c['rule']}; the sweep's highest point, so "
                   f"the true knee is at or above it (sweep in `{c['sweep']}`). The origin runs as one process on embedded "
                   "RocksDB with one Netty I/O thread on the serve path and the publish path on its own port and event "
                   "loops; 50 live caches on 3 renditions; DVR readers open-loop at 1.5x and 2x capacity; 30 s. With "
                   "priority on, the DVR token bucket is set to the capacity.\n")
        out.append(table(["config", "DVR req/s offered", "live publish to first byte p99 ms",
                          "live request to delivery p99 ms", "live missing", "DVR refused with 503 %",
                          "DVR served per s", "DVR p99 ms (served)"], body))
        out.append(f"\n{len(rs)} runs, {load_note(rs)}.\n")
        out.append("**Reading it.** At 1.5x the origin was not yet past its knee: every live segment arrived with priority "
                   "off or on, and live p99 is inside the run-to-run spread either way. At 2x it was: with priority off "
                   "the origin collapsed and most live deliveries were missed, while with priority on, replay was held to "
                   "the configured rate, half of it was refused with a 503 and `max-age=5`, and every live segment "
                   "arrived. The priority-off 2x runs were added after the rest (bug 8 had ruled them out until the publish "
                   "path got its own event loops), so they ran a few minutes later in the same session.\n")
        v1 = rows("exp5_v1")
        if v1:
            x = [r for r in v1 if r["label"] == "priority-x1.5"]
            y = [r for r in v1 if r["label"] == "no-priority-x1.5"]
            if x and y:
                out.append(f"The failure it could not prevent is in `results/exp5_v1.jsonl` (bug 9): with the publish path on "
                           f"the same event loops as the serve path, 2,400 DVR requests/s starved the writes. Live deliveries "
                           f"were {fc(y[0], 'live.delivered')} of {y[0]['fleet']['expected_live_deliveries']} with priority off "
                           f"and {fc(x[0], 'live.delivered')} of {x[0]['fleet']['expected_live_deliveries']} with it on, write "
                           f"p99 {write_lat(x[0], 'p99'):.0f} ms. Isolating the publish path is what fixed it.\n")

    rs = rows("exp6")
    if rs:
        g = group(rs)
        body = []
        for lab in ("shared-slow0", "shared-slow200", "isolated-slow0", "isolated-slow200"):
            if lab not in g:
                continue
            x = g[lab]
            body.append([lab, med([write_lat(r, "p50") for r in x]), med([write_lat(r, "p99") for r in x]),
                         med([(r["packagers"].get("A") or {}).get("client_over_500ms", 0) for r in x]),
                         med([srv(r)["latency_us"].get("replication", {}).get("p99", 0) / 1000 for r in x]),
                         med([fl(r, "dvr.request_us.200", "p99") for r in x]),
                         med([fl(r, "live.available_to_delivered_us", "p99") for r in x]),
                         med([fc(r, "live.missing") for r in x])])
        out.append("## exp6 isolation: the store serving reads is made slow (`results/exp6.jsonl`)\n")
        out.append("Toxiproxy adds 200 ms to every response from the store instance that serves reads. In `shared` that "
                   "store is also the write store; in `isolated` writes go to their own store and only replication "
                   "(after the acknowledgement) and reads cross the slow proxy. 50 caches plus 50 DVR requests/s, 45 s.\n")
        out.append(table(["config", "write p50 ms", "write p99 ms", "A writes over 500 ms",
                          "replication p99 ms", "DVR p99 ms", "live request to delivery p99 ms", "live missing"], body))
        out.append(f"\n{len(rs)} runs, {load_note(rs)}.\n")

    rs = rows("exp7")
    if rs:
        body = [[r["repeat"], r.get("side", {}).get("restart_s"), fc(r, "live.delivered"), r["fleet"]["expected_live_deliveries"],
                 fc(r, "live.missing"), fc(r, "live.errors"), round(fl(r, "live.available_to_delivered_us", "max") or 0)] for r in rs]
        out.append("## exp7 restart durability: edge-server killed with SIGKILL at 20 s (`results/exp7.jsonl`)\n")
        out.append("50 caches, 60 s. The new process starts with an empty cache and resumes from the read store. "
                   "Caches give up on a segment after 10 s.\n")
        out.append(table(["repeat", "kill to healthy s", "delivered", "expected", "missing", "connection errors seen by caches",
                          "worst request to delivery ms"], body))
        out.append("")

    rs = rows("exp8")
    if rs:
        body = [[r["repeat"], ", ".join(f"{f['fault']}@{f['t_s']:.0f}s" for f in r.get("side", {}).get("faults", [])),
                 fc(r, "live.delivered"), r["fleet"]["expected_live_deliveries"], fc(r, "live.missing"),
                 fc(r, "live.delivered_corrupt"), round(write_lat(r, "p99") or 0)] for r in rs]
        out.append("## exp8 chaos: seeded random faults for five minutes (`results/exp8.jsonl`)\n")
        out.append("50 caches plus 30 DVR requests/s. Faults: kill pipeline A (restart 10 to 20 s later), put pipeline B "
                   "3 s behind schedule, SIGKILL the edge-server, add 200 ms to the read store, make A corrupt half its "
                   "segments. Invariants: nothing missing while one pipeline is healthy, nothing corrupt delivered, "
                   "write p99 under 500 ms.\n")
        out.append(table(["repeat", "faults (time into run)", "delivered", "expected", "missing", "corrupt delivered",
                          "write p99 ms"], body))
        out.append("")
    rs = rows("m5_player")
    if rs:
        body = [[r["ts"][:16], r["played_s"], r["steady_samples"], r["drift_segments"]["median"], r["drift_segments"]["max"],
                 r["behind_wall_clock_s"]["median"], r["video"].get("decoded"), r["video"].get("dropped"),
                 "yes" if "ad-break" in (r["video"].get("events") or "") else "no"] for r in rs if r.get("drift_segments")]
        out.append("## M5 player: a real browser plays the live edge (`results/m5_player.jsonl`)\n")
        out.append("Headless Chrome with hls.js on the demo page, two real-time ffmpeg pipelines publishing to the split "
                   "origin (`scripts/demo.sh`, `bench/player_check.py`). Drift is the origin's live-edge segment minus the "
                   "segment on screen, sampled every 500 ms after the first 20 s.\n")
        out.append(table(["run", "played s", "samples", "drift median (segments)", "drift max", "behind wall clock s",
                          "frames decoded", "frames dropped", "ad-break header seen"], body))
        out.append("")
    return out


def main():
    head = ["# NUMBERS", "",
            "Every figure StreamOrigin reports, generated by `bench/ledger.py` from `results/*.jsonl`. Each cell is "
            "the median across repeats with the range in brackets. Latencies are in milliseconds.", "",
            f"Machine: {machine_line()}", ""]
    (ROOT / "NUMBERS.md").write_text("\n".join(head + sections()) + "\n")
    print((ROOT / "NUMBERS.md").read_text())


if __name__ == "__main__":
    main()
