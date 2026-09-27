"""StreamOrigin experiments. Every run appends one row to results/<exp>.jsonl.

    python3 bench/experiments.py exp2 --repeats 3
    python3 bench/experiments.py all --repeats 3

Each experiment compares the designed origin against a baseline on the same packagers and the
same fleet. Configurations are listed in CONFIGS below so the ledger names exactly which flags
were on.
"""
import argparse
import json
import sys
import threading
import time

import harness as h

# Origin configurations. "split" is two processes (publish-server + edge-server) on two Redis
# instances; "combined" is one process. Naive turns every serve-path feature off and puts both
# paths on one store connection.
DESIGNED = {"topology": "split", "props": {}}
NAIVE = {"topology": "combined", "props": {"naive": "true", "store.shared": "true"}}


def origin(base, **extra):
    props = dict(base["props"])
    props.update(extra)
    return {"topology": base["topology"], "props": props}


def one_run(exp, label, config, pipelines, packager_flags=None, fleet=None, measure_s=60, warmup_s=6,
            repeat=0, during=None, dvr=150, edge_heap="2g", jvm_opts=(), setup=None, extra=None, flush=True):
    """Runs one configuration and appends the ledger row. `during(run)` runs alongside the fleet."""
    packager_flags = packager_flags or {}
    fleet = dict(fleet or {})
    with h.run(f"{exp}-{label}-{repeat}", pipelines=pipelines, dvr=dvr, flush=flush) as r:
        if setup:
            setup()
        r.start_origin(config["topology"], config["props"], edge_heap=edge_heap, jvm_opts=jvm_opts)
        for p, delay in pipelines:
            r.start_packager(p, delay_ms=delay, duration_s=warmup_s + measure_s + 25, **packager_flags.get(p, {}))
        time.sleep(warmup_s)
        r.reset_stats()
        load0 = h.load()
        t = None
        side = {}
        if during:
            t = threading.Thread(target=lambda: side.update(during(r) or {}), daemon=True)
            t.start()
        fleet.setdefault("duration_s", measure_s)
        fleet.setdefault("label", f"{exp}/{label}")
        f = r.run_fleet(**fleet)
        if t:
            t.join(timeout=60)
        stats = r.server_stats()
        load1 = h.load()
        r.stop()
        pkgs = {p: r.packager_stats(p, wait_s=5) for p, _ in pipelines}
        row = {
            "exp": exp, "label": label, "repeat": repeat, "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "topology": config["topology"], "props": config["props"],
            "pipelines": [{"id": p, "encode_delay_ms": d, **packager_flags.get(p, {})} for p, d in pipelines],
            "measure_s": measure_s, "machine": h.machine(), "load_before": load0, "load_after": load1,
            "fleet": f, "server": stats, "packagers": pkgs,
        }
        if jvm_opts:
            row["jvm_opts"] = list(jvm_opts)
        if side:
            row["side"] = side
        if extra:
            row.update(extra)
        h.append(exp, row)
        return row


def brief(row):
    f = row["fleet"]["counters"]
    lat = row["fleet"]["latency"]
    srv = row["server"]
    pub = srv.get("publish", srv.get("combined", {}))
    w = pub.get("latency_us", {}).get("write", {})
    p2fb = lat.get("live.publish_to_first_byte_us", {})
    return (f"{row['exp']}/{row['label']}#{row['repeat']}: expected={row['fleet']['expected_live_deliveries']} "
            f"delivered={f.get('live.delivered', 0)} missing={f.get('live.missing', 0)} "
            f"404={f.get('live.status.404', 0)} 503={f.get('live.status.503', 0)} "
            f"corrupt={f.get('live.delivered_corrupt', 0)} failover={f.get('live.delivered.failover', 0)} "
            f"p2fb_p50={p2fb.get('p50', 0)//1000}ms p99={p2fb.get('p99', 0)//1000}ms "
            f"write_p50={w.get('p50', 0)//1000}ms p99={w.get('p99', 0)//1000}ms load={row['load_before'][0]:.1f}")


# Pipeline B runs in a second region and publishes 200 ms after A.
TWO = (("A", 600), ("B", 800))


def rocks(base_props, run_label):
    """Combined process on embedded RocksDB: no store traffic crosses the Docker network."""
    props = dict(base_props)
    props.update({"store.backend": "rocksdb", "store.rocksdb-path": f"run/rocksdb/{run_label}-{time.time_ns()}"})
    return {"topology": "combined", "props": props}
ONE = (("A", 600),)


# ---------------------------------------------------------------------------------------- exp2
def exp2(repeats, caches=50):
    """Live edge: hold-open versus polling. Both pipelines healthy."""
    fleet = {"caches": caches, "jitter_ms": 100, "poll_ms": 250}
    configs = [
        ("naive", NAIVE),
        ("designed", DESIGNED),
        ("designed-no-hold", origin(DESIGNED, **{"features.hold-open": "false"})),
    ]
    for rep in range(repeats):
        for label, cfg in configs:
            print(brief(one_run("exp2", label, cfg, TWO, fleet=fleet, repeat=rep)), flush=True)


# ---------------------------------------------------------------------------------------- exp3
def exp3(repeats, caches=25):
    """Failover: pipeline A drops or corrupts X% of segments while B stays healthy."""
    fleet = {"caches": caches, "jitter_ms": 100, "poll_ms": 250}
    cases = [
        # label, config, pipelines, packager flags for A
        ("single-drop10", DESIGNED, ONE, {"drop_rate": 0.10}),
        ("designed-drop10", DESIGNED, TWO, {"drop_rate": 0.10}),
        ("single-drop30", DESIGNED, ONE, {"drop_rate": 0.30}),
        ("designed-drop30", DESIGNED, TWO, {"drop_rate": 0.30}),
        ("naive-corrupt10", NAIVE, TWO, {"corrupt_rate": 0.10}),
        ("designed-corrupt10", DESIGNED, TWO, {"corrupt_rate": 0.10}),
        ("naive-silent10", NAIVE, TWO, {"silent_corrupt_rate": 0.10}),
        ("designed-silent10", DESIGNED, TWO, {"silent_corrupt_rate": 0.10}),
        ("designed-dies-at-20s", DESIGNED, TWO, {"fail_after_s": 26}),
    ]
    for rep in range(repeats):
        for label, cfg, pipes, flags in cases:
            print(brief(one_run("exp3", label, cfg, pipes, packager_flags={"A": dict(flags, seed=100 + rep)},
                                fleet=fleet, repeat=rep)), flush=True)


# ---------------------------------------------------------------------------------------- exp1
def exp1(repeats):
    """Origin storm: N caches all request the newest segment within 100 ms. Write latency with and
    without isolation of the publish path."""
    configs = [
        ("naive", NAIVE),
        ("designed-shared", {"topology": "combined", "props": {"store.shared": "true"}}),
        ("designed", DESIGNED),
    ]
    for rep in range(repeats):
        for n in (25, 50, 100):
            for label, cfg in configs:
                print(brief(one_run("exp1", f"{label}-n{n}", cfg, TWO, fleet={"caches": n, "jitter_ms": 100},
                                    measure_s=45, repeat=rep, extra={"caches": n, "config": label})), flush=True)


# ---------------------------------------------------------------------------------------- exp4
def exp4(repeats):
    """404 storm: requests for segments that cannot exist, alongside 50 live caches."""
    configs = [
        ("naive", NAIVE),
        ("designed-no-cp", origin(DESIGNED, **{"features.control-plane": "false"})),
        ("designed", DESIGNED),
    ]
    for rep in range(repeats):
        for rate in (1000, 3000):
            for label, cfg in configs:
                # A 10-segment DVR window, so "older than the window" requests exist in a young event.
                print(brief(one_run("exp4", f"{label}-m{rate}", cfg, TWO, dvr=10,
                                    fleet={"caches": 50, "junk_rate": rate}, measure_s=45, repeat=rep,
                                    extra={"junk_rate": rate, "config": label})), flush=True)


# ---------------------------------------------------------------------------------------- exp5
# The serve path gets one Netty I/O thread, so the origin saturates at a load the laptop can generate
# without the fleet and the host collapsing first (bugs 4 and 8). The publish path runs on its own
# port and event loops inside the same process, so this experiment measures serve-path priority,
# not write starvation.
EXP5_JVM = ("-Dreactor.netty.ioWorkerCount=1",)
EXP5_PUBLISH = {"publish-port": 27080}
EXP5_DVR_WINDOW = 40


def exp5_run(label, rate, priority, repeat, mode="gradient", measure_s=30, exp="exp5", dvr_rate=None):
    props = {"features.priority": priority, "admission.mode": mode, **EXP5_PUBLISH}
    if dvr_rate:
        # Priority's token bucket for replay traffic, set from the measured capacity.
        props["admission.dvr-rate"] = dvr_rate
    cfg = rocks(props, label)
    return one_run(exp, label, cfg, TWO, packager_flags={"A": {"backfill_segments": EXP5_DVR_WINDOW}},
                   fleet={"caches": 50, "dvr_rate": rate}, measure_s=measure_s, warmup_s=12, repeat=repeat,
                   dvr=EXP5_DVR_WINDOW, edge_heap="1g", jvm_opts=EXP5_JVM, flush=False,
                   extra={"dvr_rate": rate, "priority": priority, "admission_mode": mode, "dvr_bucket": dvr_rate})


def exp5_capacity(repeats=1, start_from=None):
    """Capacity: the highest DVR rate the origin serves with priority off while DVR p99 stays under
    500 ms and no live-edge delivery is missed."""
    capacity = 0
    rates = (100, 200, 300, 400, 500, 600, 800, 1000, 1200, 1400, 1600)
    if start_from:
        # Resume a sweep whose earlier points are already in the ledger.
        capacity = int(json.load(open(h.RESULTS / "exp5_capacity_value.json"))["capacity_rps"])
        rates = tuple(r for r in rates if r >= start_from)
    for rate in rates:
        row = exp5_run(f"cap-{rate}", rate, "false", 0, exp="exp5_capacity", measure_s=20)
        if any(v.get("unresponsive") for v in row["server"].values()):
            print(f"capacity sweep {rate}/s: origin stopped answering -> beyond capacity", flush=True)
            break
        L = row["fleet"]["latency"].get("dvr.request_us", {})
        c = row["fleet"]["counters"]
        requested = max(1, c.get("dvr.requests", 0))
        ok = (L.get("p99", 10**9) < 500_000 and c.get("live.missing", 0) == 0
              and c.get("dvr.status.200", 0) >= 0.99 * requested)
        print(f"capacity sweep {rate}/s: dvr ok={c.get('dvr.status.200', 0)}/{requested} p99={L.get('p99', 0)//1000}ms "
              f"live missing={c.get('live.missing', 0)} -> {'within' if ok else 'beyond'} capacity", flush=True)
        if not ok:
            break
        capacity = rate
    (h.RESULTS / "exp5_capacity_value.json").write_text(json.dumps({
        "capacity_rps": capacity,
        "rule": "highest swept DVR rate with priority off where DVR p99 < 500 ms, >= 99% of DVR requests "
                "answered 200, and zero live-edge deliveries missed",
        "sweep": "results/exp5_capacity.jsonl", "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z")}, indent=2))
    print("capacity", capacity, flush=True)


def exp5(repeats, capacity=None):
    """Overload at 1.5x and 2x measured capacity, with and without priority."""
    capacity = capacity or int(json.load(open(h.RESULTS / "exp5_capacity_value.json"))["capacity_rps"])
    # Priority off already collapses the origin at 1.5x, and a collapsed origin plus the fleet starve
    # the whole laptop (Docker Desktop hung twice). So priority off runs at 1.5x only; 2x is priority on.
    cases = [(1.5, "no-priority", "false"), (1.5, "priority", "true"), (2.0, "priority", "true")]
    for rep in range(repeats):
        for mult, label, prio in cases:
            print(brief(exp5_run(f"{label}-x{mult}", int(capacity * mult), prio, rep,
                                 dvr_rate=capacity if prio == "true" else None)), flush=True)


def exp5_np2(repeats):
    """Priority off at 2x, added once separate publish loops stopped the collapse that bug 8 guarded against."""
    capacity = int(json.load(open(h.RESULTS / "exp5_capacity_value.json"))["capacity_rps"])
    for rep in range(repeats):
        print(brief(exp5_run("no-priority-x2.0", int(capacity * 2.0), "false", rep)), flush=True)


def _have(exp, label, repeat):
    p = h.RESULTS / f"{exp}.jsonl"
    return p.exists() and any(json.loads(l)["label"] == label and json.loads(l)["repeat"] == repeat
                              for l in p.read_text().splitlines() if l.strip())


# ---------------------------------------------------------------------------------------- exp6
def exp6(repeats, slow_ms=200):
    """Write p99 while the store serving reads is made slow, with and without separate stores and
    processes. Latency is injected with Toxiproxy on the store instance, not in the code path."""
    proxied_read = "redis://localhost:27390"
    proxied_write = "redis://localhost:27391"
    configs = [
        # One process, one store: the slow store is also the one writes go to.
        ("shared", {"topology": "combined", "props": {"store.shared": "true", "store.write-uri": proxied_write}}),
        # Two processes, two stores: only the read store is slow.
        ("isolated", {"topology": "split", "props": {"store.read-uri": proxied_read}}),
    ]
    for rep in range(repeats):
        for label, cfg in configs:
            for slow in (0, slow_ms):
                def setup(slow=slow, label=label):
                    h.toxiproxy_reset()
                    if slow:
                        h.toxiproxy_latency("write" if label == "shared" else "read", slow)
                row = one_run("exp6", f"{label}-slow{slow}", cfg, TWO, packager_flags={"A": {"backfill_segments": 30}},
                              fleet={"caches": 50, "dvr_rate": 50}, measure_s=45, warmup_s=10, repeat=rep, dvr=40,
                              setup=setup, extra={"config": label, "read_store_latency_ms": slow})
                print(brief(row), flush=True)
    h.toxiproxy_reset()


# ---------------------------------------------------------------------------------------- exp7
def exp7(repeats):
    """M6 restart durability: the edge-server is killed with SIGKILL mid-stream and restarted with
    an empty cache; it must resume from the read store."""
    def kill_and_restart(r):
        time.sleep(20)
        killed, healthy = r.restart_edge()
        return {"killed_at": killed, "healthy_at": healthy, "restart_s": round(healthy - killed, 2)}
    for rep in range(repeats):
        row = one_run("exp7", "edge-restart", DESIGNED, TWO, fleet={"caches": 50, "give_up_ms": 10000},
                      measure_s=60, repeat=rep, during=kill_and_restart)
        print(brief(row), row.get("side"), flush=True)


# ---------------------------------------------------------------------------------------- exp8
def exp8(repeats, minutes=5):
    """Chaos: a seeded random schedule of faults for several minutes while 50 caches watch the live
    edge. Invariants: no live segment missed while at least one pipeline is healthy, nothing
    corrupt delivered, publish writes under 500 ms at p99."""
    import random
    measure_s = minutes * 60

    def chaos(r, seed):
        rnd = random.Random(seed)
        log = []
        t0 = time.time()
        faults = ["kill_a", "lag_b", "kill_edge", "slow_read", "corrupt_a"]
        while time.time() - t0 < measure_s - 40:
            time.sleep(rnd.uniform(15, 30))
            fault = rnd.choice(faults)
            at = round(time.time() - t0, 1)
            if fault == "kill_a":
                r.kill("packager-A")
                time.sleep(rnd.uniform(10, 20))
                r.start_packager("A", delay_ms=600, duration_s=measure_s, seed=seed)
            elif fault == "lag_b":
                # B falls 3 s behind its schedule for a while, then recovers.
                r.kill("packager-B")
                r.start_packager("B", delay_ms=800, duration_s=measure_s, lag_ms=3000)
                time.sleep(rnd.uniform(10, 20))
                r.kill("packager-B")
                r.start_packager("B", delay_ms=800, duration_s=measure_s)
            elif fault == "kill_edge":
                r.restart_edge()
            elif fault == "slow_read":
                h.toxiproxy_latency("read", 200)
                time.sleep(rnd.uniform(10, 20))
                h.toxiproxy_reset()
            elif fault == "corrupt_a":
                r.kill("packager-A")
                r.start_packager("A", delay_ms=600, duration_s=measure_s, corrupt_rate=0.5, silent_corrupt_rate=0.2, seed=seed)
                time.sleep(rnd.uniform(10, 20))
                r.kill("packager-A")
                r.start_packager("A", delay_ms=600, duration_s=measure_s, seed=seed)
            log.append({"t_s": at, "fault": fault, "done_s": round(time.time() - t0, 1)})
        return {"faults": log, "seed": seed}

    for rep in range(repeats):
        seed = 7 + rep
        row = one_run("exp8", f"chaos-{minutes}min", origin(DESIGNED, **{"store.read-uri": "redis://localhost:27390"}),
                      TWO, fleet={"caches": 50, "give_up_ms": 10000, "dvr_rate": 30},
                      packager_flags={"A": {"backfill_segments": 30}}, measure_s=measure_s, warmup_s=10,
                      repeat=rep, dvr=40, setup=h.toxiproxy_reset, during=lambda r, s=seed: chaos(r, s))
        print(brief(row), [f["fault"] for f in row.get("side", {}).get("faults", [])], flush=True)
    h.toxiproxy_reset()


def diag_shared100(repeats):
    """Re-runs the one configuration that showed corrupt deliveries, with the fleet that separates
    truncated transfers from wrong bytes."""
    cfg = {"topology": "combined", "props": {"store.shared": "true"}}
    for rep in range(repeats):
        print(brief(one_run("exp1_diag", "designed-shared-n100", cfg, TWO, fleet={"caches": 100, "jitter_ms": 100},
                            measure_s=45, repeat=rep, extra={"caches": 100, "config": "designed-shared"})), flush=True)


def exp2_redo(repeats):
    """Replaces exp2/designed repeat 1, which overlapped another project's load test."""
    print(brief(one_run("exp2", "designed", DESIGNED, TWO, fleet={"caches": 50, "jitter_ms": 100, "poll_ms": 250},
                        repeat=1)), flush=True)


EXPERIMENTS = {"diag_shared100": diag_shared100, "exp2_redo": lambda r: exp2_redo(r),"exp1": exp1, "exp2": exp2, "exp3": exp3, "exp4": exp4, "exp5_capacity": lambda r: exp5_capacity(),
               "exp5_capacity_ext": lambda r: exp5_capacity(start_from=1200),
               "exp5": exp5, "exp5_np2": exp5_np2, "exp6": exp6, "exp7": exp7,
               "exp8": exp8}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("exp")
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    names = list(EXPERIMENTS) if a.exp == "all" else a.exp.split(",")
    for n in names:
        with h.bench_lock():
            EXPERIMENTS[n](a.repeats)
