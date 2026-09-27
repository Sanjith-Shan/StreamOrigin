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
            repeat=0, during=None, dvr=150, edge_heap="2g"):
    """Runs one configuration and appends the ledger row. `during(run)` runs alongside the fleet."""
    packager_flags = packager_flags or {}
    fleet = dict(fleet or {})
    with h.run(f"{exp}-{label}-{repeat}", pipelines=pipelines, dvr=dvr) as r:
        r.start_origin(config["topology"], config["props"], edge_heap=edge_heap)
        for p, delay in pipelines:
            r.start_packager(p, delay_ms=delay, duration_s=warmup_s + measure_s + 25, **packager_flags.get(p, {}))
        time.sleep(warmup_s)
        r.reset_stats()
        load0 = h.load()
        t = None
        if during:
            t = threading.Thread(target=during, args=(r,), daemon=True)
            t.start()
        fleet.setdefault("duration_s", measure_s)
        fleet.setdefault("label", f"{exp}/{label}")
        f = r.run_fleet(**fleet)
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


EXPERIMENTS = {"exp2": exp2, "exp3": exp3}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("exp")
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    names = list(EXPERIMENTS) if a.exp == "all" else a.exp.split(",")
    for n in names:
        EXPERIMENTS[n](a.repeats)
