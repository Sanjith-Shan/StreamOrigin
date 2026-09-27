"""Process orchestration for StreamOrigin experiments.

Starts the origin (split: publish-server + edge-server, or combined: one process), one packager
per pipeline, and the edge fleet; resets server counters when the measurement window opens;
collects every process's figures; and appends one JSON row per run to results/<exp>.jsonl with
the machine and the load it ran under.
"""
import json
import os
import platform
import signal
import subprocess
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JAVA = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@21") + "/bin/java"
RUN = ROOT / "run"
RESULTS = ROOT / "results"
PUBLISH_PORT, EDGE_PORT = 27080, 27081
PUBLISH = f"http://localhost:{PUBLISH_PORT}"
EDGE = f"http://localhost:{EDGE_PORT}"
REDIS_WRITE, REDIS_READ = "redis://localhost:27379", "redis://localhost:27380"
TOXI_API = "http://localhost:27474"
_BUILT = {
    "publish": ROOT / "publish-server/build/libs/publish-server.jar",
    "edge": ROOT / "edge-server/build/libs/edge-server.jar",
    "packager": ROOT / "packager/build/libs/packager.jar",
    "fleet": ROOT / "edge-sim/build/libs/edge-sim.jar",
}


def _snapshot_jars():
    """Copies the built jars once per harness process, so a rebuild mid-experiment cannot swap binaries."""
    import shutil
    dest = RUN / "jars" / str(os.getpid())
    dest.mkdir(parents=True, exist_ok=True)
    out = {}
    for k, src in _BUILT.items():
        shutil.copy2(src, dest / src.name)
        out[k] = dest / src.name
    return out


JARS = _snapshot_jars()

EVENT_TEMPLATE = """events:
  - id: demo
    epoch: {epoch}
    segmentDurationMs: 2000
    dvrWindowSegments: {dvr}
    renditions:
      - {{ id: 1080p, bandwidth: 4500000, width: 1920, height: 1080 }}
      - {{ id: 720p,  bandwidth: 2500000, width: 1280, height: 720 }}
      - {{ id: 480p,  bandwidth: 1000000, width: 854,  height: 480 }}
    pipelines:
{pipelines}
"""


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def machine():
    return {
        "host_model": sh("sysctl -n hw.model"),
        "cpu": sh("sysctl -n machdep.cpu.brand_string"),
        "cores": int(sh("sysctl -n hw.ncpu") or 0),
        "ram_gb": round(int(sh("sysctl -n hw.memsize") or 0) / 2**30),
        "os": f"macOS {platform.mac_ver()[0]}",
        "java": sh(f"{JAVA} -version 2>&1 | head -1"),
        "power": sh("pmset -g batt | head -1").replace("Now drawing from ", ""),
        "low_power_mode": sh("pmset -g | awk '/lowpowermode/ {print $2}'"),
        "git": sh(f"git -C {ROOT} rev-parse --short HEAD 2>/dev/null") or "uncommitted",
        "note": "origin, packagers, fleet and Redis share one laptop",
    }


def load():
    return [float(x) for x in os.getloadavg()]


def http_json(url, method="GET", body=None, timeout=10):
    req = urllib.request.Request(url, method=method, data=body)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        return json.loads(data) if data else None


def wait_healthy(url, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            urllib.request.urlopen(url + "/actuator/health", timeout=2)
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{url} did not become healthy")


def flush_redis():
    for svc in ("redis-write", "redis-read"):
        subprocess.run(["docker", "compose", "exec", "-T", svc, "redis-cli", "FLUSHALL"], cwd=ROOT,
                       capture_output=True, check=True)


def toxiproxy_reset():
    """Proxies in front of redis-read (27390) and redis-write (27391), no toxics."""
    try:
        http_json(TOXI_API + "/reset", "POST", b"{}")
    except Exception:
        pass
    for name, port, upstream in (("read", 27390, "redis-read:6379"), ("write", 27391, "redis-write:6379")):
        try:
            http_json(f"{TOXI_API}/proxies/{name}", "DELETE")
        except Exception:
            pass
        http_json(TOXI_API + "/proxies", "POST", json.dumps(
            {"name": name, "listen": f"0.0.0.0:{port}", "upstream": upstream}).encode())


def toxiproxy_latency(proxy, ms):
    http_json(f"{TOXI_API}/proxies/{proxy}/toxics", "POST", json.dumps(
        {"name": "lat", "type": "latency", "stream": "downstream", "attributes": {"latency": ms, "jitter": 0}}).encode())


class Run:
    """One experiment run: processes, epoch and files. Use as a context manager."""

    def __init__(self, name, pipelines=(("A", 600), ("B", 600)), dvr=150, epoch_lead_s=0):
        self.name = name
        self.procs = []
        self.dir = RUN / name
        self.dir.mkdir(parents=True, exist_ok=True)
        # The event starts at a segment boundary a little in the past.
        now = int(time.time() * 1000)
        self.epoch = now - (now % 2000) - 20_000 + epoch_lead_s * 1000
        self.events = self.dir / "events.yaml"
        pl = "\n".join(f"      - {{ id: {p}, encodeDelayMs: {d} }}" for p, d in pipelines)
        self.events.write_text(EVENT_TEMPLATE.format(epoch=self.epoch, dvr=dvr, pipelines=pl))

    def _spawn(self, tag, args):
        log = open(self.dir / f"{tag}.log", "w")
        p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT, start_new_session=True)
        self.procs.append((tag, p))
        return p

    def start_origin(self, topology="split", props=None, edge_heap="2g", jvm_opts=()):
        props = dict(props or {})
        self.jvm_opts = list(jvm_opts)
        common = [f"--origin.events-file={self.events}", "--logging.level.root=WARN",
                  "--logging.level.io.streamorigin=INFO"]
        extra = [f"--origin.{k}={v}" for k, v in props.items()]
        if topology == "combined":
            self._spawn("origin", [JAVA, f"-Xmx{edge_heap}", *self.jvm_opts, "-jar", str(JARS["edge"]), "--origin.role=combined",
                                   f"--server.port={EDGE_PORT}"] + common + extra)
            wait_healthy(EDGE)
            self.publish_url = EDGE
        else:
            self._spawn("publish", [JAVA, "-Xmx1g", "-jar", str(JARS["publish"])] + common + extra)
            self._spawn("edge", [JAVA, f"-Xmx{edge_heap}", *self.jvm_opts, "-jar", str(JARS["edge"])] + common + extra)
            wait_healthy(PUBLISH)
            wait_healthy(EDGE)
            self.publish_url = PUBLISH
        self.topology = topology

    def start_packager(self, pipeline, delay_ms=600, duration_s=60, **flags):
        args = [JAVA, "-Xmx512m", "-jar", str(JARS["packager"]), "publish", "--origin", self.publish_url,
                "--pipeline", pipeline, "--epoch-ms", str(self.epoch), "--encode-delay-ms", str(delay_ms),
                "--duration-s", str(duration_s), "--stats-out", str(self.dir / f"packager-{pipeline}.json")]
        for k, v in flags.items():
            args += ["--" + k.replace("_", "-"), str(v)]
        return self._spawn(f"packager-{pipeline}", args)

    def restart_edge(self, props=None, edge_heap="2g"):
        """Kills the edge-server outright and starts a fresh one (M6 restart durability)."""
        for tag, p in self.procs:
            if tag in ("edge", "edge-restarted") and p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
        killed_at = time.time()
        props = dict(props or {})
        common = [f"--origin.events-file={self.events}", "--logging.level.root=WARN",
                  "--logging.level.io.streamorigin=INFO"]
        extra = [f"--origin.{k}={v}" for k, v in props.items()]
        self._spawn("edge-restarted", [JAVA, f"-Xmx{edge_heap}", "-jar", str(JARS["edge"])] + common + extra)
        wait_healthy(EDGE)
        return killed_at, time.time()

    def kill(self, tag):
        """SIGKILLs every live process with this tag."""
        for t, p in self.procs:
            if t == tag and p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()

    def reset_stats(self):
        for url in self.server_urls():
            http_json(url + "/internal/stats/reset", "POST", b"")

    def server_urls(self):
        return [EDGE] if self.topology == "combined" else [PUBLISH, EDGE]

    def server_stats(self):
        out = {}
        for url in self.server_urls():
            s = http_json(url + "/internal/stats")
            out[s["role"]] = s
        return out

    def run_fleet(self, **flags):
        out = self.dir / "fleet.json"
        args = [JAVA, "-Xmx2g", "-jar", str(JARS["fleet"]), "--origin", EDGE, "--out", str(out)]
        for k, v in flags.items():
            args += ["--" + k.replace("_", "-"), str(v)]
        with open(self.dir / "fleet.log", "w") as log:
            subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT, check=True)
        return json.loads(out.read_text())

    def packager_stats(self, pipeline, wait_s=30):
        path = self.dir / f"packager-{pipeline}.json"
        t0 = time.time()
        while not path.exists() and time.time() - t0 < wait_s:
            time.sleep(0.5)
        return json.loads(path.read_text()) if path.exists() else None

    def stop(self):
        for tag, p in reversed(self.procs):
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for tag, p in self.procs:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
        self.procs = []


@contextmanager
def run(name, **kw):
    flush_redis()
    r = Run(name, **kw)
    try:
        yield r
    finally:
        r.stop()


LOCK = Path("/tmp/claude-501/bench.lock")


@contextmanager
def bench_lock(owner="streamorigin", poll_s=30):
    """Shared with the other project benchmarking on this laptop: one load test at a time."""
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    waited = 0
    while True:
        try:
            LOCK.mkdir()
            break
        except FileExistsError:
            if waited % 300 == 0:
                who = (LOCK / "owner").read_text().strip() if (LOCK / "owner").exists() else "?"
                print(f"bench lock held by {who}; waiting", flush=True)
            time.sleep(poll_s)
            waited += poll_s
    tag = f"{owner} pid={os.getpid()} "
    (LOCK / "owner").write_text(f"{tag}since={time.strftime('%H:%M:%S')}\n")
    try:
        yield
    finally:
        try:
            if (LOCK / "owner").read_text().startswith(tag):  # never remove someone else's lock
                (LOCK / "owner").unlink()
                LOCK.rmdir()
        except FileNotFoundError:
            pass
        time.sleep(60)  # let the other project's waiter take its turn


def append(exp, row):
    RESULTS.mkdir(exist_ok=True)
    with open(RESULTS / f"{exp}.jsonl", "a") as f:
        f.write(json.dumps(row, sort_keys=False) + "\n")
