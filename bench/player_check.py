"""M5 check: a real browser plays the live edge. Launches headless Chrome on the demo page (the one
scripts/demo.sh serves), lets hls.js play for a while, reads the page's drift samples over the
DevTools protocol, saves a screenshot, and appends a row to results/m5_player.jsonl.

    scripts/demo.sh &          # live ffmpeg pipelines + split origin
    python3 bench/player_check.py --seconds 90
"""
import argparse
import base64
import json
import os
import shutil
import statistics
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import websocket

import harness as h

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT = 27222


class Cdp:
    def __init__(self, ws_url):
        self.ws = websocket.create_connection(ws_url, timeout=30)
        self.n = 0

    def call(self, method, **params):
        self.n += 1
        self.ws.send(json.dumps({"id": self.n, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == self.n:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg.get("result", {})

    def eval(self, expr):
        r = self.call("Runtime.evaluate", expression=expr, returnByValue=True, awaitPromise=True)
        return r.get("result", {}).get("value")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=90)
    ap.add_argument("--url", default=h.EDGE + "/demo/index.html")
    ap.add_argument("--screenshot", default=str(h.ROOT / "docs" / "demo.png"))
    a = ap.parse_args()

    profile = tempfile.mkdtemp(prefix="so-chrome-")
    chrome = subprocess.Popen([CHROME, "--headless=new", f"--remote-debugging-port={PORT}", f"--user-data-dir={profile}",
                               "--autoplay-policy=no-user-gesture-required", "--window-size=1400,900",
                               "--no-first-run", "--mute-audio", f"--remote-allow-origins=http://127.0.0.1:{PORT}", "about:blank"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                tabs = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json", timeout=2))
                page = next(t for t in tabs if t["type"] == "page")
                break
            except Exception:
                time.sleep(0.2)
        cdp = Cdp(page["webSocketDebuggerUrl"])
        cdp.call("Page.enable")
        cdp.call("Page.navigate", url=a.url)
        t0 = time.time()
        # Partway through, announce an ad break the way an operator would, so the cumulative
        # stream-event header shows up on the segments that follow.
        time.sleep(a.seconds * 0.6)
        cdp.eval("document.getElementById('notify').click(); true")
        time.sleep(a.seconds * 0.4)
        samples = cdp.eval("JSON.stringify(window.soSamples || [])")
        samples = json.loads(samples or "[]")
        video = cdp.eval("""JSON.stringify((() => { const v = document.getElementById('video');
            const q = v.getVideoPlaybackQuality ? v.getVideoPlaybackQuality() : {};
            return {currentTime: v.currentTime, readyState: v.readyState, paused: v.paused,
                    decoded: q.totalVideoFrames, dropped: q.droppedVideoFrames,
                    level: window.hls && window.hls.currentLevel,
                    events: document.getElementById('events').textContent}; })())""")
        shot = cdp.call("Page.captureScreenshot", format="png")
        Path(a.screenshot).parent.mkdir(parents=True, exist_ok=True)
        Path(a.screenshot).write_bytes(base64.b64decode(shot["data"]))
    finally:
        chrome.terminate()
        shutil.rmtree(profile, ignore_errors=True)

    video = json.loads(video or "{}")
    # Skip the first 20 s: the player is still converging on its live sync point.
    steady = [s for s in samples if s["t"] - samples[0]["t"] > 20_000] if samples else []
    drifts = [s["drift"] for s in steady]
    behind = [s["behind_s"] for s in steady]
    row = {
        "exp": "m5_player", "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "machine": h.machine(), "load": h.load(),
        "browser": "headless Chrome (new), hls.js 1.5.20", "played_s": a.seconds, "samples": len(samples),
        "steady_samples": len(steady),
        "drift_segments": {"max": max(drifts), "median": statistics.median(drifts),
                           "share_at_or_under_1": round(sum(d <= 1 for d in drifts) / len(drifts), 4)} if drifts else None,
        "behind_wall_clock_s": {"median": round(statistics.median(behind), 2), "max": round(max(behind), 2)} if behind else None,
        "video": video,
        "definition": "drift = live edge segment (origin's schedule) minus the segment the player is showing; "
                      "sampled every 500 ms",
    }
    h.append("m5_player", row)
    print(json.dumps({k: v for k, v in row.items() if k != "machine"}, indent=2))


if __name__ == "__main__":
    main()
