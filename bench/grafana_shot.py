"""Screenshots the provisioned Grafana dashboard with headless Chrome (display may be off)."""
import base64
import json
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

from player_check import CHROME, Cdp

PORT = 27223
url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:27030/d/streamorigin/streamorigin?orgId=1&from=now-6m&to=now&kiosk"
out = sys.argv[2] if len(sys.argv) > 2 else "docs/grafana.png"
profile = tempfile.mkdtemp(prefix="so-chrome-")
chrome = subprocess.Popen([CHROME, "--headless=new", f"--remote-debugging-port={PORT}", f"--user-data-dir={profile}",
                           "--window-size=1600,1100", "--no-first-run", f"--remote-allow-origins=http://127.0.0.1:{PORT}",
                           "about:blank"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(50):
        try:
            page = next(t for t in json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json", timeout=2))
                        if t["type"] == "page")
            break
        except Exception:
            time.sleep(0.2)
    cdp = Cdp(page["webSocketDebuggerUrl"])
    cdp.call("Page.enable")
    cdp.call("Page.navigate", url=url)
    time.sleep(12)
    shot = cdp.call("Page.captureScreenshot", format="png")
    open(out, "wb").write(base64.b64decode(shot["data"]))
    print("saved", out)
finally:
    chrome.terminate()
    shutil.rmtree(profile, ignore_errors=True)
