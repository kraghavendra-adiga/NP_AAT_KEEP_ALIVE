"""Live, interactive dashboard for the keep-alive testbed (browser UI).

    python dashboard.py              # then open http://127.0.0.1:8000 (opens automatically)
    python dashboard.py --port 8080  # use another port

It runs the SAME real-socket simulation as run_experiment.py (clients -> NAT
proxy -> server) in a background thread and streams its state to a web page
that polls /api/state.  Uses only the standard library.  No existing project
file is modified: just put dashboard.py and dashboard.html next to them.
"""
import argparse
import asyncio
import json
import os
import threading
import time
import webbrowser
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from client import Client
from common import VClock
from nat_proxy import NatProxy
from run_experiment import MSG_GAP, SCENARIOS, Scenario
from server import Server

HERE = os.path.dirname(os.path.abspath(__file__))

# id, long label, short label (chart end-labels), colour (same as the report figures)
POLICIES = [
    ("adaptive-ucb", "Adaptive UCB (proposed)", "Adaptive", "#2a78d6"),
    ("two-mode", "Two-mode (Johnsson-style)", "Two-mode", "#eb6834"),
    ("fixed-30", "Fixed 30 s", "Fixed 30", "#1baf7a"),
    ("fixed-60", "Fixed 60 s", "Fixed 60", "#eda100"),
    ("fixed-120", "Fixed 120 s", "Fixed 120", "#e87ba4"),
    ("default-keepalive", "Default keep-alive (7200 s)", "Default", "#008300"),
]
PINFO = {p[0]: p for p in POLICIES}

LOCK = threading.Lock()
STATE = {"version": 0, "status": "idle"}


def publish(**kw):
    with LOCK:
        STATE.update(kw)
        STATE["version"] = STATE.get("version", 0) + 1


# --------------------------------------------------------------------------
# simulation side (uses the unchanged project modules)
# --------------------------------------------------------------------------
class LiveServer(Server):
    """Server that also remembers the current interval of every client."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.cur_interval = {}
        self.iv0 = []                      # (t, interval) history of client 0

    def log_interval(self, cid, t, interval):
        self.cur_interval[cid] = interval
        if cid == 0:
            self.iv0.append((round(t, 1), interval))
        super().log_interval(cid, t, interval)


class Lane:
    """One policy running against its own proxy + server + clients."""

    def __init__(self, pol, scn, seed, n, scale, duration):
        self.pol, self.scn, self.n, self.seed = pol, scn, n, seed
        self.clock = VClock(scale)
        self.stats = defaultdict(int)
        self.server = LiveServer(self.clock, pol, n, seed, MSG_GAP, duration, self.stats)
        self.proxy = None
        self.tasks = []
        self.hist = {"t": [], "hb": [], "drops": []}
        self.tout0 = []
        self.seen_drops = 0

    async def start(self):
        await self.server.start()
        self.proxy = NatProxy(self.clock, ("127.0.0.1", self.server.port), self.scn, self.seed, self.stats)
        await self.proxy.start()
        self.tasks = [
            asyncio.create_task(Client(cid, self.clock, self.proxy.port, self.stats).run(start_delay=(cid * 37) % 60))
            for cid in range(self.n)
        ]

    async def stop(self):
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.server.stop()
        await self.proxy.stop()

    def snapshot(self, duration, events):
        now = self.clock.now()
        shown = min(now, duration)
        hours = max(self.n * shown / 3600.0, 1e-9)
        st = self.stats
        by_cid = {}
        for f in self.proxy.flows:
            if f.cid is not None:
                by_cid[f.cid] = f              # newest flow per client wins
        tiles = []
        for cid in range(self.n):
            f = by_cid.get(cid)
            if f is None:
                tiles.append({"id": cid, "status": "connecting"})
                continue
            conn = self.server.clients[cid].conn
            tout = self.scn.timeout(now) * f.jitter
            age = now - f.last
            if conn is None or conn.lost:
                status = "reconnecting"
            elif f.dead or age > tout:
                status = "expired"             # NAT already forgot it, server has not noticed yet
            else:
                status = "alive"
            iv = self.server.cur_interval.get(cid)
            nxt = None
            if conn is not None and not conn.lost and iv:
                nxt = max(0.0, conn.last_ok + iv - now)
            tiles.append({
                "id": cid, "status": status, "age": round(age, 1),
                "timeout": round(tout, 1) if tout < 1e8 else None,
                "interval": iv, "next": None if nxt is None else round(nxt, 1),
            })
        if tiles and tiles[0].get("timeout"):
            self.tout0.append((round(shown, 1), tiles[0]["timeout"]))
        self.hist["t"].append(round(shown, 1))
        self.hist["hb"].append(st["hb_sent"])
        self.hist["drops"].append(st["drops"])
        for t, cid in self.server.drop_log[self.seen_drops:]:
            events.append({"t": round(t, 1), "pol": self.pol, "cid": cid})
        self.seen_drops = len(self.server.drop_log)
        lat = self.server.latencies
        info = PINFO[self.pol]
        return {
            "pol": self.pol, "label": info[1], "short": info[2], "color": info[3],
            "hb": st["hb_sent"], "drops": st["drops"], "created": st["created"],
            "delivered": st["delivered"], "nat_expiries": st["nat_expiries"],
            "hb_h": st["hb_sent"] / hours, "drops_h": st["drops"] / hours,
            "lat_mean": (sum(lat) / len(lat)) if lat else None,
            "wake_h": (st["client_rx_frames"] + st["client_connects"]) / hours,
            "hist": self.hist, "iv": self.server.iv0[-400:], "tout0": self.tout0[-600:],
            "drops0": [round(t, 1) for t, c in self.server.drop_log if c == 0],
            "clients": tiles,
        }


def build_scenario(cfg):
    if cfg["scenario"] == "dynamic":
        return SCENARIOS["dynamic"]
    if cfg["scenario"] == "lossy":
        return SCENARIOS["lossy"]
    T, loss = cfg["timeout"], cfg["loss"] / 100.0
    return Scenario("static", lambda t, T=T: T, loss=loss)


async def simulate(cfg, stop):
    scn = build_scenario(cfg)
    lanes = [Lane(p, scn, cfg["seed"], cfg["n"], 1.0 / cfg["speed"], cfg["duration"]) for p in cfg["policies"]]
    events = []
    base = {"config": cfg, "duration": cfg["duration"], "n": cfg["n"], "error": None}
    try:
        for ln in lanes:
            await ln.start()
        while True:
            finished = min(ln.clock.now() for ln in lanes) >= cfg["duration"]
            out = [ln.snapshot(cfg["duration"], events) for ln in lanes]
            t = min(min(ln.clock.now() for ln in lanes), cfg["duration"])
            status = "done" if finished else ("stopped" if stop.is_set() else "running")
            publish(status=status, t=t, lanes=out, events=events[-60:], **base)
            if finished or stop.is_set():
                break
            await asyncio.sleep(0.25)
    finally:
        for ln in lanes:
            await ln.stop()


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def clean_config(d):
    pols = [p for p in d.get("policies", []) if p in PINFO] or ["adaptive-ucb"]
    scen = d.get("scenario") if d.get("scenario") in ("static", "dynamic", "lossy") else "static"
    return {
        "policies": pols, "scenario": scen,
        "timeout": clamp(float(d.get("timeout", 300)), 20, 1200),
        "loss": clamp(float(d.get("loss", 0)), 0, 40),
        "duration": clamp(float(d.get("duration", 10800)), 600, 21600),
        "speed": clamp(float(d.get("speed", 250)), 10, 400),
        "n": int(clamp(int(d.get("n", 10)), 2, 20)),
        "seed": int(d.get("seed", 1)),
    }


class Runner:
    def __init__(self):
        self.thread = None
        self.stop = threading.Event()

    def halt(self):
        if self.thread and self.thread.is_alive():
            self.stop.set()
            self.thread.join(timeout=15)

    def start(self, cfg):
        self.halt()
        self.stop = threading.Event()
        publish(status="starting", lanes=[], events=[], t=0, error=None, config=cfg,
                duration=cfg["duration"], n=cfg["n"])
        self.thread = threading.Thread(target=self._run, args=(cfg, self.stop), daemon=True)
        self.thread.start()

    def _run(self, cfg, stop):
        try:
            asyncio.run(simulate(cfg, stop))
        except Exception as e:                     # show the problem in the UI
            publish(status="error", error=f"{type(e).__name__}: {e}")


RUNNER = Runner()


# --------------------------------------------------------------------------
# HTTP side
# --------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, body, ctype="application/json", code=200):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                self._send(f.read(), "text/html; charset=utf-8")
        elif path == "/api/options":
            self._send(json.dumps({"policies": [
                {"id": p[0], "label": p[1], "short": p[2], "color": p[3]} for p in POLICIES]}))
        elif path == "/api/state":
            with LOCK:
                body = json.dumps(STATE)
            self._send(body)
        elif path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        else:
            self._send("not found", "text/plain", 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        try:
            data = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            data = {}
        if self.path == "/api/start":
            try:
                RUNNER.start(clean_config(data))
                self._send('{"ok": true}')
            except (ValueError, TypeError) as e:
                self._send(json.dumps({"ok": False, "error": str(e)}), code=400)
        elif self.path == "/api/stop":
            RUNNER.stop.set()
            self._send('{"ok": true}')
        else:
            self._send("not found", "text/plain", 404)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}"
    print(f"Dashboard running at {url}   (Ctrl+C to quit)")
    if not args.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        RUNNER.stop.set()
        RUNNER.halt()


if __name__ == "__main__":
    main()