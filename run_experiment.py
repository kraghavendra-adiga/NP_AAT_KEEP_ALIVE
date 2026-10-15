"""Run one experiment: scenario x policy x seed, N concurrent clients.

usage: python run_experiment.py <scenario> <policy> <seed> [out.json]
"""
import asyncio
import json
import statistics
import sys
from collections import defaultdict

from common import VClock
from nat_proxy import NatProxy
from server import Server
from client import Client

DURATION = 10800.0     # 3 virtual hours
N_CLIENTS = 10
MSG_GAP = 900.0        # mean virtual seconds between application messages per client


class Scenario:
    def __init__(self, name, timeout_fn, loss=0.0):
        self.name, self.timeout, self.loss = name, timeout_fn, loss


SCENARIOS = {
    "control": Scenario("control", lambda t: 1e9),
    "static": Scenario("static", lambda t: 300.0),
    "dynamic": Scenario("dynamic", lambda t: 300.0 if t < 3600 else (60.0 if t < 7200 else 180.0)),
    "lossy": Scenario("lossy", lambda t: 240.0, loss=0.10),
}


async def run_once(scn_name, pol_name, seed, n_clients=N_CLIENTS, duration=DURATION):
    scn = SCENARIOS[scn_name]
    clock = VClock()
    stats = defaultdict(int)
    server = Server(clock, pol_name, n_clients, seed, MSG_GAP, duration, stats)
    await server.start()
    proxy = NatProxy(clock, ("127.0.0.1", server.port), scn, seed, stats)
    await proxy.start()
    clients = [Client(cid, clock, proxy.port, stats) for cid in range(n_clients)]
    tasks = [asyncio.create_task(c.run(start_delay=(cid * 37) % 60)) for cid, c in enumerate(clients)]
    await clock.sleep(duration)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await server.stop()
    await proxy.stop()
    await asyncio.sleep(0.05)

    lat = sorted(server.latencies)
    hours = n_clients * duration / 3600.0

    def pct(p):
        return lat[min(len(lat) - 1, int(p * len(lat)))] if lat else float("nan")

    res = {
        "scenario": scn_name, "policy": pol_name, "seed": seed,
        "n_clients": n_clients, "duration": duration, "client_hours": hours,
        "hb_sent": stats["hb_sent"], "hb_acks": stats["hb_acks"], "hb_bytes": stats["hb_bytes"],
        "hb_per_client_hour": stats["hb_sent"] / hours,
        "drops": stats["drops"], "drops_per_client_hour": stats["drops"] / hours,
        "nat_expiries": stats["nat_expiries"], "lost_frames": stats["lost_frames"],
        "connects": stats["connects"], "watchdog_reconnects": stats["watchdog_reconnects"],
        "reconnect_while_alive": stats["reconnect_while_alive"],
        "created": stats["created"], "delivered": stats["delivered"],
        "delivery_ratio": stats["delivered"] / max(1, stats["created"]),
        "lat_mean": statistics.fmean(lat) if lat else float("nan"),
        "lat_median": pct(0.5), "lat_p95": pct(0.95), "lat_max": lat[-1] if lat else float("nan"),
        "client_rx_frames": stats["client_rx_frames"],
        "wakeups_per_client_hour": (stats["client_rx_frames"] + stats["client_connects"]) / hours,
        "drop_log": [(round(t, 1), c) for t, c in server.drop_log],
        "interval_log": {str(k): v for k, v in server.interval_log.items()},
    }
    return res


def main():
    scn, pol, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
    res = asyncio.run(run_once(scn, pol, seed))
    if len(sys.argv) > 4:
        with open(sys.argv[4], "w") as f:
            json.dump(res, f)
    brief = {k: (round(v, 2) if isinstance(v, float) else v) for k, v in res.items()
             if k not in ("drop_log", "interval_log")}
    print(json.dumps(brief))


if __name__ == "__main__":
    main()
