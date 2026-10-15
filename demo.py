"""Quick demo (~1 minute): compares three policies on the static-NAT scenario.

    python demo.py

Runs 10 real TCP clients through a NAT-emulating proxy whose hidden idle
timeout is 300 s (+/-20% per client).  Time is compressed 250x, so 1.5 virtual
hours take about 22 real seconds per policy.
"""
import asyncio

from run_experiment import run_once

POLICIES = ["default-keepalive", "fixed-60", "adaptive-ucb"]
DURATION = 5400.0     # virtual seconds (1.5 h)


async def main():
    print(f"{'policy':20s} {'heartbeats/client-h':>20s} {'drops/client-h':>16s} {'mean latency (s)':>18s}")
    for pol in POLICIES:
        r = await run_once("static", pol, seed=1, duration=DURATION)
        print(f"{pol:20s} {r['hb_per_client_hour']:20.1f} {r['drops_per_client_hour']:16.2f} "
              f"{r['lat_mean']:18.1f}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
