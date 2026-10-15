"""Run the full experiment grid (scenario x policy x seed), 2 runs in parallel."""
import itertools
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

SCENARIOS = ["static", "dynamic", "lossy"]
POLICIES = ["default-keepalive", "fixed-30", "fixed-60", "fixed-120", "two-mode", "adaptive-ucb"]
SEEDS = [1, 2, 3]
OUT = "results"
os.makedirs(OUT, exist_ok=True)


def job(args):
    scn, pol, seed = args
    path = f"{OUT}/{scn}__{pol}__{seed}.json"
    if os.path.exists(path):
        return path
    subprocess.run([sys.executable, "run_experiment.py", scn, pol, str(seed), path],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    print("done", path, flush=True)
    return path


if __name__ == "__main__":
    grid = list(itertools.product(SCENARIOS, POLICIES, SEEDS))
    with ThreadPoolExecutor(max_workers=2) as ex:
        list(ex.map(job, grid))
    print("ALL DONE", flush=True)
