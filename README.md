# AI-Based Adaptive Server-Initiated Keep-Alive for TCP

Testbed for learning a heartbeat interval online when a NAT/firewall silently
drops idle TCP flows after an unknown, changing timeout.

```
 client(s) <--TCP--> NAT proxy (hidden idle timeout, loss) <--TCP--> server (heartbeat policy)
```

## Files

| File | Role |
|---|---|
| `common.py` | Virtual clock (time compressed 250x), protocol constants |
| `nat_proxy.py` | NAT/firewall emulator: after the idle timeout the flow is silently blackholed (no RST/FIN); optional packet loss |
| `server.py` | Sends heartbeats (`HB n`), expects `ACK n`, retries 3x, declares the link lost, queues undelivered messages |
| `client.py` | Passive client: replies to heartbeats/messages, reconnects on EOF or 900 s silence |
| `policies.py` | Interval policies: default keep-alive (7200 s), fixed-N, two-mode, **AdaptiveUCB (proposed)** |
| `run_experiment.py` | One run: scenario x policy x seed |
| `run_all.py` | Full grid (3 scenarios x 6 policies x 3 seeds, about 40 min) |
| `analyze.py` | Builds `summary.csv` and figures in `figs/` |
| `demo.py` | 1-minute comparison of 3 policies |

## Setup (Python 3.9+)

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Everything except plotting uses only the standard library.
No root access or special network setup is needed: all sockets are on `127.0.0.1`.

## Run

```bash
python demo.py                                    # ~1 min quick comparison
python run_experiment.py dynamic adaptive-ucb 1   # one run (~45 s), prints JSON metrics
python run_experiment.py dynamic fixed-60 1 out.json   # also saves full result incl. drop log
python run_all.py                                 # full grid -> results/*.json (~40 min)
python analyze.py                                 # -> summary.csv, figs/*.png
```

Scenarios: `static` (T=300 s), `dynamic` (300 -> 60 -> 180 s), `lossy` (T=240 s, 10% loss).
Policies: `default-keepalive`, `fixed-30`, `fixed-60`, `fixed-120`, `two-mode`, `adaptive-ucb`.

## Metrics
heartbeats / client-hour, connection drops / client-hour, delivery latency (mean, p95),
delivery ratio, wakeups / client-hour (frames the client receives + connects; an energy proxy).

## How the adaptive policy works
A ladder of candidate intervals (10 ... 450 s). Each arm keeps a Beta(s, f) belief that the
link survives an idle gap of that length. Because the NAT timeout is monotone, one observation
updates many arms (success at gap g -> all shorter arms; failure at g -> all longer arms).
The arm with the lowest optimistic cost `(1 + (1 - theta_UCB) * drop_cost) / interval` is played,
restricted to one rung above the largest gap known safe. A failure resets to the safe interval
and voids stale success evidence (change-point handling); beliefs decay with gamma = 0.98.

## Notes
- Timing is virtual: change `SCALE` in `common.py` (e.g. `1.0` for real time) to slow it down.
  Heavily loaded machines can distort compressed timing; keep other programs idle during runs.
- Results vary slightly between machines; use several seeds.
