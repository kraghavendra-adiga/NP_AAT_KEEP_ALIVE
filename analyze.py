"""Aggregate results/*.json -> summary.csv + figures (PNG) for the report."""
import glob
import json
import math
import os
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SCEN = ["static", "dynamic", "lossy"]
SCEN_LABEL = {"static": "Static NAT\n(T = 300 s)",
              "dynamic": "Dynamic NAT\n(300 -> 60 -> 180 s)",
              "lossy": "Lossy link\n(T = 240 s, 10% loss)"}
POL = ["adaptive-ucb", "two-mode", "fixed-30", "fixed-60", "fixed-120", "default-keepalive"]
ABL = "adaptive-ucb-rung"
POL_LABEL = {"adaptive-ucb-rung": "Adaptive UCB, gradual back-off (ablation)",
             "adaptive-ucb": "Adaptive UCB (proposed)", "two-mode": "Two-mode (Johnsson-style)",
             "fixed-30": "Fixed 30 s", "fixed-60": "Fixed 60 s", "fixed-120": "Fixed 120 s",
             "default-keepalive": "Default keep-alive (7200 s)"}
# fixed categorical order (validated palette): colour follows the entity, never its rank
COL = {"adaptive-ucb": "#2a78d6", "two-mode": "#eb6834", "fixed-30": "#1baf7a",
       "fixed-60": "#eda100", "fixed-120": "#e87ba4", "default-keepalive": "#008300"}
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"

METRICS = ["hb_per_client_hour", "drops_per_client_hour", "lat_mean", "lat_p95",
           "delivery_ratio", "wakeups_per_client_hour", "watchdog_reconnects", "hb_bytes"]


def load():
    data = {}
    for fn in glob.glob("results/*.json"):
        d = json.load(open(fn))
        data.setdefault((d["scenario"], d["policy"]), []).append(d)
    return data


def agg(runs, key):
    vals = [r[key] for r in runs if not (isinstance(r[key], float) and math.isnan(r[key]))]
    if not vals:
        return float("nan"), float("nan")
    return statistics.fmean(vals), (statistics.stdev(vals) if len(vals) > 1 else 0.0)


def style(ax):
    ax.set_facecolor(SURF)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8)
    ax.yaxis.grid(True, color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def grouped_bars(data, key, ylabel, title, fname, fmt="{:.1f}"):
    fig, ax = plt.subplots(figsize=(8.6, 4.2), facecolor=SURF)
    style(ax)
    n = len(POL)
    w = 0.13
    for j, p in enumerate(POL):
        for i, s in enumerate(SCEN):
            runs = data.get((s, p), [])
            if not runs:
                continue
            m, sd = agg(runs, key)
            x = i + (j - (n - 1) / 2) * (w + 0.012)
            ax.bar(x, m, w, color=COL[p], edgecolor=SURF, linewidth=1.2,
                   label=POL_LABEL[p] if i == 0 else None)
            ax.errorbar(x, m, yerr=sd, color=INK2, linewidth=0.8, capsize=1.5)
            ax.text(x, m + sd + 0.01 * ax.get_ylim()[1], fmt.format(m), ha="center", va="bottom",
                    fontsize=6.3, color=INK2, rotation=90)
    ax.set_xticks(range(len(SCEN)))
    ax.set_xticklabels([SCEN_LABEL[s] for s in SCEN], fontsize=8.5, color=INK)
    ax.set_ylabel(ylabel, fontsize=9, color=INK)
    ax.set_title(title, fontsize=10.5, color=INK, loc="left")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.12)
    ax.legend(fontsize=7.2, frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.2))
    fig.tight_layout()
    fig.savefig(fname, dpi=200, facecolor=SURF)
    plt.close(fig)


def adaptation_plot(data, fname):
    """Dynamic scenario: hidden NAT timeout band vs. intervals chosen by the proposed policy."""
    runs = data.get(("dynamic", "adaptive-ucb"), [])
    if not runs:
        return
    r = sorted(runs, key=lambda d: d["seed"])[0]
    fig, ax = plt.subplots(figsize=(8.6, 3.8), facecolor=SURF)
    style(ax)
    xs = [0, 3600, 3600, 7200, 7200, 10800]
    lo = [240, 240, 48, 48, 144, 144]
    hi = [360, 360, 72, 72, 216, 216]
    ax.fill_between(xs, lo, hi, color="#c9c8c2", alpha=0.7, linewidth=0,
                    label="Hidden NAT timeout (0.8-1.2 x T across clients)")
    for cid, ls in (("0", "-"), ("1", "--")):
        log = r["interval_log"].get(cid, [])
        if log:
            t = [a for a, _ in log]
            v = [b for _, b in log]
            ax.step(t, v, where="post", color=COL["adaptive-ucb"], linewidth=1.6, linestyle=ls,
                    label=f"Adaptive UCB interval (client {cid})")
    dts = [t for t, c in r["drop_log"] if c in (0, 1)]
    ax.scatter(dts, [4.5] * len(dts), marker="v", s=26, color="#e34948", zorder=5,
               edgecolor=SURF, linewidth=0.8, label="Connection drop (client 0/1)")
    ax.set_yscale("log")
    ax.set_ylim(4, 700)
    ax.set_yticks([10, 30, 60, 120, 300])
    ax.set_yticklabels(["10", "30", "60", "120", "300"])
    ax.set_xlabel("Virtual time (s)", fontsize=9, color=INK)
    ax.set_ylabel("Seconds", fontsize=9, color=INK)
    ax.set_title("Dynamic scenario: heartbeat interval tracks the hidden idle timeout",
                 fontsize=10.5, color=INK, loc="left")
    ax.legend(fontsize=7.2, frameon=False, ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.2))
    fig.tight_layout()
    fig.savefig(fname, dpi=200, facecolor=SURF)
    plt.close(fig)


def tradeoff(data, fname):
    fig, axes = plt.subplots(1, 3, figsize=(9.2, 3.5), facecolor=SURF, sharey=False)
    for ax, s in zip(axes, SCEN):
        style(ax)
        for p in POL:
            runs = data.get((s, p), [])
            if not runs:
                continue
            hb, _ = agg(runs, "hb_per_client_hour")
            dr, _ = agg(runs, "drops_per_client_hour")
            ax.scatter(max(hb, 0.3), dr, s=46, color=COL[p], edgecolor=SURF, linewidth=1.2, zorder=3)
            ax.annotate(POL_LABEL[p].split(" (")[0].replace("Default keep-alive", "Default"),
                        (max(hb, 0.3), dr), textcoords="offset points", xytext=(5, 4),
                        fontsize=6.3, color=INK2)
        ax.set_xscale("log")
        ax.set_xlim(0.3, 400)
        ax.set_title(SCEN_LABEL[s].replace("\n", " "), fontsize=8.5, color=INK, loc="left")
        ax.set_xlabel("Heartbeats / client-hour (log)", fontsize=8, color=INK)
    axes[0].set_ylabel("Connection drops / client-hour", fontsize=8.5, color=INK)
    fig.suptitle("Overhead vs. reliability (bottom-left is better)", fontsize=10.5, color=INK, x=0.01,
                 ha="left")
    fig.tight_layout()
    fig.savefig(fname, dpi=200, facecolor=SURF)
    plt.close(fig)


def main():
    os.makedirs("figs", exist_ok=True)
    data = load()
    rows = []
    for s in SCEN:
        for p in POL + [ABL]:
            runs = data.get((s, p), [])
            if not runs:
                continue
            row = {"scenario": s, "policy": p, "runs": len(runs)}
            for k in METRICS:
                m, sd = agg(runs, k)
                row[k] = m
                row[k + "_sd"] = sd
            rows.append(row)
    import csv
    with open("summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    grouped_bars(data, "hb_per_client_hour", "Heartbeat packets / client-hour",
                 "Keep-alive overhead (lower is better)", "figs/fig_overhead.png")
    grouped_bars(data, "drops_per_client_hour", "Connection drops / client-hour",
                 "Silent connection drops (lower is better)", "figs/fig_drops.png", "{:.2f}")
    grouped_bars(data, "lat_mean", "Mean delivery latency (virtual s)",
                 "Message delivery latency (lower is better)", "figs/fig_latency.png", "{:.0f}")
    adaptation_plot(data, "figs/fig_adaptation.png")
    tradeoff(data, "figs/fig_tradeoff.png")
    print("rows:", len(rows))
    for r in rows:
        print(f"{r['scenario']:8s} {r['policy']:18s} n={r['runs']} hb/h={r['hb_per_client_hour']:7.1f}"
              f" drops/h={r['drops_per_client_hour']:5.2f} lat_mean={r['lat_mean']:7.1f}"
              f" p95={r['lat_p95']:7.1f} deliv={r['delivery_ratio']:.3f}")


if __name__ == "__main__":
    main()
