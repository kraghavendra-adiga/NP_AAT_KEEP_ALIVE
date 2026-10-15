"""Heartbeat-interval policies used by the server.

Every policy answers: "how long should the server stay idle on this connection
before sending the next heartbeat?" and may learn from observations.

observe(gap, ok, now): the server probed the connection after it had been
idle for `gap` virtual seconds and the probe either succeeded (ok=True) or
timed out after all retries (ok=False).
"""
import random

ARMS = [10, 15, 20, 30, 45, 60, 90, 135, 200, 300, 450]   # candidate intervals (virtual s)


class Policy:
    name = "base"

    def next_interval(self, now: float) -> float:
        raise NotImplementedError

    def observe(self, gap: float, ok: bool, now: float) -> None:
        pass

    def note_message(self, now: float) -> None:
        pass


class FixedPolicy(Policy):
    def __init__(self, interval: float, name: str = None):
        self.interval = float(interval)
        self.name = name or f"fixed-{int(interval)}"

    def next_interval(self, now):
        return self.interval


class TwoModePolicy(Policy):
    """Hand-set active/idle rule in the spirit of Johnsson et al. (2017)."""

    def __init__(self, active=15.0, idle=120.0, window=120.0):
        self.active, self.idle, self.window = active, idle, window
        self.last_msg = -1e18
        self.name = "two-mode"

    def note_message(self, now):
        self.last_msg = now

    def next_interval(self, now):
        return self.active if (now - self.last_msg) < self.window else self.idle


class AdaptiveUCB(Policy):
    """Proposed policy: Bayesian-UCB bandit over a ladder of heartbeat intervals.

    Arm i succeeds if the link survives an idle gap of ARMS[i].  Because the
    idle timeout is monotone, an observation shares information across arms:
      success at gap g -> every arm <= g would also have succeeded,
      failure  at gap g -> every arm >= g would also have failed.
    Each arm keeps Beta(s, f) pseudo-counts that are discounted toward the
    Beta(1,1) prior after every observation (forgetting), so the policy can
    re-explore when the path changes (e.g. Wi-Fi -> cellular).

    Objective: expected cost per unit time of arm a with success prob theta
        cost(a) = (1 + (1 - theta) * drop_cost) / a
    (one heartbeat costs 1 unit, one dropped connection `drop_cost` units).
    The arm with the lowest *optimistic* cost is played, where theta is
    replaced by its upper confidence bound  mean + z*std  of the Beta
    posterior (optimism in the face of uncertainty).  Exploration is
    frontier-limited: only arms up to one rung above the largest idle gap
    already confirmed safe are candidates.
    """

    def __init__(self, rng=None, drop_cost=60.0, gamma=0.98, z=2.0,
                 safe_arm=20, prior_strength=3.0, backoff="reset"):
        self.backoff = backoff              # "reset": fall back to safe arm; "rung": one rung down
        self.safe_arm = float(safe_arm)
        self.drop_cost = drop_cost
        self.gamma = gamma
        self.z = z
        n = len(ARMS)
        self.s = [1.0] * n
        self.f = [1.0] * n
        for i, a in enumerate(ARMS):
            if a <= safe_arm:
                self.s[i] += prior_strength
        self.max_ok = float(safe_arm)       # largest idle gap known to be safe
        self.name = "adaptive-ucb"

    def _limit_index(self):
        for i, a in enumerate(ARMS):
            if a > self.max_ok:
                return i                    # one rung above the frontier
        return len(ARMS) - 1

    def next_interval(self, now):
        best, best_c = 0, 1e18
        for i in range(self._limit_index() + 1):
            a, s, f = ARMS[i], self.s[i], self.f[i]
            mean = s / (s + f)
            std = (s * f / ((s + f) ** 2 * (s + f + 1.0))) ** 0.5
            theta = min(1.0, mean + self.z * std)
            c = (1.0 + (1.0 - theta) * self.drop_cost) / a
            if c < best_c:
                best, best_c = i, c
        return float(ARMS[best])

    def observe(self, gap, ok, now):
        g = self.gamma
        for i in range(len(ARMS)):
            self.s[i] = 1.0 + (self.s[i] - 1.0) * g
            self.f[i] = 1.0 + (self.f[i] - 1.0) * g
        # the measured gap overshoots the armed interval slightly (scheduling
        # and propagation delay), so compare with a 5% tolerance
        fail_gap = gap / 1.05
        for i, a in enumerate(ARMS):
            if ok and a <= gap:
                self.s[i] += 1.0
            elif (not ok) and a >= fail_gap:
                self.f[i] += 1.0
                if self.backoff == "reset":
                    # change-point: a failure voids stale success evidence for
                    # every arm at or above the failing gap (monotone timeout)
                    self.s[i] = 1.0
        if ok:
            self.max_ok = max(self.max_ok, gap)
        else:
            if self.backoff == "reset":
                self.max_ok = self.safe_arm
            else:
                below = [a for a in ARMS if a < fail_gap]
                self.max_ok = min(self.max_ok, float(below[-1]) if below else float(ARMS[0]))


def make_policy(name: str, rng: random.Random) -> Policy:
    if name == "default-keepalive":     # Linux tcp_keepalive_time = 7200 s
        return FixedPolicy(7200, "default-keepalive")
    if name.startswith("fixed-"):
        return FixedPolicy(float(name.split("-")[1]), name)
    if name == "two-mode":
        return TwoModePolicy()
    if name == "adaptive-ucb":
        return AdaptiveUCB(rng, backoff="reset")
    if name == "adaptive-ucb-rung":     # ablation: gradual one-rung back-off
        return AdaptiveUCB(rng, gamma=0.99, backoff="rung")
    raise ValueError(name)
