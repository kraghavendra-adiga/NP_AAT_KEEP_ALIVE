"""Heartbeat server: owns the keep-alive decision for every client.

The server (not the client) initiates heartbeats, as in the server-side
heartbeat disclosure.  The interval before each heartbeat is chosen by a
pluggable Policy.  A probe (heartbeat or application message) is sent up to
TRIES times; if no application-level ACK arrives the connection is declared
lost and the client must reconnect.  Undelivered messages stay queued on the
server and are re-sent after the client reconnects.
"""
import asyncio
import random
from common import ACK_TIMEOUT, TRIES, HELLO, HB, ACK, MSG, MACK
from policies import make_policy


class ClientState:
    def __init__(self, cid, policy):
        self.cid = cid
        self.policy = policy
        self.pending = {}          # msg_id -> created time (virtual)
        self.conn = None
        self.next_mid = 0


class Conn:
    def __init__(self, server, st, reader, writer):
        self.srv, self.st, self.r, self.w = server, st, reader, writer
        self.clock = server.clock
        self.last_ok = self.clock.now()
        self.lost = False
        self.acks = {}
        self.tasks = []
        self.seq = 0

    # ---- framing -------------------------------------------------------
    def _send(self, frame):
        try:
            self.w.write(frame)
        except Exception:
            pass

    async def reader_loop(self):
        try:
            while True:
                line = await self.r.readline()
                if not line:
                    return
                parts = line.split()
                if not parts:
                    continue
                if parts[0] == ACK:
                    self.srv.stats["hb_acks"] += 1
                    self.srv.stats["hb_bytes"] += len(line)
                    ev = self.acks.get(("HB", int(parts[1])))
                elif parts[0] == MACK:
                    ev = self.acks.get(("M", int(parts[1])))
                else:
                    continue
                if ev:
                    ev.set()
        except (ConnectionError, asyncio.CancelledError):
            return

    async def probe(self, frame, key, kind):
        ev = asyncio.Event()
        self.acks[key] = ev
        gap = 0.0
        first_gap = None
        for _ in range(TRIES):
            t_send = self.clock.now()
            gap = t_send - self.last_ok
            if first_gap is None:
                first_gap = gap          # idle gap at the first attempt
            self._send(frame)
            if kind == "HB":
                self.srv.stats["hb_sent"] += 1
                self.srv.stats["hb_bytes"] += len(frame)
            else:
                self.srv.stats["msg_frames"] += 1
            try:
                await asyncio.wait_for(ev.wait(), self.clock.real(ACK_TIMEOUT))
            except asyncio.TimeoutError:
                continue
            self.last_ok = self.clock.now()
            self.acks.pop(key, None)
            return True, gap
        self.acks.pop(key, None)
        return False, first_gap

    async def hb_loop(self):
        pol = self.st.policy
        try:
            while not self.lost:
                interval = pol.next_interval(self.clock.now())
                self.srv.log_interval(self.st.cid, self.clock.now(), interval)
                while True:
                    due = self.last_ok + interval
                    now = self.clock.now()
                    if now >= due:
                        break
                    await self.clock.sleep(due - now)
                    if self.lost:
                        return
                self.seq += 1
                ok, gap = await self.probe(f"HB {self.seq}\n".encode(), ("HB", self.seq), "HB")
                pol.observe(gap, ok, self.clock.now())
                if not ok:
                    self.declare_lost()
                    return
        except asyncio.CancelledError:
            return

    async def send_msg(self, mid):
        created = self.st.pending.get(mid)
        if created is None or self.lost:
            return
        ok, gap = await self.probe(f"MSG {mid}\n".encode(), ("M", mid), "MSG")
        now = self.clock.now()
        self.st.policy.observe(gap, ok, now)
        if ok:
            if mid in self.st.pending:
                self.srv.record_delivery(now - self.st.pending.pop(mid))
        elif not self.lost:
            self.declare_lost()

    def declare_lost(self):
        if self.lost:
            return
        self.lost = True
        self.srv.stats["drops"] += 1
        self.srv.drop_log.append((self.clock.now(), self.st.cid))
        self.close()

    def close(self):
        self.lost = True
        for t in self.tasks:
            if t is not asyncio.current_task():
                t.cancel()
        try:
            self.w.close()
        except Exception:
            pass


class Server:
    def __init__(self, clock, policy_name, n_clients, seed, msg_gap, duration, stats):
        self.clock = clock
        self.stats = stats
        self.latencies = []
        self.drop_log = []
        self.interval_log = {}
        self.duration = duration
        self.msg_gap = msg_gap
        self.seed = seed
        self.clients = {
            cid: ClientState(cid, make_policy(policy_name, random.Random(seed * 31 + cid)))
            for cid in range(n_clients)
        }
        self.server = None
        self.port = None
        self.bg = []

    def log_interval(self, cid, t, interval):
        if cid < 3:        # keep logs small: first three clients only
            self.interval_log.setdefault(cid, []).append((round(t, 1), interval))

    def record_delivery(self, latency):
        self.latencies.append(latency)
        self.stats["delivered"] += 1

    async def start(self):
        self.server = await asyncio.start_server(self._on_conn, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        for cid in self.clients:
            self.bg.append(asyncio.create_task(self._message_source(cid)))

    async def stop(self):
        for t in self.bg:
            t.cancel()
        for st in self.clients.values():
            if st.conn:
                st.conn.close()
        self.server.close()

    async def _message_source(self, cid):
        """Poisson message arrivals (same stream for every policy given the seed)."""
        rng = random.Random(self.seed * 100003 + cid)
        st = self.clients[cid]
        try:
            while True:
                await self.clock.sleep(rng.expovariate(1.0 / self.msg_gap))
                mid = st.next_mid
                st.next_mid += 1
                now = self.clock.now()
                st.pending[mid] = now
                self.stats["created"] += 1
                st.policy.note_message(now)
                c = st.conn
                if c and not c.lost:
                    c.tasks.append(asyncio.create_task(c.send_msg(mid)))
        except asyncio.CancelledError:
            return

    async def _on_conn(self, r, w):
        try:
            line = await r.readline()
            parts = line.split()
            if not parts or parts[0] != HELLO:
                w.close()
                return
            cid = int(parts[1])
        except Exception:
            w.close()
            return
        st = self.clients[cid]
        if st.conn and not st.conn.lost:
            self.stats["reconnect_while_alive"] += 1
        if st.conn:
            st.conn.close()
        conn = Conn(self, st, r, w)
        st.conn = conn
        self.stats["connects"] += 1
        conn.tasks.append(asyncio.create_task(conn.reader_loop()))
        conn.tasks.append(asyncio.create_task(conn.hb_loop()))
        for mid in sorted(st.pending):                  # re-send queued messages
            conn.tasks.append(asyncio.create_task(conn.send_msg(mid)))
