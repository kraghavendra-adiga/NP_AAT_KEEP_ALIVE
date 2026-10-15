"""NAT / firewall emulator (TCP proxy with a hidden idle timeout).

client <--TCP--> NatProxy <--TCP--> server

For each flow the proxy keeps a "last activity" timestamp that is refreshed by
every frame forwarded in either direction.  If a frame arrives after the flow
has been idle longer than the (hidden, possibly time-varying) timeout, the
mapping is considered expired: this and every later frame is silently
discarded (no RST, no FIN) - exactly what a real NAT/firewall does.
Optionally each frame is also dropped with probability `loss`.
"""
import asyncio
import random
from common import HELLO


class Flow:
    def __init__(self, now):
        self.last = now
        self.dead = False
        self.jitter = 1.0
        self.cid = None


class NatProxy:
    def __init__(self, clock, server_addr, scenario, seed, stats):
        self.clock = clock
        self.server_addr = server_addr
        self.scn = scenario
        self.seed = seed
        self.stats = stats
        self.rng = random.Random(seed * 7919 + 13)
        self.server = None
        self.port = None
        self.flows = []
        self._writers = set()

    async def start(self):
        self.server = await asyncio.start_server(self._on_client, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        self.server.close()
        for w in list(self._writers):
            try:
                w.close()
            except Exception:
                pass

    async def _on_client(self, cr, cw):
        try:
            sr, sw = await asyncio.open_connection(*self.server_addr)
        except Exception:
            cw.close()
            return
        self._writers.update((cw, sw))
        flow = Flow(self.clock.now())
        self.flows.append(flow)
        self.stats["flows"] += 1
        t1 = asyncio.create_task(self._pump(cr, sw, flow, True, cw))
        t2 = asyncio.create_task(self._pump(sr, cw, flow, False, cw))
        try:
            await asyncio.gather(t1, t2, return_exceptions=True)
        finally:
            for w in (cw, sw):
                self._writers.discard(w)
                try:
                    w.close()
                except Exception:
                    pass

    async def _pump(self, reader, writer, flow, from_client, cw):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    if from_client:
                        return                      # client closed -> tear down
                    if flow.dead:
                        return                      # server FIN is swallowed by expired NAT
                    try:
                        writer.close()
                    except Exception:
                        pass
                    return
                now = self.clock.now()
                parts = line.split()
                if from_client and parts and parts[0] == HELLO and flow.cid is None:
                    flow.cid = int(parts[1])
                    flow.jitter = random.Random(self.seed * 1000003 + flow.cid).uniform(0.8, 1.2)
                    flow.last = now
                    writer.write(line)              # connection set-up always passes
                    await writer.drain()
                    continue
                if flow.dead:
                    self.stats["blackholed"] += 1
                    continue
                timeout = self.scn.timeout(now) * flow.jitter
                if now - flow.last > timeout:
                    flow.dead = True
                    self.stats["nat_expiries"] += 1
                    self.stats["blackholed"] += 1
                    continue
                if self.scn.loss > 0 and self.rng.random() < self.scn.loss:
                    self.stats["lost_frames"] += 1
                    continue
                flow.last = now
                writer.write(line)
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            return
