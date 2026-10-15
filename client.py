"""Mobile client: passive receiver that only replies to the server.

The client never originates keep-alive traffic.  It owns one safety net: if it
hears nothing from the server for CLIENT_WATCHDOG virtual seconds (or sees an
EOF) it reconnects, mirroring the "client uses a larger timer" rule of the
server-side heartbeat disclosure.
"""
import asyncio
from common import CLIENT_WATCHDOG, RECONNECT_DELAY, HELLO, HB, ACK, MSG, MACK


class Client:
    def __init__(self, cid, clock, proxy_port, stats):
        self.cid, self.clock, self.port, self.stats = cid, clock, proxy_port, stats

    async def run(self, start_delay=0.0):
        await self.clock.sleep(start_delay)
        try:
            while True:
                try:
                    r, w = await asyncio.open_connection("127.0.0.1", self.port)
                except OSError:
                    await self.clock.sleep(RECONNECT_DELAY)
                    continue
                w.write(f"{HELLO.decode()} {self.cid}\n".encode())
                self.stats["client_connects"] += 1
                last_rx = self.clock.now()
                try:
                    while True:
                        remaining = CLIENT_WATCHDOG - (self.clock.now() - last_rx)
                        try:
                            line = await asyncio.wait_for(r.readline(), max(0.0, self.clock.real(remaining)))
                        except asyncio.TimeoutError:
                            self.stats["watchdog_reconnects"] += 1
                            break
                        if not line:
                            break
                        last_rx = self.clock.now()
                        parts = line.split()
                        if not parts:
                            continue
                        self.stats["client_rx_frames"] += 1
                        if parts[0] == HB:
                            w.write(f"{ACK.decode()} {parts[1].decode()}\n".encode())
                        elif parts[0] == MSG:
                            w.write(f"{MACK.decode()} {parts[1].decode()}\n".encode())
                except (ConnectionError, OSError):
                    pass
                finally:
                    try:
                        w.close()
                    except Exception:
                        pass
                await self.clock.sleep(RECONNECT_DELAY)
        except asyncio.CancelledError:
            return
