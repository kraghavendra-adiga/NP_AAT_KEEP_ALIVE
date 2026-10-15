"""Shared helpers: virtual clock and frame definitions.

The testbed runs real TCP sockets on localhost, but time is compressed:
1 virtual second = SCALE real seconds, so a 3-hour experiment takes ~43 s.
All timers (heartbeat intervals, NAT idle timeout, ACK timeouts) are expressed
in virtual seconds and converted with the clock below.
"""
import asyncio
import time

SCALE = 0.004          # real seconds per virtual second (250x speed-up)
ACK_TIMEOUT = 10.0     # virtual seconds to wait for an application-level ACK
TRIES = 3              # first attempt + 2 retries before a link is declared lost
CLIENT_WATCHDOG = 900.0   # client reconnects after this much silence (virtual s)
RECONNECT_DELAY = 1.0

HELLO = b"HELLO"
HB = b"HB"
ACK = b"ACK"
MSG = b"MSG"
MACK = b"MACK"


class VClock:
    def __init__(self, scale: float = SCALE):
        self.scale = scale
        self.t0 = time.monotonic()

    def now(self) -> float:
        return (time.monotonic() - self.t0) / self.scale

    async def sleep(self, virtual_seconds: float) -> None:
        await asyncio.sleep(max(0.0, virtual_seconds) * self.scale)

    def real(self, virtual_seconds: float) -> float:
        return virtual_seconds * self.scale
