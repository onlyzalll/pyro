import asyncio
import logging
from typing import Optional

from ..session.internals import DataCenter
from .transport import TCP, TCPAbridged

log = logging.getLogger(__name__)


class Connection:
    MAX_CONNECTION_ATTEMPTS = 5
    CONNECT_TIMEOUT = 10

    def __init__(
        self, dc_id: int, test_mode: bool, ipv6: bool, proxy: dict, media: bool = False
    ):
        self.dc_id = dc_id
        self.test_mode = test_mode
        self.ipv6 = ipv6
        self.proxy = proxy
        self.media = media

        self.address = DataCenter(dc_id, test_mode, ipv6, media)
        self.protocol: TCP = None

    async def connect(self):
        for attempt in range(Connection.MAX_CONNECTION_ATTEMPTS):
            self.protocol = TCPAbridged(self.ipv6, self.proxy)

            try:
                log.info("Connecting to Telegram DC%s...", self.dc_id)

                await asyncio.wait_for(
                    self.protocol.connect(self.address),
                    timeout=Connection.CONNECT_TIMEOUT
                )

            except (OSError, asyncio.TimeoutError) as e:
                log.warning(
                    "Connection attempt %s failed due to network issues: %s",
                    attempt + 1,
                    e
                )

                try:
                    await self.protocol.close()
                except Exception:
                    pass

                await asyncio.sleep(2)

            else:
                log.info(
                    "Connected! %s DC%s%s - IPv%s",
                    "Test" if self.test_mode else "Production",
                    self.dc_id,
                    " (media)" if self.media else "",
                    "6" if self.ipv6 else "4",
                )
                return

        log.error("Connection failed after %s attempts", Connection.MAX_CONNECTION_ATTEMPTS)
        raise ConnectionError("Unable to connect to Telegram DC")

    async def close(self):
        if self.protocol:
            try:
                await self.protocol.close()
            except Exception:
                pass
        log.info("Disconnected")

    async def send(self, data: bytes):
        try:
            await self.protocol.send(data)
        except Exception as e:
            log.warning("Send failed: %s. Reconnecting...", e)
            await self.reconnect()
            await self.protocol.send(data)

    async def recv(self) -> Optional[bytes]:
        try:
            return await self.protocol.recv()

        except (OSError, asyncio.TimeoutError) as e:
            log.warning("Connection lost while receiving: %s", e)
            await self.reconnect()
            return None

        except Exception as e:
            log.warning("Unexpected recv error: %s", e)
            await self.reconnect()
            return None

    async def reconnect(self):
        log.info("Reconnecting to Telegram DC%s...", self.dc_id)

        try:
            await self.close()
        except Exception:
            pass

        await asyncio.sleep(2)
        await self.connect()
