"""mDNS / Bonjour advertisement for the Meshtastic TCP API.

Meshtastic apps discover network nodes by browsing the
``_meshtastic._tcp.local.`` service type, so advertising it lets a phone
on the same LAN find the Meshpoint automatically instead of typing an IP.

Depends on the optional ``zeroconf`` package. If it is not installed (or
registration fails), discovery degrades to "reachable by IP" and the TCP
server keeps working -- this module never raises into the caller.
"""

from __future__ import annotations

import logging
import socket

logger = logging.getLogger(__name__)

SERVICE_TYPE = "_meshtastic._tcp.local."


class MdnsAdvertiser:
    """Registers/unregisters a ``_meshtastic._tcp`` service via zeroconf."""

    def __init__(
        self,
        *,
        port: int,
        node_num: int,
        long_name: str,
        short_name: str,
    ):
        self._port = port
        self._node_num = node_num & 0xFFFFFFFF
        self._long_name = long_name
        self._short_name = short_name
        self._aiozc = None
        self._info = None

    async def start(self) -> bool:
        """Register the service. Returns True if advertising started."""
        try:
            from zeroconf import ServiceInfo
            from zeroconf.asyncio import AsyncZeroconf
        except ImportError:
            logger.info(
                "mDNS discovery disabled -- 'zeroconf' not installed "
                "(the TCP API is still reachable by IP at port %d)",
                self._port,
            )
            return False

        try:
            ip = _local_ip()
            node_hex = f"{self._node_num:08x}"
            instance = f"Meshpoint-{node_hex}"
            self._info = ServiceInfo(
                type_=SERVICE_TYPE,
                name=f"{instance}.{SERVICE_TYPE}",
                addresses=[socket.inet_aton(ip)],
                port=self._port,
                properties={
                    "id": f"!{node_hex}",
                    "shortname": self._short_name,
                    "longname": self._long_name,
                },
                server=f"{instance}.local.",
            )
            self._aiozc = AsyncZeroconf()
            await self._aiozc.async_register_service(self._info)
            logger.info(
                "mDNS: advertising %s on %s:%d", instance, ip, self._port
            )
            return True
        except Exception:
            logger.warning(
                "mDNS advertisement failed; continuing without discovery",
                exc_info=True,
            )
            await self._safe_close()
            return False

    async def stop(self) -> None:
        if self._aiozc is None:
            return
        try:
            if self._info is not None:
                await self._aiozc.async_unregister_service(self._info)
        except Exception:
            logger.debug("mDNS unregister failed", exc_info=True)
        await self._safe_close()

    async def _safe_close(self) -> None:
        if self._aiozc is None:
            return
        try:
            await self._aiozc.async_close()
        except Exception:
            logger.debug("mDNS close failed", exc_info=True)
        finally:
            self._aiozc = None
            self._info = None


def _local_ip() -> str:
    """Best-effort LAN IP for the advertised address."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No packets are sent; this just selects the outbound interface.
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()
