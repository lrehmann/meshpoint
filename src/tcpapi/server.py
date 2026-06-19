"""Asyncio TCP server implementing the Meshtastic client stream API.

One :class:`MeshtasticTcpServer` is built and started by the FastAPI
lifespan when ``tcp_api.enabled`` is set. It:

* accepts TCP connections on the configured port (default 4403),
* answers each client's ``want_config`` with the device/channel/node
  snapshot the Meshtastic apps expect,
* streams every decoded Meshtastic packet (subscribed via
  ``PipelineCoordinator.on_packet``) to connected clients as
  ``FromRadio{packet}``,
* transmits text the app sends (``ToRadio{packet}``) through the
  existing :class:`TxService`, and
* optionally advertises itself over mDNS for LAN discovery.

The heavy protobuf work lives in :mod:`src.tcpapi.protocol`; this module
is the I/O and lifecycle shell. Protobuf-dependent imports are done
lazily inside methods so importing the app never requires ``meshtastic``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Optional

from src.models.packet import Packet, Protocol
from src.tcpapi.discovery import MdnsAdvertiser
from src.tcpapi.framing import FrameAccumulator, encode_frame

if TYPE_CHECKING:  # pragma: no cover
    from src.config import AppConfig
    from src.coordinator import PipelineCoordinator
    from src.models.device_identity import DeviceIdentity
    from src.transmit.tx_service import TxService

logger = logging.getLogger(__name__)

# Per-client outbound queue depth. A phone that stalls drops the oldest
# streamed packets rather than letting the buffer grow without bound.
_CLIENT_QUEUE_MAX = 256
_READ_CHUNK = 4096


class ClientConnection:
    """A single connected Meshtastic app client."""

    def __init__(self, server: "MeshtasticTcpServer", reader, writer, peer):
        self._server = server
        self._reader = reader
        self._writer = writer
        self._peer = peer
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=_CLIENT_QUEUE_MAX)
        self._accumulator = FrameAccumulator()
        self._dropped = 0

    @property
    def peer(self):
        return self._peer

    def enqueue(self, frame: bytes) -> None:
        """Queue a streamed frame, dropping the oldest if the client lags."""
        try:
            self._queue.put_nowait(frame)
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                self._queue.put_nowait(frame)
            self._dropped += 1
            if self._dropped % 50 == 1:
                logger.warning(
                    "TCP API client %s is slow; dropped %d frame(s)",
                    self._peer,
                    self._dropped,
                )

    async def send(self, frame: bytes) -> None:
        """Queue a handshake/ack frame with backpressure (no drop)."""
        await self._queue.put(frame)

    async def run(self) -> None:
        writer_task = asyncio.create_task(self._writer_loop())
        try:
            await self._reader_loop()
        finally:
            writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await writer_task
            await self._close()

    async def _reader_loop(self) -> None:
        while True:
            data = await self._reader.read(_READ_CHUNK)
            if not data:
                break  # EOF
            for payload in self._accumulator.feed(data):
                await self._server.handle_to_radio(self, payload)

    async def _writer_loop(self) -> None:
        while True:
            frame = await self._queue.get()
            self._writer.write(frame)
            await self._writer.drain()

    def request_close(self) -> None:
        with contextlib.suppress(Exception):
            self._writer.close()

    async def _close(self) -> None:
        with contextlib.suppress(Exception):
            self._writer.close()
            await self._writer.wait_closed()


class MeshtasticTcpServer:
    """TCP stream-API server presenting the Meshpoint as a Meshtastic node."""

    def __init__(
        self,
        *,
        config: "AppConfig",
        pipeline: "PipelineCoordinator",
        tx_service: Optional["TxService"],
        identity: "DeviceIdentity",
    ):
        self._config = config
        self._cfg = config.tcp_api
        self._pipeline = pipeline
        self._tx_service = tx_service
        self._identity = identity
        self._clients: set[ClientConnection] = set()
        self._server: Optional[asyncio.AbstractServer] = None
        self._mdns: Optional[MdnsAdvertiser] = None
        self._my_node_num = self._resolve_my_node_num()
        self._channel_defs: list = []
        self._hash_to_index: dict[int, int] = {}

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        self._build_channels()
        self._pipeline.on_packet(self._on_mesh_packet)
        self._server = await asyncio.start_server(
            self._on_client, self._cfg.host, self._cfg.port
        )
        logger.info(
            "TCP API (Meshtastic stream) listening on %s:%d  node=!%08x  tx=%s",
            self._cfg.host,
            self._cfg.port,
            self._my_node_num,
            "on" if self._tx_enabled() else "off (read-only)",
        )
        if self._cfg.mdns:
            self._mdns = MdnsAdvertiser(
                port=self._cfg.port,
                node_num=self._my_node_num,
                long_name=self._identity.long_name,
                short_name=self._identity.short_name,
            )
            await self._mdns.start()

    async def stop(self) -> None:
        if self._mdns is not None:
            await self._mdns.stop()
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
        for conn in list(self._clients):
            conn.request_close()
        self._clients.clear()
        logger.info("TCP API stopped")

    # -- connection handling ----------------------------------------------

    async def _on_client(self, reader, writer) -> None:
        peer = writer.get_extra_info("peername")
        conn = ClientConnection(self, reader, writer, peer)
        self._clients.add(conn)
        logger.info(
            "TCP API client connected: %s (%d total)", peer, len(self._clients)
        )
        try:
            await conn.run()
        except Exception:
            logger.debug("TCP API client %s error", peer, exc_info=True)
        finally:
            self._clients.discard(conn)
            logger.info("TCP API client disconnected: %s", peer)

    async def handle_to_radio(self, conn: ClientConnection, payload: bytes) -> None:
        from meshtastic.protobuf import mesh_pb2

        try:
            to_radio = mesh_pb2.ToRadio()
            to_radio.ParseFromString(payload)
        except Exception:
            logger.debug("TCP API: undecodable ToRadio frame", exc_info=True)
            return

        which = to_radio.WhichOneof("payload_variant")
        if which == "want_config_id":
            await self._handle_want_config(conn, to_radio.want_config_id)
        elif which == "packet":
            await self._handle_to_radio_packet(conn, to_radio.packet)
        elif which == "heartbeat":
            pass
        elif which == "disconnect":
            conn.request_close()
        else:
            logger.debug("TCP API: unhandled ToRadio variant %r", which)

    async def _handle_want_config(
        self, conn: ClientConnection, want_config_id: int
    ) -> None:
        from src.tcpapi import protocol as proto

        nodes: list[dict] = []
        try:
            nodes = await self._pipeline.node_repo.get_all_with_signal(
                limit=self._cfg.max_nodes
            )
        except Exception:
            logger.debug("TCP API: node snapshot unavailable", exc_info=True)

        radio = self._config.radio
        tx = self._config.transmit
        frames = proto.build_want_config_frames(
            my_node_num=self._my_node_num,
            long_name=self._identity.long_name,
            short_name=self._identity.short_name,
            public_key=self._public_key(),
            device_id=self._config.device.device_id,
            latitude=self._identity.latitude,
            longitude=self._identity.longitude,
            altitude=self._identity.altitude,
            firmware_version=self._identity.firmware_version,
            nodes=nodes,
            channels=self._channel_defs,
            region=radio.region,
            spreading_factor=radio.spreading_factor,
            bandwidth_khz=radio.bandwidth_khz,
            frequency_mhz=radio.frequency_mhz,
            tx_enabled=tx.enabled,
            tx_power_dbm=tx.tx_power_dbm,
            hop_limit=tx.hop_limit,
            want_config_id=want_config_id,
        )
        logger.info(
            "TCP API: want_config from %s -> %d frames (%d nodes)",
            conn.peer,
            len(frames),
            len(nodes),
        )
        for fr in frames:
            await conn.send(encode_frame(fr.SerializeToString()))

    async def _handle_to_radio_packet(
        self, conn: ClientConnection, mesh_packet
    ) -> None:
        from src.tcpapi.mappings import TEXT_MESSAGE_APP

        data = mesh_packet.decoded
        orig_id = mesh_packet.id

        if data.portnum != TEXT_MESSAGE_APP:
            logger.info(
                "TCP API: ignoring outbound packet from %s (portnum=%s "
                "not supported for TX yet)",
                conn.peer,
                data.portnum,
            )
            await self._ack(conn, orig_id, success=False)
            return

        if not self._tx_enabled():
            logger.warning(
                "TCP API: %s tried to send but transmit is disabled "
                "(read-only mode)",
                conn.peer,
            )
            await self._ack(conn, orig_id, success=False)
            return

        text = data.payload.decode("utf-8", errors="replace")
        destination = mesh_packet.to
        channel = mesh_packet.channel
        want_ack = mesh_packet.want_ack

        result = await self._tx_service.send_text(
            text,
            destination=destination,
            channel=channel,
            want_ack=want_ack,
            packet_id=orig_id,
        )
        if not result.success:
            logger.warning(
                "TCP API: send from %s failed: %s", conn.peer, result.error
            )
        await self._ack(conn, orig_id, success=result.success)
        if result.success and want_ack:
            from src.tcpapi import protocol as proto

            ack = proto.build_routing_ack(
                packet_id=orig_id,
                from_node=destination,
                to_node=self._my_node_num,
                channel=channel,
            )
            await conn.send(encode_frame(ack.SerializeToString()))

    async def _ack(
        self, conn: ClientConnection, packet_id: int, *, success: bool
    ) -> None:
        from src.tcpapi import protocol as proto

        fr = proto.build_queue_status(packet_id=packet_id, success=success)
        await conn.send(encode_frame(fr.SerializeToString()))

    # -- packet fan-out ----------------------------------------------------

    def _on_mesh_packet(self, packet: Packet) -> None:
        """Sync callback from the decode pipeline; fan out to clients.

        Must be fast and non-blocking -- it runs inline on the pipeline's
        event loop. Builds the FromRadio frame once and enqueues it on each
        client's bounded queue.
        """
        if not self._clients or packet.protocol != Protocol.MESHTASTIC:
            return
        try:
            from src.tcpapi import protocol as proto

            channel_index = self._hash_to_index.get(packet.channel_hash, 0)
            fr = proto.packet_to_from_radio(
                packet,
                channel_index=channel_index,
                forward_encrypted=self._cfg.forward_encrypted,
            )
            if fr is None:
                return
            frame = encode_frame(fr.SerializeToString())
        except Exception:
            logger.debug(
                "TCP API: failed to build FromRadio for %s",
                packet.packet_id,
                exc_info=True,
            )
            return
        for conn in list(self._clients):
            conn.enqueue(frame)

    # -- setup helpers -----------------------------------------------------

    def _build_channels(self) -> None:
        from src.tcpapi.protocol import channel_defs

        mt = self._config.meshtastic
        self._channel_defs = channel_defs(
            mt.primary_channel_name, mt.default_key_b64, mt.channel_keys
        )
        self._hash_to_index = {}
        crypto = getattr(self._pipeline, "_crypto", None)
        if crypto is None:
            return
        try:
            keys = crypto.get_all_keys()
            for cdef in self._channel_defs:
                if cdef.index < len(keys):
                    h = crypto.compute_channel_hash(cdef.name, keys[cdef.index])
                    self._hash_to_index.setdefault(h, cdef.index)
        except Exception:
            logger.debug("TCP API: channel hash map unavailable", exc_info=True)

    def _public_key(self) -> Optional[bytes]:
        crypto = getattr(self._pipeline, "_crypto", None)
        return getattr(crypto, "public_key", None) if crypto else None

    def _tx_enabled(self) -> bool:
        return (
            self._tx_service is not None and self._tx_service.meshtastic_enabled
        )

    def _resolve_my_node_num(self) -> int:
        if self._tx_service is not None:
            try:
                return int(self._tx_service.source_node_id) & 0xFFFFFFFF
            except (TypeError, ValueError):
                pass
        configured = self._config.transmit.node_id
        if configured:
            return int(configured) & 0xFFFFFFFF
        device_id = self._config.device.device_id
        if device_id:
            from src.transmit.tx_service import TxService

            return TxService._derive_node_id(device_id)
        return 0


def build_tcp_api_server(
    config: "AppConfig",
    pipeline: "PipelineCoordinator",
    tx_service: Optional["TxService"],
    identity: "DeviceIdentity",
) -> Optional[MeshtasticTcpServer]:
    """Construct the server when enabled, else return None.

    Mirrors the ``_build_*`` helpers in src/api/server.py so the lifespan
    wiring is a single ``build -> start ... stop`` call.
    """
    if not getattr(config, "tcp_api", None) or not config.tcp_api.enabled:
        return None
    return MeshtasticTcpServer(
        config=config,
        pipeline=pipeline,
        tx_service=tx_service,
        identity=identity,
    )
