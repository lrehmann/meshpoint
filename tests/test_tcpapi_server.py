"""Integration tests for the Meshtastic TCP API server.

A raw asyncio client speaks the stream framing directly (deterministic,
no threads) and drives the full handshake + send/receive round trip
against the server backed by fakes. An optional test exercises the real
``meshtastic.TCPInterface`` client when ``MESHPOINT_TCP_INTEROP`` is set.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import unittest
from datetime import datetime, timezone

from meshtastic.protobuf import mesh_pb2, portnums_pb2

from src.config import AppConfig
from src.decode.crypto_service import CryptoService
from src.models.device_identity import DeviceIdentity
from src.models.packet import Packet, PacketType, Protocol
from src.models.signal import SignalMetrics
from src.tcpapi.framing import FrameAccumulator, encode_frame
from src.tcpapi.server import MeshtasticTcpServer, build_tcp_api_server


class _FakeNodeRepo:
    async def get_all_with_signal(self, limit=200):
        return [
            {
                "node_id": "a1b2c3d4", "long_name": "Alpha", "short_name": "ALFA",
                "hardware_model": "31", "role": "CLIENT", "protocol": "meshtastic",
                "latitude": 37.77, "longitude": -122.41, "altitude": 10,
                "last_heard": datetime.now(timezone.utc).isoformat(),
                "latest_snr": 6.0, "latest_battery": 90,
            },
        ]


class _FakePipeline:
    def __init__(self):
        self.callbacks = []
        self.node_repo = _FakeNodeRepo()
        self._crypto = CryptoService(default_key_b64="AQ==")

    def on_packet(self, cb):
        self.callbacks.append(cb)

    def emit(self, packet):
        for cb in self.callbacks:
            cb(packet)


class _SendResult:
    def __init__(self, ok):
        self.success = ok
        self.error = "" if ok else "stub failure"


class _FakeTx:
    def __init__(self, enabled=True):
        self.sent = []
        self._enabled = enabled

    @property
    def source_node_id(self):
        return 0x12345678

    @property
    def meshtastic_enabled(self):
        return self._enabled

    async def send_text(
        self, text, destination=0, channel=0, want_ack=False, packet_id=None
    ):
        self.sent.append((text, destination, channel, want_ack, packet_id))
        return _SendResult(self._enabled)


def _make_config(enabled=True, transmit=True):
    cfg = AppConfig()
    cfg.tcp_api.enabled = enabled
    cfg.tcp_api.port = 0  # ephemeral; real port read from the bound socket
    cfg.tcp_api.mdns = False
    cfg.transmit.enabled = transmit
    cfg.meshtastic.primary_channel_name = "LongFast"
    return cfg


def _identity():
    return DeviceIdentity(
        device_id="dev-1", long_name="Harness", short_name="HMP",
        latitude=37.0, longitude=-122.0, altitude=5,
    )


def _text_rx_packet():
    return Packet(
        packet_id="0000abcd", source_id="a1b2c3d4", destination_id="ffffffff",
        protocol=Protocol.MESHTASTIC, packet_type=PacketType.TEXT,
        hop_limit=3, hop_start=3, channel_hash=8,
        decoded_payload={"text": "hello mesh"}, raw_app_payload=b"hello mesh",
        decrypted=True,
        signal=SignalMetrics(rssi=-70, snr=5.5, frequency_mhz=906.875,
                             spreading_factor=11, bandwidth_khz=250.0),
        timestamp=datetime.now(timezone.utc),
    )


class TestBuildGating(unittest.TestCase):
    def test_disabled_returns_none(self) -> None:
        cfg = _make_config(enabled=False)
        self.assertIsNone(
            build_tcp_api_server(cfg, _FakePipeline(), None, _identity())
        )

    def test_enabled_returns_server(self) -> None:
        cfg = _make_config(enabled=True)
        srv = build_tcp_api_server(cfg, _FakePipeline(), _FakeTx(), _identity())
        self.assertIsInstance(srv, MeshtasticTcpServer)

    def test_node_num_falls_back_to_config_when_no_tx(self) -> None:
        cfg = _make_config(enabled=True, transmit=False)
        cfg.transmit.node_id = 0x0BADCAFE
        srv = MeshtasticTcpServer(
            config=cfg, pipeline=_FakePipeline(), tx_service=None,
            identity=_identity(),
        )
        self.assertEqual(srv._my_node_num, 0x0BADCAFE)


class TestTcpServerRoundTrip(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pipeline = _FakePipeline()
        self.tx = _FakeTx()
        self.server = MeshtasticTcpServer(
            config=_make_config(), pipeline=self.pipeline,
            tx_service=self.tx, identity=_identity(),
        )
        await self.server.start()
        self.port = self.server._server.sockets[0].getsockname()[1]
        self.reader, self.writer = await asyncio.open_connection(
            "127.0.0.1", self.port
        )
        self.acc = FrameAccumulator()

    async def asyncTearDown(self):
        self.writer.close()
        with contextlib.suppress(Exception):
            await self.writer.wait_closed()
        await self.server.stop()

    async def _read_frames(self, predicate, timeout=5.0):
        """Read FromRadio frames until ``predicate(fr)`` is true."""
        collected = []
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise AssertionError("timed out waiting for frame")
            data = await asyncio.wait_for(self.reader.read(4096), remaining)
            if not data:
                raise AssertionError("connection closed early")
            for payload in self.acc.feed(data):
                fr = mesh_pb2.FromRadio()
                fr.ParseFromString(payload)
                collected.append(fr)
                if predicate(fr):
                    return collected

    async def _do_want_config(self, want_id=4242):
        tr = mesh_pb2.ToRadio()
        tr.want_config_id = want_id
        self.writer.write(encode_frame(tr.SerializeToString()))
        await self.writer.drain()
        return await self._read_frames(
            lambda fr: fr.WhichOneof("payload_variant") == "config_complete_id"
        )

    async def test_want_config_handshake(self):
        frames = await self._do_want_config(99)
        kinds = [fr.WhichOneof("payload_variant") for fr in frames]
        self.assertEqual(kinds[0], "my_info")
        self.assertEqual(frames[-1].config_complete_id, 99)

        my = next(f for f in frames if f.WhichOneof("payload_variant") == "my_info")
        self.assertEqual(my.my_info.my_node_num, 0x12345678)

        node_nums = [
            f.node_info.num for f in frames
            if f.WhichOneof("payload_variant") == "node_info"
        ]
        self.assertIn(0x12345678, node_nums)  # self
        self.assertIn(0xA1B2C3D4, node_nums)  # peer

        chan_idx = [
            f.channel.index for f in frames
            if f.WhichOneof("payload_variant") == "channel"
        ]
        self.assertIn(0, chan_idx)

    async def test_outbound_text_reaches_tx(self):
        await self._do_want_config()
        tr = mesh_pb2.ToRadio()
        tr.packet.to = 0xFFFFFFFF
        tr.packet.channel = 0
        tr.packet.id = 0x7777
        tr.packet.decoded.portnum = portnums_pb2.PortNum.Value("TEXT_MESSAGE_APP")
        tr.packet.decoded.payload = b"ping from phone"
        self.writer.write(encode_frame(tr.SerializeToString()))
        await self.writer.drain()

        frames = await self._read_frames(
            lambda fr: fr.WhichOneof("payload_variant") == "queueStatus"
        )
        qs = frames[-1].queueStatus
        self.assertEqual(qs.mesh_packet_id, 0x7777)
        self.assertEqual(qs.res, 0)
        self.assertEqual(
            self.tx.sent[-1], ("ping from phone", 0xFFFFFFFF, 0, False, 0x7777)
        )

    async def test_outbound_text_want_ack_gets_routing_ack(self):
        await self._do_want_config()
        tr = mesh_pb2.ToRadio()
        tr.packet.to = 0xA1B2C3D4
        tr.packet.channel = 0
        tr.packet.id = 0xEACDE3D6
        tr.packet.want_ack = True
        tr.packet.decoded.portnum = portnums_pb2.PortNum.Value("TEXT_MESSAGE_APP")
        tr.packet.decoded.payload = b"ping with ack"
        self.writer.write(encode_frame(tr.SerializeToString()))
        await self.writer.drain()

        frames = await self._read_frames(
            lambda fr: (
                fr.WhichOneof("payload_variant") == "packet"
                and fr.packet.decoded.request_id == 0xEACDE3D6
            )
        )
        mp = frames[-1].packet
        self.assertEqual(mp.decoded.portnum, portnums_pb2.PortNum.Value("ROUTING_APP"))
        self.assertEqual(getattr(mp, "from"), 0xA1B2C3D4)
        self.assertEqual(mp.to, 0x12345678)
        routing = mesh_pb2.Routing()
        routing.ParseFromString(mp.decoded.payload)
        self.assertEqual(routing.error_reason, mesh_pb2.Routing.Error.Value("NONE"))
        self.assertEqual(
            self.tx.sent[-1], ("ping with ack", 0xA1B2C3D4, 0, True, 0xEACDE3D6)
        )

    async def test_inbound_packet_streamed_to_client(self):
        await self._do_want_config()
        # Emit a decoded RX packet from the pipeline; it must reach the client.
        self.pipeline.emit(_text_rx_packet())
        frames = await self._read_frames(
            lambda fr: fr.WhichOneof("payload_variant") == "packet"
        )
        mp = frames[-1].packet
        self.assertEqual(getattr(mp, "from"), 0xA1B2C3D4)
        self.assertEqual(mp.decoded.payload, b"hello mesh")


@unittest.skipUnless(
    os.environ.get("MESHPOINT_TCP_INTEROP"),
    "interop test with the real meshtastic TCPInterface; set "
    "MESHPOINT_TCP_INTEROP=1 to run (spawns threads + pubsub)",
)
class TestRealClientInterop(unittest.IsolatedAsyncioTestCase):
    async def test_real_tcp_interface_connects(self):
        import functools

        pipeline = _FakePipeline()
        server = MeshtasticTcpServer(
            config=_make_config(), pipeline=pipeline,
            tx_service=_FakeTx(), identity=_identity(),
        )
        await server.start()
        port = server._server.sockets[0].getsockname()[1]
        try:
            import meshtastic.tcp_interface as ti

            iface = await asyncio.to_thread(
                functools.partial(
                    ti.TCPInterface, "127.0.0.1", portNumber=port, connectNow=True
                )
            )
            self.assertEqual(iface.myInfo.my_node_num, 0x12345678)
            self.assertIn("!a1b2c3d4", iface.nodes)
            await asyncio.to_thread(iface.close)
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
