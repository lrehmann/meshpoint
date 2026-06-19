"""Tests for the FromRadio/ToRadio protocol builders.

These parse the produced protobufs back with the meshtastic library to
confirm a real client would read what we intend.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from meshtastic.protobuf import config_pb2, mesh_pb2, portnums_pb2

from src.models.packet import Packet, PacketType, Protocol
from src.models.signal import SignalMetrics
from src.tcpapi import protocol as proto
from src.tcpapi.mappings import PACKET_TYPE_TO_PORTNUM

DEFAULT_KEY_B64 = "AQ=="
SECRET_KEY_B64 = "1PG7OiApB1nwvP+rz05pAQ=="


def _signal() -> SignalMetrics:
    return SignalMetrics(
        rssi=-70, snr=5.5, frequency_mhz=906.875,
        spreading_factor=11, bandwidth_khz=250.0,
    )


class TestPortnumMap(unittest.TestCase):
    """Canonical portnums -- guards against the mqtt_formatter drift."""

    def test_map_matches_protobuf_enum(self) -> None:
        cases = {
            PacketType.TEXT: "TEXT_MESSAGE_APP",
            PacketType.POSITION: "POSITION_APP",
            PacketType.NODEINFO: "NODEINFO_APP",
            PacketType.TELEMETRY: "TELEMETRY_APP",
            PacketType.DETECTION_SENSOR: "DETECTION_SENSOR_APP",
            PacketType.PAXCOUNTER: "PAXCOUNTER_APP",
            PacketType.STORE_FORWARD: "STORE_FORWARD_APP",
            PacketType.RANGE_TEST: "RANGE_TEST_APP",
        }
        for ptype, name in cases.items():
            self.assertEqual(
                PACKET_TYPE_TO_PORTNUM[ptype],
                portnums_pb2.PortNum.Value(name),
            )


class TestChannelDefs(unittest.TestCase):
    def test_primary_then_secondaries(self) -> None:
        defs = proto.channel_defs(
            "LongFast", DEFAULT_KEY_B64, {"Secret": SECRET_KEY_B64}
        )
        self.assertEqual([d.index for d in defs], [0, 1])
        self.assertEqual(defs[0].name, "LongFast")
        self.assertEqual(defs[0].psk, b"\x01")  # default sent as 1-byte form
        self.assertEqual(defs[1].name, "Secret")
        self.assertEqual(len(defs[1].psk), 16)


class TestWantConfig(unittest.TestCase):
    def _build(self):
        nodes = [
            {
                "node_id": "a1b2c3d4", "long_name": "Alpha", "short_name": "ALFA",
                "hardware_model": "31", "role": "CLIENT", "protocol": "meshtastic",
                "latitude": 37.77, "longitude": -122.41, "altitude": 10,
                "last_heard": datetime.now(timezone.utc).isoformat(),
                "latest_snr": 6.0, "latest_battery": 90,
            },
            {  # meshcore node must be filtered out of the meshtastic node DB
                "node_id": "ff01", "long_name": "MC", "protocol": "meshcore",
                "last_heard": datetime.now(timezone.utc).isoformat(),
            },
        ]
        return proto.build_want_config_frames(
            my_node_num=0x12345678,
            long_name="Harness", short_name="HMP", public_key=None,
            device_id="dev-1", latitude=37.0, longitude=-122.0, altitude=5,
            firmware_version="0.7.6", nodes=nodes,
            channels=proto.channel_defs(
                "LongFast", DEFAULT_KEY_B64, {"Secret": SECRET_KEY_B64}
            ),
            region="EU_868", spreading_factor=11, bandwidth_khz=250.0,
            frequency_mhz=869.525, tx_enabled=True, tx_power_dbm=14,
            hop_limit=3, want_config_id=4242,
        )

    def test_sequence_shape(self) -> None:
        frames = self._build()
        kinds = [fr.WhichOneof("payload_variant") for fr in frames]
        self.assertEqual(kinds[0], "my_info")
        self.assertEqual(kinds[-1], "config_complete_id")
        self.assertIn("channel", kinds)
        self.assertIn("metadata", kinds)

    def test_metadata_uses_app_compatible_firmware_version(self) -> None:
        frames = self._build()
        meta = next(f for f in frames if f.WhichOneof("payload_variant") == "metadata")
        self.assertEqual(meta.metadata.firmware_version, "2.6.0.meshpoint")

    def test_my_info_node_num(self) -> None:
        frames = self._build()
        my = next(f for f in frames if f.WhichOneof("payload_variant") == "my_info")
        self.assertEqual(my.my_info.my_node_num, 0x12345678)

    def test_config_complete_echoes_id(self) -> None:
        frames = self._build()
        self.assertEqual(frames[-1].config_complete_id, 4242)

    def test_self_node_first_and_meshcore_filtered(self) -> None:
        frames = self._build()
        nums = [
            f.node_info.num
            for f in frames
            if f.WhichOneof("payload_variant") == "node_info"
        ]
        self.assertEqual(nums[0], 0x12345678)       # self node first
        self.assertIn(0xA1B2C3D4, nums)             # meshtastic peer present
        self.assertEqual(len(nums), 2)              # meshcore node excluded

    def test_peer_node_fields(self) -> None:
        frames = self._build()
        peer = next(
            f.node_info for f in frames
            if f.WhichOneof("payload_variant") == "node_info"
            and f.node_info.num == 0xA1B2C3D4
        )
        self.assertEqual(peer.user.long_name, "Alpha")
        self.assertEqual(peer.user.id, "!a1b2c3d4")
        self.assertEqual(peer.position.latitude_i, int(37.77 * 1e7))
        self.assertEqual(peer.device_metrics.battery_level, 90)

    def test_lora_config_region_and_preset(self) -> None:
        frames = self._build()
        lora = next(
            f.config.lora for f in frames
            if f.WhichOneof("payload_variant") == "config"
            and f.config.WhichOneof("payload_variant") == "lora"
        )
        self.assertEqual(
            lora.region, config_pb2.Config.LoRaConfig.RegionCode.Value("EU_868")
        )
        self.assertTrue(lora.use_preset)
        self.assertEqual(
            lora.modem_preset,
            config_pb2.Config.LoRaConfig.ModemPreset.Value("LONG_FAST"),
        )

    def test_channels_roundtrip(self) -> None:
        frames = self._build()
        chans = {
            f.channel.index: f.channel
            for f in frames
            if f.WhichOneof("payload_variant") == "channel"
        }
        self.assertEqual(chans[0].role, 1)  # PRIMARY
        self.assertEqual(bytes(chans[0].settings.psk), b"\x01")
        self.assertEqual(chans[1].role, 2)  # SECONDARY
        self.assertEqual(chans[1].settings.name, "Secret")


class TestPacketToFromRadio(unittest.TestCase):
    def _text_packet(self, **kw) -> Packet:
        base = dict(
            packet_id="0000abcd", source_id="a1b2c3d4",
            destination_id="ffffffff", protocol=Protocol.MESHTASTIC,
            packet_type=PacketType.TEXT, hop_limit=3, hop_start=3,
            channel_hash=8, decoded_payload={"text": "hi"},
            raw_app_payload=b"hi", decrypted=True, signal=_signal(),
            timestamp=datetime.now(timezone.utc),
        )
        base.update(kw)
        return Packet(**base)

    def test_decoded_text(self) -> None:
        fr = proto.packet_to_from_radio(self._text_packet(), channel_index=0)
        self.assertEqual(fr.WhichOneof("payload_variant"), "packet")
        mp = fr.packet
        self.assertEqual(getattr(mp, "from"), 0xA1B2C3D4)
        self.assertEqual(mp.to, 0xFFFFFFFF)
        self.assertEqual(mp.id, 0x0000ABCD)
        self.assertEqual(mp.decoded.portnum, portnums_pb2.PortNum.Value("TEXT_MESSAGE_APP"))
        self.assertEqual(mp.decoded.payload, b"hi")
        self.assertEqual(mp.rx_rssi, -70)

    def test_request_id_preserved_for_acks(self) -> None:
        pkt = self._text_packet(
            packet_type=PacketType.ROUTING,
            decoded_payload={"request_id": 0xDEAD},
            raw_app_payload=b"",
        )
        fr = proto.packet_to_from_radio(pkt)
        self.assertEqual(fr.packet.decoded.request_id, 0xDEAD)
        routing = mesh_pb2.Routing()
        routing.ParseFromString(fr.packet.decoded.payload)
        self.assertEqual(routing.error_reason, mesh_pb2.Routing.Error.Value("NONE"))

    def test_build_routing_ack(self) -> None:
        fr = proto.build_routing_ack(
            packet_id=0xEACDE3D6,
            from_node=0xDE3ED0F6,
            to_node=0x890574FE,
            channel=0,
        )
        self.assertEqual(fr.WhichOneof("payload_variant"), "packet")
        mp = fr.packet
        self.assertEqual(mp.id, 0xEACDE3D6)
        self.assertEqual(getattr(mp, "from"), 0xDE3ED0F6)
        self.assertEqual(mp.to, 0x890574FE)
        self.assertEqual(mp.decoded.portnum, portnums_pb2.PortNum.Value("ROUTING_APP"))
        self.assertEqual(mp.decoded.request_id, 0xEACDE3D6)
        routing = mesh_pb2.Routing()
        routing.ParseFromString(mp.decoded.payload)
        self.assertEqual(routing.error_reason, mesh_pb2.Routing.Error.Value("NONE"))

    def test_encrypted_variant(self) -> None:
        pkt = self._text_packet(
            packet_type=PacketType.ENCRYPTED, decrypted=False,
            decoded_payload=None, raw_app_payload=None,
            encrypted_payload=b"\xde\xad\xbe\xef", channel_hash=0x23,
        )
        fr = proto.packet_to_from_radio(pkt, forward_encrypted=True)
        self.assertEqual(fr.packet.encrypted, b"\xde\xad\xbe\xef")
        self.assertEqual(fr.packet.channel, 0x23)

    def test_encrypted_dropped_when_forwarding_disabled(self) -> None:
        pkt = self._text_packet(
            packet_type=PacketType.ENCRYPTED, decrypted=False,
            decoded_payload=None, raw_app_payload=None,
            encrypted_payload=b"\xde\xad",
        )
        self.assertIsNone(
            proto.packet_to_from_radio(pkt, forward_encrypted=False)
        )


class TestQueueStatus(unittest.TestCase):
    def test_success_is_zero(self) -> None:
        fr = proto.build_queue_status(packet_id=7, success=True)
        self.assertEqual(fr.queueStatus.res, 0)
        self.assertEqual(fr.queueStatus.mesh_packet_id, 7)

    def test_failure_is_nonzero(self) -> None:
        fr = proto.build_queue_status(packet_id=7, success=False)
        self.assertNotEqual(fr.queueStatus.res, 0)


if __name__ == "__main__":
    unittest.main()
