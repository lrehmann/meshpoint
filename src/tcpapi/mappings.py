"""Mappings between Meshpoint's internal models and Meshtastic protobuf enums.

Values are resolved from the installed ``meshtastic`` protobufs by name
(authoritative) rather than hard-coded, so they cannot drift from the
protocol the connected app expects.
"""

from __future__ import annotations

import logging

from meshtastic.protobuf import config_pb2, mesh_pb2, portnums_pb2

from src.models.packet import PacketType

logger = logging.getLogger(__name__)

# Meshpoint PacketType -> Meshtastic PortNum enum *name*. Resolved to ints
# below. These are the canonical portnums (they match
# src/decode/portnum_handlers.py and meshtastic.protobuf.portnums_pb2).
_PACKET_TYPE_TO_PORTNUM_NAME: dict[PacketType, str] = {
    PacketType.TEXT: "TEXT_MESSAGE_APP",
    PacketType.POSITION: "POSITION_APP",
    PacketType.NODEINFO: "NODEINFO_APP",
    PacketType.ROUTING: "ROUTING_APP",
    PacketType.ADMIN: "ADMIN_APP",
    PacketType.WAYPOINT: "WAYPOINT_APP",
    PacketType.DETECTION_SENSOR: "DETECTION_SENSOR_APP",
    PacketType.PAXCOUNTER: "PAXCOUNTER_APP",
    PacketType.STORE_FORWARD: "STORE_FORWARD_APP",
    PacketType.RANGE_TEST: "RANGE_TEST_APP",
    PacketType.TELEMETRY: "TELEMETRY_APP",
    PacketType.TRACEROUTE: "TRACEROUTE_APP",
    PacketType.NEIGHBORINFO: "NEIGHBORINFO_APP",
    PacketType.MAP_REPORT: "MAP_REPORT_APP",
}


def _build_portnum_map() -> dict[PacketType, int]:
    out: dict[PacketType, int] = {}
    for ptype, name in _PACKET_TYPE_TO_PORTNUM_NAME.items():
        try:
            out[ptype] = portnums_pb2.PortNum.Value(name)
        except ValueError:  # pragma: no cover - enum name missing in this build
            logger.debug("PortNum name %s not in this meshtastic build", name)
    return out


PACKET_TYPE_TO_PORTNUM: dict[PacketType, int] = _build_portnum_map()

TEXT_MESSAGE_APP: int = portnums_pb2.PortNum.Value("TEXT_MESSAGE_APP")

# Meshtastic region names match Meshpoint's radio.region values 1:1 today
# (US, EU_868, ANZ, IN, KR, SG_923) but the lookup is name-based with a
# graceful fallback so an unknown region just yields UNSET.
_REGION_UNSET = config_pb2.Config.LoRaConfig.RegionCode.Value("UNSET")


def portnum_for_packet_type(ptype: PacketType, fallback: int = 0) -> int:
    """Return the Meshtastic portnum int for a Meshpoint ``PacketType``."""
    return PACKET_TYPE_TO_PORTNUM.get(ptype, fallback)


def region_code(region: str | None) -> int:
    """Map a config region string to a ``LoRaConfig.RegionCode`` value."""
    if not region:
        return _REGION_UNSET
    try:
        return config_pb2.Config.LoRaConfig.RegionCode.Value(region)
    except ValueError:
        logger.debug("Unknown region %r; reporting UNSET to client", region)
        return _REGION_UNSET


# (spreading_factor, bandwidth_khz) -> ModemPreset enum name. Mirrors the
# preset table in src/transmit/tx_service.py:PRESET_DISPLAY_NAMES.
_PRESET_BY_SF_BW: dict[tuple[int, int], str] = {
    (7, 250): "SHORT_FAST",
    (7, 500): "SHORT_TURBO",
    (8, 250): "SHORT_SLOW",
    (9, 250): "MEDIUM_FAST",
    (10, 250): "MEDIUM_SLOW",
    (11, 250): "LONG_FAST",
    (11, 125): "LONG_MODERATE",
    (12, 125): "LONG_SLOW",
}


def modem_preset(spreading_factor: int, bandwidth_khz: float) -> int | None:
    """Map SF/bandwidth to a ``LoRaConfig.ModemPreset`` value, or None.

    None means the radio is on a custom slot with no standard preset; the
    caller should set ``use_preset = False`` and send explicit parameters.
    """
    name = _PRESET_BY_SF_BW.get((int(spreading_factor), int(bandwidth_khz)))
    if name is None:
        return None
    try:
        return config_pb2.Config.LoRaConfig.ModemPreset.Value(name)
    except ValueError:  # pragma: no cover
        return None


def hw_model_int(value: object, default: int = 0) -> int:
    """Coerce a node's stored hardware_model into a HardwareModel int.

    The node DB stores ``str(enum_int)`` (e.g. "31") and many rows are
    NULL, so be defensive: accept ints, numeric strings, or enum names.
    """
    if value is None:
        return default
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return mesh_pb2.HardwareModel.Value(text)
    except ValueError:
        return default


def role_int(value: object, default: int = 0) -> int:
    """Coerce a node's stored role into a DeviceConfig.Role int."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return config_pb2.Config.DeviceConfig.Role.Value(text)
    except ValueError:
        return default
