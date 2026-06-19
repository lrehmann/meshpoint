"""Build Meshtastic ``FromRadio`` messages and parse ``ToRadio`` messages.

This is the protocol layer: it turns Meshpoint's internal data (device
identity, the node DB, channel config, decoded packets) into the
protobuf messages the Meshtastic apps expect, and decodes the messages
they send back. It performs no I/O and no decryption -- decoded packets
arrive already processed by the capture/decode pipeline.

``meshtastic`` protobufs are imported at module top: this module is only
imported lazily (from the server, which is only built when
``tcp_api.enabled``), so importing the rest of the app never requires
the runtime-only ``meshtastic`` package.
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from meshtastic.protobuf import (
    channel_pb2,
    config_pb2,
    mesh_pb2,
    module_config_pb2,
)

from src.models.packet import Packet, PacketType
from src.tcpapi.mappings import (
    hw_model_int,
    modem_preset,
    portnum_for_packet_type,
    region_code,
    role_int,
)

logger = logging.getLogger(__name__)

BROADCAST_ADDR = 0xFFFFFFFF
# Reported to the app as this node's hardware. PRIVATE_HW marks a
# non-standard device, matching what the Meshpoint broadcasts over RF in
# its own NodeInfo (src/transmit/tx_service.py:HW_MODEL_PRIVATE_HW).
HW_MODEL_PRIVATE_HW = 255
_ROLE_CLIENT = config_pb2.Config.DeviceConfig.Role.Value("CLIENT")
_MIN_APP_VERSION = 30200
_CLIENT_APP_MIN_VERSION = (2, 5, 18)
_CLIENT_APP_COMPAT_FIRMWARE_VERSION = "2.6.0.meshpoint"

# Config oneof variants sent with default values during want_config so the
# app's config download completes cleanly (device + lora carry real data).
_DEFAULT_CONFIG_VARIANTS = (
    "position",
    "power",
    "network",
    "display",
    "bluetooth",
)


@dataclass
class ChannelDef:
    """One Meshtastic channel slot advertised to the client."""

    index: int
    name: str
    psk: bytes
    role: int  # channel_pb2.Channel.Role value


def channel_defs(
    primary_name: str,
    default_key_b64: str,
    channel_keys: dict[str, str],
) -> list[ChannelDef]:
    """Build the ordered channel list from Meshtastic config.

    Index 0 is the primary channel (``meshtastic.primary_channel_name`` +
    ``default_key_b64``); indices 1..N follow the insertion order of
    ``meshtastic.channel_keys``. This order matches
    ``TxService._resolve_channel`` so a channel index chosen by the phone
    resolves to the same PSK/hash on transmit.

    PSKs are the raw (base64-decoded) key bytes as configured -- the
    well-known 1-byte form (``0x01`` for the default ``"AQ=="``) is sent
    verbatim, which is what the apps expect for the default channel.
    """
    primary_role = channel_pb2.Channel.Role.Value("PRIMARY")
    secondary_role = channel_pb2.Channel.Role.Value("SECONDARY")

    defs = [
        ChannelDef(
            index=0,
            name=primary_name or "",
            psk=_decode_psk(default_key_b64),
            role=primary_role,
        )
    ]
    for offset, (name, key_b64) in enumerate(channel_keys.items(), start=1):
        defs.append(
            ChannelDef(
                index=offset,
                name=name,
                psk=_decode_psk(key_b64),
                role=secondary_role,
            )
        )
    return defs


def _decode_psk(key_b64: str) -> bytes:
    if not key_b64:
        return b""
    try:
        return base64.b64decode(key_b64)
    except (ValueError, TypeError):
        logger.warning("Invalid base64 PSK in channel config; sending empty key")
        return b""


# --------------------------------------------------------------------------
# want_config response builders
# --------------------------------------------------------------------------


def build_want_config_frames(
    *,
    my_node_num: int,
    long_name: str,
    short_name: str,
    public_key: Optional[bytes],
    device_id: Optional[str],
    latitude: Optional[float],
    longitude: Optional[float],
    altitude: Optional[float],
    firmware_version: str,
    nodes: list[dict[str, Any]],
    channels: list[ChannelDef],
    region: Optional[str],
    spreading_factor: int,
    bandwidth_khz: float,
    frequency_mhz: Optional[float],
    tx_enabled: bool,
    tx_power_dbm: int,
    hop_limit: int,
    want_config_id: int,
) -> list["mesh_pb2.FromRadio"]:
    """Build the full ordered ``FromRadio`` handshake for a want_config.

    Sequence: my_info -> self node_info -> peer node_infos -> channels ->
    configs -> module configs -> metadata -> config_complete_id.
    """
    frames: list[mesh_pb2.FromRadio] = []

    frames.append(_my_node_info(my_node_num, device_id))

    # This node first, then peers (skip a peer row that duplicates us).
    frames.append(
        _self_node_info(
            my_node_num, long_name, short_name, public_key,
            latitude, longitude, altitude,
        )
    )
    for node in nodes:
        fr = _peer_node_info(node, my_node_num)
        if fr is not None:
            frames.append(fr)

    for ch in channels:
        frames.append(_channel(ch))

    frames.extend(
        _configs(
            region=region,
            spreading_factor=spreading_factor,
            bandwidth_khz=bandwidth_khz,
            frequency_mhz=frequency_mhz,
            tx_enabled=tx_enabled,
            tx_power_dbm=tx_power_dbm,
            hop_limit=hop_limit,
        )
    )
    frames.extend(_module_configs())

    frames.append(_metadata(firmware_version, tx_enabled))

    complete = mesh_pb2.FromRadio()
    complete.config_complete_id = want_config_id
    frames.append(complete)

    return frames


def _my_node_info(my_node_num: int, device_id: Optional[str]) -> "mesh_pb2.FromRadio":
    info = mesh_pb2.MyNodeInfo()
    info.my_node_num = my_node_num & 0xFFFFFFFF
    info.min_app_version = _MIN_APP_VERSION
    if device_id:
        info.device_id = device_id.encode("utf-8", errors="replace")[:32]
    fr = mesh_pb2.FromRadio()
    fr.my_info.CopyFrom(info)
    return fr


def _self_node_info(
    my_node_num: int,
    long_name: str,
    short_name: str,
    public_key: Optional[bytes],
    latitude: Optional[float],
    longitude: Optional[float],
    altitude: Optional[float],
) -> "mesh_pb2.FromRadio":
    node = mesh_pb2.NodeInfo()
    node.num = my_node_num & 0xFFFFFFFF
    node.user.id = f"!{node.num:08x}"
    node.user.long_name = long_name or "Meshpoint"
    node.user.short_name = short_name or "MPNT"
    node.user.hw_model = HW_MODEL_PRIVATE_HW
    node.user.role = _ROLE_CLIENT
    if public_key and len(public_key) == 32:
        node.user.public_key = public_key
    _apply_position(node, latitude, longitude, altitude)
    fr = mesh_pb2.FromRadio()
    fr.node_info.CopyFrom(node)
    return fr


def _peer_node_info(
    node: dict[str, Any], my_node_num: int
) -> Optional["mesh_pb2.FromRadio"]:
    if node.get("protocol", "meshtastic") != "meshtastic":
        return None
    node_id = node.get("node_id")
    num = _hex_to_int(node_id, default=-1)
    if num < 0 or num == (my_node_num & 0xFFFFFFFF):
        return None

    info = mesh_pb2.NodeInfo()
    info.num = num
    info.user.id = f"!{num:08x}"
    if node.get("long_name"):
        info.user.long_name = str(node["long_name"])
    if node.get("short_name"):
        info.user.short_name = str(node["short_name"])
    info.user.hw_model = hw_model_int(node.get("hardware_model"))
    info.user.role = role_int(node.get("role"))
    pubkey = node.get("public_key")
    if pubkey:
        try:
            raw = bytes.fromhex(str(pubkey))
            if len(raw) == 32:
                info.user.public_key = raw
        except ValueError:
            pass

    _apply_position(
        info, node.get("latitude"), node.get("longitude"), node.get("altitude")
    )

    last_heard = _epoch_seconds(node.get("last_heard"))
    if last_heard:
        info.last_heard = last_heard

    snr = node.get("latest_snr")
    if snr is not None:
        info.snr = float(snr)

    hops = node.get("latest_hops")
    if isinstance(hops, int) and hops > 0:
        info.hops_away = hops

    _apply_device_metrics(info, node)

    fr = mesh_pb2.FromRadio()
    fr.node_info.CopyFrom(info)
    return fr


def _apply_position(
    node: "mesh_pb2.NodeInfo",
    latitude: Optional[float],
    longitude: Optional[float],
    altitude: Optional[float],
) -> None:
    if latitude is None or longitude is None:
        return
    try:
        node.position.latitude_i = int(float(latitude) * 1e7)
        node.position.longitude_i = int(float(longitude) * 1e7)
        if altitude is not None:
            node.position.altitude = int(float(altitude))
    except (ValueError, TypeError):
        pass


def _apply_device_metrics(info: "mesh_pb2.NodeInfo", node: dict[str, Any]) -> None:
    battery = node.get("latest_battery")
    voltage = node.get("latest_voltage")
    chan_util = node.get("latest_channel_util")
    air_util = node.get("latest_air_util")
    if battery is None and voltage is None and chan_util is None and air_util is None:
        return
    if battery is not None:
        info.device_metrics.battery_level = int(battery)
    if voltage is not None:
        info.device_metrics.voltage = float(voltage)
    if chan_util is not None:
        info.device_metrics.channel_utilization = float(chan_util)
    if air_util is not None:
        info.device_metrics.air_util_tx = float(air_util)


def _channel(ch: ChannelDef) -> "mesh_pb2.FromRadio":
    channel = channel_pb2.Channel()
    channel.index = ch.index
    channel.role = ch.role
    if ch.psk:
        channel.settings.psk = ch.psk
    if ch.name:
        channel.settings.name = ch.name
    fr = mesh_pb2.FromRadio()
    fr.channel.CopyFrom(channel)
    return fr


def _configs(
    *,
    region: Optional[str],
    spreading_factor: int,
    bandwidth_khz: float,
    frequency_mhz: Optional[float],
    tx_enabled: bool,
    tx_power_dbm: int,
    hop_limit: int,
) -> list["mesh_pb2.FromRadio"]:
    out: list[mesh_pb2.FromRadio] = []

    device = config_pb2.Config.DeviceConfig()
    device.role = _ROLE_CLIENT
    out.append(_config_frame("device", device))

    lora = config_pb2.Config.LoRaConfig()
    lora.region = region_code(region)
    preset = modem_preset(spreading_factor, bandwidth_khz)
    if preset is not None:
        lora.use_preset = True
        lora.modem_preset = preset
    else:
        lora.use_preset = False
        lora.spread_factor = int(spreading_factor)
        lora.bandwidth = int(bandwidth_khz)
    lora.hop_limit = max(0, min(7, int(hop_limit)))
    lora.tx_enabled = bool(tx_enabled)
    lora.tx_power = int(tx_power_dbm)
    if frequency_mhz is not None:
        lora.override_frequency = float(frequency_mhz)
    out.append(_config_frame("lora", lora))

    for name in _DEFAULT_CONFIG_VARIANTS:
        cfg = config_pb2.Config()
        getattr(cfg, name).SetInParent()
        fr = mesh_pb2.FromRadio()
        fr.config.CopyFrom(cfg)
        out.append(fr)

    return out


def _config_frame(variant: str, message) -> "mesh_pb2.FromRadio":
    cfg = config_pb2.Config()
    getattr(cfg, variant).CopyFrom(message)
    fr = mesh_pb2.FromRadio()
    fr.config.CopyFrom(cfg)
    return fr


def _module_configs() -> list["mesh_pb2.FromRadio"]:
    out: list[mesh_pb2.FromRadio] = []
    for field in module_config_pb2.ModuleConfig.DESCRIPTOR.oneofs_by_name[
        "payload_variant"
    ].fields:
        mc = module_config_pb2.ModuleConfig()
        getattr(mc, field.name).SetInParent()
        fr = mesh_pb2.FromRadio()
        fr.moduleConfig.CopyFrom(mc)
        out.append(fr)
    return out


def _metadata(firmware_version: str, tx_enabled: bool) -> "mesh_pb2.FromRadio":
    meta = mesh_pb2.DeviceMetadata()
    meta.firmware_version = _client_firmware_version(firmware_version)
    meta.hw_model = HW_MODEL_PRIVATE_HW
    meta.role = _ROLE_CLIENT
    meta.hasWifi = True
    meta.hasEthernet = True
    meta.hasBluetooth = False
    meta.canShutdown = False
    fr = mesh_pb2.FromRadio()
    fr.metadata.CopyFrom(meta)
    return fr


def _client_firmware_version(firmware_version: str) -> str:
    """Return a firmware string Meshtastic apps accept during version checks.

    The Apple app treats the last dotted component as a build/hash suffix, then
    compares the remaining version against its minimum supported firmware. A
    Meshpoint version such as ``0.7.6`` is valid for Meshpoint but makes the app
    reject the TCP endpoint as old firmware, so advertise API compatibility here
    without changing Meshpoint's own version elsewhere.
    """
    raw = (firmware_version or "").strip()
    parts = raw.split(".")
    try:
        version = tuple(int(part) for part in parts[:3])
    except ValueError:
        return _CLIENT_APP_COMPAT_FIRMWARE_VERSION

    if len(version) != 3 or version < _CLIENT_APP_MIN_VERSION:
        return _CLIENT_APP_COMPAT_FIRMWARE_VERSION
    if len(parts) == 3:
        return f"{raw}.meshpoint"
    return raw


# --------------------------------------------------------------------------
# RX packet -> FromRadio{packet}
# --------------------------------------------------------------------------


def packet_to_from_radio(
    packet: Packet,
    *,
    channel_index: int = 0,
    forward_encrypted: bool = True,
) -> Optional["mesh_pb2.FromRadio"]:
    """Reconstruct a ``FromRadio{packet}`` from a decoded Meshpoint packet.

    Decoded packets are forwarded with their ``decoded`` Data variant
    (portnum + the byte-exact inner payload); undecryptable packets are
    forwarded as the ``encrypted`` variant when ``forward_encrypted`` is
    set, so a phone holding the PSK can decode them itself. Returns None
    when neither variant can be produced.
    """
    mp = mesh_pb2.MeshPacket()
    mp.id = _hex_to_int(packet.packet_id)
    setattr(mp, "from", _hex_to_int(packet.source_id))
    mp.to = _hex_to_int(packet.destination_id, default=BROADCAST_ADDR)
    mp.hop_limit = packet.hop_limit
    mp.hop_start = packet.hop_start
    mp.want_ack = packet.want_ack
    mp.via_mqtt = packet.via_mqtt
    if packet.signal:
        if packet.signal.rssi is not None:
            mp.rx_rssi = int(packet.signal.rssi)
        if packet.signal.snr is not None:
            mp.rx_snr = float(packet.signal.snr)
    if packet.timestamp is not None:
        mp.rx_time = int(packet.timestamp.timestamp())

    if packet.decrypted and packet.raw_app_payload is not None:
        portnum = _resolve_portnum(packet)
        if portnum is None:
            return _maybe_encrypted(mp, packet, forward_encrypted)
        mp.channel = channel_index
        mp.decoded.portnum = portnum
        mp.decoded.payload = packet.raw_app_payload
        request_id = (packet.decoded_payload or {}).get("request_id")
        if request_id:
            try:
                mp.decoded.request_id = int(request_id) & 0xFFFFFFFF
            except (ValueError, TypeError):
                pass
        fr = mesh_pb2.FromRadio()
        fr.packet.CopyFrom(mp)
        return fr

    return _maybe_encrypted(mp, packet, forward_encrypted)


def _maybe_encrypted(
    mp: "mesh_pb2.MeshPacket", packet: Packet, forward_encrypted: bool
) -> Optional["mesh_pb2.FromRadio"]:
    if not forward_encrypted or not packet.encrypted_payload:
        return None
    mp.channel = packet.channel_hash
    mp.encrypted = packet.encrypted_payload
    fr = mesh_pb2.FromRadio()
    fr.packet.CopyFrom(mp)
    return fr


def _resolve_portnum(packet: Packet) -> Optional[int]:
    if packet.packet_type != PacketType.UNKNOWN:
        mapped = portnum_for_packet_type(packet.packet_type, fallback=-1)
        if mapped >= 0:
            return mapped
    val = (packet.decoded_payload or {}).get("portnum")
    if isinstance(val, int):
        return val
    return None


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def build_queue_status(
    *, packet_id: int, success: bool, free: int = 16, maxlen: int = 16
) -> "mesh_pb2.FromRadio":
    """A ``FromRadio{queueStatus}`` acknowledging an outbound packet.

    ``res`` is a Routing error code; 0 (NONE) means accepted. A non-zero
    value tells the app the send was rejected (e.g. TX disabled or duty
    cycle exhausted) so it can surface the failure.
    """
    status = mesh_pb2.QueueStatus()
    status.res = 0 if success else _routing_error_no_response()
    status.free = free
    status.maxlen = maxlen
    status.mesh_packet_id = packet_id & 0xFFFFFFFF
    fr = mesh_pb2.FromRadio()
    fr.queueStatus.CopyFrom(status)
    return fr


def _routing_error_no_response() -> int:
    try:
        return mesh_pb2.Routing.Error.Value("NO_RESPONSE")
    except ValueError:  # pragma: no cover
        return 1


def _hex_to_int(value: Optional[str], default: int = 0) -> int:
    if not value:
        return default
    text = value[1:] if value.startswith("!") else value
    try:
        return int(text, 16) & 0xFFFFFFFF
    except (ValueError, TypeError):
        return default


def _epoch_seconds(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        return int(datetime.fromisoformat(str(value)).timestamp())
    except (ValueError, TypeError):
        return 0
