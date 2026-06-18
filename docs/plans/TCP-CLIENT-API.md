# Meshtastic TCP Client API (port 4403)

**Status:** experimental, off by default (`tcp_api.enabled: false`).

Lets the official Meshtastic phone apps (iOS / Android) connect to a
Meshpoint directly over the local network and use it as a node —
channels, direct messages, the node list, and the map — exactly as they
would talk to a physical Meshtastic device over WiFi.

This is the same "stream API" the apps already speak to firmware over
TCP, so no app changes are needed: point the app at `<pi-ip>:4403` (or
let it discover the Meshpoint over mDNS) and it connects.

---

## What you get

| Meshtastic app feature | How it is served |
|---|---|
| **Channels** | The configured primary + secondary channels (with PSKs) are sent in the `want_config` handshake; channel messages stream in live. |
| **Direct messages** | DMs the Meshpoint receives are forwarded; DMs you send are transmitted to the target node via the existing TX path. |
| **Node list** | The Meshpoint's node DB is sent as `NodeInfo` on connect, then kept live by the streamed `NODEINFO`/`POSITION` packets. |
| **Map** | Node positions (`POSITION` packets + stored lat/lon) populate the app map. |
| **Local discovery** | The node advertises itself over mDNS (`_meshtastic._tcp`) so the app finds it on the LAN automatically. |

Sending from the app requires `transmit.enabled` (the app's outbound
packets are transmitted through the onboard SX1302). With transmit
disabled the app still connects **read-only**: you see everything the
Meshpoint hears, but sends are rejected.

---

## Design — a thin protocol adapter

The Meshpoint already captures, decodes, decrypts, stores, and transmits
Meshtastic traffic. The TCP API does **not** reimplement any of that. It
is a protocol shim that:

* **RX:** subscribes to the existing decode pipeline
  (`PipelineCoordinator.on_packet`) and reframes each decoded `Packet`
  as a Meshtastic `FromRadio{packet}`.
* **TX:** unpacks the app's `ToRadio{packet}` and routes text through the
  existing `TxService.send_text(...)`, inheriting its channel
  resolution, encryption, duty-cycle gating, and packet-id allocation.
* **State:** builds the `want_config` reply from the existing device
  identity, node repository, channel config, and radio config.

Because RX decode and TX transmit are reused verbatim, this feature adds
**no new packet-parsing or radio behaviour** (the two CONTRIBUTING
"high review" areas) — it only adds a new *consumer* of the RX stream
and a new *caller* of the existing send path.

### Module layout (`src/tcpapi/`)

```
src/tcpapi/
  framing.py     # 0x94 0xC3 + uint16 length stream framing (pure, no deps)
  mappings.py    # PacketType/region/preset/hw/role -> protobuf enums
  protocol.py    # build FromRadio (want_config + packets), parse ToRadio
  server.py      # asyncio TCP server, per-client lifecycle, fan-out, TX
  discovery.py   # optional mDNS (_meshtastic._tcp) advertisement
```

It is wired into the app only in `src/api/server.py`'s lifespan, guarded
by `tcp_api.enabled`, and started after the TX service is built /
stopped before the pipeline shuts down.

### The stream protocol

Framing (all transports): `0x94 0xC3 <len_hi> <len_lo> <protobuf>`.

Handshake: the app sends `ToRadio{want_config_id}`; the Meshpoint replies
with `my_info` → self `node_info` → peer `node_info`s → `channel`s →
`config`s (LoRa + device populated) → `moduleConfig`s → `metadata` →
`config_complete_id`. Afterwards `FromRadio{packet}` streams live and
`ToRadio{packet}` / `ToRadio{heartbeat}` are accepted.

---

## Configuration

```yaml
tcp_api:
  enabled: false        # opt-in; trusted LAN only (the protocol has no auth)
  host: "0.0.0.0"
  port: 4403            # Meshtastic default TCP port
  mdns: true            # advertise over _meshtastic._tcp for app auto-discovery
  forward_encrypted: true   # also relay packets the Meshpoint couldn't decrypt
  max_nodes: 200        # ceiling on the initial node_info burst
```

mDNS needs the optional `zeroconf` package (in `requirements.txt`); if it
is missing the server still runs and is reachable by IP.

---

## Limitations (current)

* **Text only on TX.** The app can send channel + DM text. Other portnums
  the app might send (position, telemetry, waypoints) are acknowledged as
  rejected for now; a generic portnum send path is future work.
* **No protocol auth.** The Meshtastic stream protocol is unauthenticated.
  Keep this on a trusted LAN; it is off by default.
* **Delivery state.** Sends are acknowledged with a `queueStatus`; for DMs
  the over-the-air routing ACK is forwarded so the app can mark delivered.
* **Channel ordering.** Channel indexes advertised to the app match
  `TxService`'s channel resolution order, so an index the app picks maps
  to the same PSK on transmit.

---

## Testing

* `tests/test_tcpapi_framing.py` — stream framing (partial reads, multiple
  frames, resync, oversize).
* `tests/test_tcpapi_protocol.py` — `want_config` sequence, channel/node
  field mapping, `FromRadio{packet}` reconstruction, canonical portnums.
* `tests/test_tcpapi_config.py` — config defaults + YAML merge.
* `tests/test_tcpapi_server.py` — full handshake + send/receive round trip
  over a real socket; an opt-in interop test
  (`MESHPOINT_TCP_INTEROP=1`) drives the real `meshtastic.TCPInterface`.

No RF hardware is required for any of these.
