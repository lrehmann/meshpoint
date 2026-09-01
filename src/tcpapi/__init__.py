"""Meshtastic client TCP API ("stream API") server.

Exposes the Meshpoint as a Meshtastic node over TCP (default port 4403)
so the official Meshtastic phone apps can connect over the LAN and use
it for channels, direct messages, the node list, and the map. This is a
thin protocol adapter: received traffic comes from the existing
capture/decode pipeline (``PipelineCoordinator.on_packet``) and outbound
messages are transmitted through the existing ``TxService``. No RF or
packet-parsing behaviour is reimplemented here.

The package is deliberately self-contained and is only wired into the
app lifecycle when ``tcp_api.enabled`` is set in config. Submodules are
imported lazily by the lifespan wiring so that importing the rest of the
app never requires the runtime-only ``meshtastic`` protobuf package.
"""
