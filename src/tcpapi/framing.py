"""Meshtastic stream-protocol framing.

The Meshtastic client API frames every protobuf message as::

    0x94 0xC3 <len_hi> <len_lo> <protobuf bytes>

(a 4-byte header: two magic bytes then a big-endian uint16 length),
identical across the serial, BLE, and TCP transports. This module has
no protobuf or socket dependencies so it can be unit-tested in
isolation.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

START1 = 0x94
START2 = 0xC3
HEADER_LEN = 4
# Matches meshtastic.stream_interface.MAX_TO_FROM_RADIO_SIZE. Frames
# claiming a larger length are treated as a desync and resynchronised.
MAX_FRAME_SIZE = 512


def encode_frame(payload: bytes) -> bytes:
    """Wrap serialized protobuf bytes in a stream frame header."""
    length = len(payload)
    if length > 0xFFFF:
        raise ValueError(f"frame too large to encode: {length} bytes")
    return bytes((START1, START2, (length >> 8) & 0xFF, length & 0xFF)) + payload


class FrameAccumulator:
    """Reassembles stream frames from arbitrarily-chunked TCP reads.

    Feed raw bytes with :meth:`feed`; it yields each complete frame's
    protobuf payload (header stripped). Handles frames split across
    reads, multiple frames in one read, and resynchronisation after
    junk/desync by rescanning for the ``0x94 0xC3`` magic.
    """

    def __init__(self, max_frame_size: int = MAX_FRAME_SIZE):
        self._buf = bytearray()
        self._max_frame_size = max_frame_size

    def feed(self, data: bytes) -> list[bytes]:
        """Append bytes and return any complete frame payloads."""
        self._buf.extend(data)
        frames: list[bytes] = []
        while True:
            frame = self._next_frame()
            if frame is None:
                break
            frames.append(frame)
        return frames

    def _next_frame(self) -> bytes | None:
        """Return the next complete frame, or None if more data is needed.

        Loops internally so a resync step (dropping junk or a false magic
        byte) keeps scanning the buffer rather than stalling until the
        next ``feed`` call.
        """
        buf = self._buf
        while True:
            # Resync: drop everything before the first START1.
            start = buf.find(START1)
            if start == -1:
                buf.clear()
                return None
            if start > 0:
                del buf[:start]

            if len(buf) < HEADER_LEN:
                return None

            if buf[1] != START2:
                # START1 not followed by START2 -- false magic, skip one
                # byte and keep scanning.
                del buf[:1]
                continue

            length = (buf[2] << 8) | buf[3]
            if length > self._max_frame_size:
                logger.debug(
                    "Oversized frame length %d (max %d); resyncing",
                    length,
                    self._max_frame_size,
                )
                del buf[:1]
                continue

            if len(buf) < HEADER_LEN + length:
                return None

            payload = bytes(buf[HEADER_LEN:HEADER_LEN + length])
            del buf[:HEADER_LEN + length]
            return payload
