"""Tests for the Meshtastic stream framing (0x94 0xC3 + uint16 length)."""

from __future__ import annotations

import unittest

from src.tcpapi.framing import (
    HEADER_LEN,
    START1,
    START2,
    FrameAccumulator,
    encode_frame,
)


class TestEncodeFrame(unittest.TestCase):
    def test_header_and_length(self) -> None:
        frame = encode_frame(b"hello")
        self.assertEqual(frame[0], START1)
        self.assertEqual(frame[1], START2)
        self.assertEqual((frame[2] << 8) | frame[3], 5)
        self.assertEqual(frame[HEADER_LEN:], b"hello")

    def test_empty_payload(self) -> None:
        frame = encode_frame(b"")
        self.assertEqual(len(frame), HEADER_LEN)

    def test_oversized_payload_raises(self) -> None:
        with self.assertRaises(ValueError):
            encode_frame(b"x" * 70000)


class TestFrameAccumulator(unittest.TestCase):
    def test_single_frame(self) -> None:
        acc = FrameAccumulator()
        self.assertEqual(acc.feed(encode_frame(b"abc")), [b"abc"])

    def test_multiple_frames_in_one_read(self) -> None:
        acc = FrameAccumulator()
        data = encode_frame(b"one") + encode_frame(b"two")
        self.assertEqual(acc.feed(data), [b"one", b"two"])

    def test_frame_split_across_reads(self) -> None:
        acc = FrameAccumulator()
        frame = encode_frame(b"split-me")
        self.assertEqual(acc.feed(frame[:3]), [])
        self.assertEqual(acc.feed(frame[3:]), [b"split-me"])

    def test_byte_at_a_time(self) -> None:
        acc = FrameAccumulator()
        frame = encode_frame(b"drip")
        out: list[bytes] = []
        for b in frame:
            out.extend(acc.feed(bytes([b])))
        self.assertEqual(out, [b"drip"])

    def test_resync_after_leading_junk(self) -> None:
        acc = FrameAccumulator()
        data = b"\x00\x11garbage" + encode_frame(b"clean")
        self.assertEqual(acc.feed(data), [b"clean"])

    def test_false_magic_start1_without_start2(self) -> None:
        acc = FrameAccumulator()
        # A lone START1 not followed by START2 must be skipped.
        data = bytes([START1, 0x00]) + encode_frame(b"ok")
        self.assertEqual(acc.feed(data), [b"ok"])

    def test_oversized_length_is_resynced(self) -> None:
        acc = FrameAccumulator(max_frame_size=16)
        # Claims length 0xFFFF -> dropped, then a real frame recovers.
        bogus = bytes([START1, START2, 0xFF, 0xFF])
        data = bogus + encode_frame(b"recovered")
        self.assertEqual(acc.feed(data), [b"recovered"])


if __name__ == "__main__":
    unittest.main()
