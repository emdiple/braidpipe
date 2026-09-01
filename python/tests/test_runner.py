"""SDK tests against a fake daemon: pure Python, no Rust or GStreamer needed.

Each test stands up the daemon side of one transport — the UDS handshake with a
real fd for shared memory, a UDP-hello/TCP-data pair for tcp-raw — and runs
`braidpipe.run()` against it, asserting the contract the Rust daemon relies on:
pixels mutated in place, slots freed, every frame acked, and an exception in
the handler reported as `"success": false` rather than a dead worker.

Run with:  python3 -m unittest discover python/tests
"""

import array
import json
import os
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import braidpipe
from braidpipe.runner import _wants_context
from braidpipe.shm import HEADER_FMT, HEADER_SIZE, SLOT_HEADER_FMT, SLOT_HEADER_SIZE

WIDTH, HEIGHT, CHANNELS, SLOT_COUNT = 8, 4, 3, 2
PAYLOAD = WIDTH * HEIGHT * CHANNELS
SLOT_SIZE = SLOT_HEADER_SIZE + PAYLOAD

TIMEOUT = 5.0


def make_segment():
    """A real mmap-able fd laid out exactly like the Rust daemon's segment."""
    f = tempfile.TemporaryFile()
    f.truncate(HEADER_SIZE + SLOT_COUNT * SLOT_SIZE)
    f.seek(0)
    f.write(struct.pack(HEADER_FMT, WIDTH, HEIGHT, CHANNELS, SLOT_COUNT, SLOT_SIZE, b""))
    f.flush()
    return f


class ShmTransportTest(unittest.TestCase):
    def test_frames_processed_freed_and_acked(self):
        tmp = tempfile.mkdtemp()
        rust_sock_path = os.path.join(tmp, "rust.sock")
        python_sock_path = os.path.join(tmp, "python.sock")

        daemon = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        daemon.bind(rust_sock_path)
        daemon.settimeout(TIMEOUT)
        self.addCleanup(daemon.close)

        segment = make_segment()
        self.addCleanup(segment.close)
        view = np.memmap(segment, dtype=np.uint8, mode="r+")

        seen = []

        def process(frame, ctx):
            seen.append((ctx.frame_id, ctx.timestamp_us, ctx.transport))
            if ctx.frame_id == 2:
                raise RuntimeError("deliberate failure")
            frame += 1

        worker = threading.Thread(
            target=braidpipe.run,
            kwargs=dict(
                process=process,
                rust_sock=rust_sock_path,
                python_sock=python_sock_path,
                name="test",
            ),
            daemon=True,  # left blocked in recvfrom once the test is over
        )
        worker.start()

        # Handshake: the worker's hello is answered with the segment's fd.
        # (sendmsg directly: socket.send_fds does not forward `address`.)
        _, worker_addr = daemon.recvfrom(512)
        daemon.sendmsg(
            [b'{"type":"config"}'],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [segment.fileno()]))],
            0,
            worker_addr,
        )

        def send_frame(frame_id, slot, fill):
            offset = HEADER_SIZE + slot * SLOT_SIZE
            view[offset : offset + SLOT_HEADER_SIZE] = np.frombuffer(
                struct.pack(SLOT_HEADER_FMT, 1, frame_id, 1_000_000 + frame_id),
                dtype=np.uint8,
            )
            view[offset + SLOT_HEADER_SIZE : offset + SLOT_SIZE] = fill
            daemon.sendto(
                json.dumps({"frame_id": frame_id, "slot_index": slot}).encode(),
                python_sock_path,
            )
            ack = json.loads(daemon.recvfrom(512)[0])
            return offset, ack

        # A control packet with no frame_id must be skipped, not crash the loop.
        daemon.sendto(b'{"type":"noise"}', python_sock_path)

        offset, ack = send_frame(1, 0, fill=10)
        self.assertEqual(ack["frame_id"], 1)
        self.assertTrue(ack["success"])
        self.assertEqual(view[offset], 0)  # slot freed
        self.assertTrue((view[offset + SLOT_HEADER_SIZE : offset + SLOT_SIZE] == 11).all())

        # A raising handler acks success=false and leaves the loop alive.
        offset, ack = send_frame(2, 1, fill=20)
        self.assertFalse(ack["success"])
        self.assertEqual(view[offset], 0)
        self.assertTrue((view[offset + SLOT_HEADER_SIZE : offset + SLOT_SIZE] == 20).all())

        _, ack = send_frame(3, 0, fill=30)
        self.assertTrue(ack["success"])

        self.assertEqual(
            seen,
            [(1, 1_000_001, "shm"), (2, 1_000_002, "shm"), (3, 1_000_003, "shm")],
        )


class TcpRawTransportTest(unittest.TestCase):
    def test_frames_round_trip_and_clean_shutdown(self):
        from braidpipe.remote import WIRE_HEADER_FMT, WIRE_HEADER_SIZE

        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.bind(("127.0.0.1", 0))
        udp.settimeout(TIMEOUT)
        self.addCleanup(udp.close)

        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.bind(("127.0.0.1", 0))
        tcp.listen(1)
        tcp.settimeout(TIMEOUT)
        self.addCleanup(tcp.close)

        def process(frame):
            frame += 1

        worker = threading.Thread(
            target=braidpipe.run,
            kwargs=dict(
                process=process,
                daemon=f"127.0.0.1:{udp.getsockname()[1]}",
                name="test",
            ),
            daemon=True,  # a failed test must not hang the runner at exit
        )
        worker.start()

        hello, worker_addr = udp.recvfrom(512)
        self.assertIn("tcp-raw", json.loads(hello)["transports"])
        udp.sendto(
            json.dumps(
                {
                    "type": "config",
                    "transport": "tcp-raw",
                    "data_port": tcp.getsockname()[1],
                    "width": WIDTH,
                    "height": HEIGHT,
                    "channels": CHANNELS,
                }
            ).encode(),
            worker_addr,
        )

        conn, _ = tcp.accept()
        conn.settimeout(TIMEOUT)
        for frame_id in (1, 2):
            conn.sendall(
                struct.pack(WIRE_HEADER_FMT, frame_id, 5_000, PAYLOAD, 0, 0)
                + bytes([frame_id * 10]) * PAYLOAD
            )
            reply = b""
            while len(reply) < WIRE_HEADER_SIZE + PAYLOAD:
                reply += conn.recv(4096)
            reply_id, _, _, _, flags = struct.unpack(
                WIRE_HEADER_FMT, reply[:WIRE_HEADER_SIZE]
            )
            self.assertEqual(reply_id, frame_id)
            self.assertEqual(flags & 1, 1)
            self.assertEqual(set(reply[WIRE_HEADER_SIZE:]), {frame_id * 10 + 1})

        # Hanging up must end run() rather than strand the worker.
        conn.close()
        worker.join(TIMEOUT)
        self.assertFalse(worker.is_alive())


class SignatureTest(unittest.TestCase):
    def test_handler_arity_detection(self):
        self.assertFalse(_wants_context(lambda frame: None))
        self.assertTrue(_wants_context(lambda frame, ctx: None))
        self.assertTrue(_wants_context(lambda *args: None))

    def test_worker_decorator_registers_handler(self):
        @braidpipe.worker
        def handler(frame):
            pass

        from braidpipe import runner

        self.assertIs(runner._registered, handler)


if __name__ == "__main__":
    unittest.main()
