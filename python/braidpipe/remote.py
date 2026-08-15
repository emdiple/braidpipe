"""The tcp-raw transport: attach to a braidpipe daemon on another machine.

Mirrors the shm handshake in shape: say hello, get a config packet back.
Here the hello goes over UDP to the daemon's --worker-listen address, the
config carries a TCP port instead of a file descriptor, and frames flow both
ways on one TCP connection as a fixed 24-byte header plus raw pixel bytes.

The frames are uncompressed, so this needs a fast link: 720p RGB at 30 fps is
~660 Mbit/s each way. Use it on a 10 GbE LAN (or 720p on a quiet gigabit
link), not across the internet.
"""

import json
import socket
import struct
import time

import numpy as np

# Wire header matching Rust net.rs, 24 bytes little-endian:
# frame_id u64, time_us u64, payload_len u32, slot u8, flags u8, 2 pad.
# time_us carries the capture timestamp daemon -> worker and this worker's
# processing time on the way back. Bit 0 of flags is "success" on results.
WIRE_HEADER_FMT = "<QQIBB2x"
WIRE_HEADER_SIZE = struct.calcsize(WIRE_HEADER_FMT)
assert WIRE_HEADER_SIZE == 24

HELLO = b'{"type":"hello","transports":["tcp-raw"]}'


def connect(daemon: str, retry_interval: float = 1.0) -> "RemoteWorkerLink":
    """Negotiates with the daemon at "host:port" until a data connection is up.

    Retries forever, so a worker may start before the daemon and simply wait
    for it -- the same contract the shm attach gives a local worker.
    """
    host, _, port_text = daemon.rpartition(":")
    address = (host, int(port_text))

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(retry_interval)
    try:
        while True:
            try:
                udp.sendto(HELLO, address)
                reply, _ = udp.recvfrom(512)
            except (TimeoutError, OSError):
                time.sleep(retry_interval)
                continue
            config = json.loads(reply)
            if config.get("type") == "config" and config.get("transport") == "tcp-raw":
                break
            raise RuntimeError(f"daemon refused tcp-raw: {config}")
    finally:
        udp.close()

    tcp = socket.create_connection((host, config["data_port"]))
    tcp.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return RemoteWorkerLink(tcp, config)


class RemoteWorkerLink:
    def __init__(self, sock: socket.socket, config: dict):
        self.sock = sock
        self.width = config["width"]
        self.height = config["height"]
        self.channels = config["channels"]
        self._payload_len = self.width * self.height * self.channels
        # One reusable buffer: frames are processed in place and sent back
        # from the same bytes, so no per-frame allocation happens.
        self._buf = bytearray(self._payload_len)
        self._frame_view = np.frombuffer(self._buf, dtype=np.uint8).reshape(
            self.height, self.width, self.channels
        )

    def frames(self):
        """Yields (frame_id, slot, timestamp_us, frame) until the daemon hangs up.

        `frame` is a NumPy view over an internal buffer that is reused for the
        next frame -- process it (in place is fine) and call `send_processed`
        before advancing the loop; copy it if you need to keep it longer.
        """
        header_buf = bytearray(WIRE_HEADER_SIZE)
        frame = self._frame_view
        while True:
            if not self._recv_exact(header_buf):
                return
            frame_id, timestamp_us, payload_len, slot, _flags = struct.unpack(
                WIRE_HEADER_FMT, header_buf
            )
            if payload_len != self._payload_len:
                raise RuntimeError(
                    f"daemon sent {payload_len} payload bytes, expected {self._payload_len}"
                )
            if not self._recv_exact(self._buf):
                return
            yield frame_id, slot, timestamp_us, frame

    def send_processed(
        self,
        frame_id: int,
        slot: int,
        frame: np.ndarray,
        processing_time_us: int,
        success: bool = True,
    ) -> None:
        """Returns a result to the daemon; this doubles as the frame's ack."""
        header = struct.pack(
            WIRE_HEADER_FMT,
            frame_id,
            processing_time_us,
            self._payload_len,
            slot,
            1 if success else 0,
        )
        self.sock.sendall(header)
        if np.shares_memory(frame, self._frame_view):
            # The frame is (a view of) the internal buffer; send it as-is.
            self.sock.sendall(self._buf)
        else:
            self.sock.sendall(np.ascontiguousarray(frame).tobytes())

    def _recv_exact(self, buf) -> bool:
        """Fills `buf` completely, or returns False on a closed connection."""
        view = memoryview(buf)
        while view:
            received = self.sock.recv_into(view, len(view))
            if received == 0:
                return False
            view = view[received:]
        return True

    def close(self):
        self.sock.close()
