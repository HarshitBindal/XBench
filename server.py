"""
server.py — XBench TCP and UDP echo servers.

Phase 2: Full echo server implementation.

Architecture
============
TCP (AF_INET + SOCK_STREAM)
  TCP is a *byte stream* — there are no built-in message boundaries.  A single
  recv() call may return fewer bytes than the message size (a "partial read"),
  or it may return bytes belonging to more than one message.  recv_exact() solves
  this by looping until exactly msg_size bytes have been collected before echoing.

  The server accepts clients one at a time (sequential connections).  After a
  client disconnects the accept() loop immediately waits for the next client,
  so the server never needs to be restarted between benchmark runs.

UDP (AF_INET + SOCK_DGRAM)
  UDP is *connectionless* and *message-oriented*.  Every recvfrom() returns
  exactly one datagram.  There is no framing problem.  The server echoes each
  datagram with sendto() without any modification, preserving the sequence
  identifier in the first 4 bytes.

Shutdown
========
A threading.Event (stop_event) is shared between the main thread and the server
threads.  Blocking calls (accept, recvfrom) are given a short timeout so they
wake up periodically, check stop_event, and exit cleanly.  SO_REUSEADDR is set
on every listening socket so ports are released immediately after shutdown.
"""

import argparse
import logging
import socket
import struct
import threading

# ── Payload layout constants ────────────────────────────────────────────────
# These are shared with benchmark.py via identical definitions so that both
# sides agree on the wire format without needing a shared module in Phase 1-2.
#
#   Byte layout of every message:
#       [0 : 4]       — sequence number  (struct ">I", big-endian uint32)
#       [4 : size]    — zero-padding bytes
#
SEQUENCE_HEADER_FMT  = ">I"                                   # big-endian uint32
SEQUENCE_HEADER_SIZE = struct.calcsize(SEQUENCE_HEADER_FMT)   # 4 bytes

# How long a blocking socket call waits before waking up to re-check stop_event.
# A value of 1 second means shutdown latency is at most ~1 s.
_POLL_TIMEOUT: float = 1.0


# ── TCP helper ──────────────────────────────────────────────────────────────

def recv_exact(
    conn: socket.socket,
    n: int,
    stop_event: threading.Event,
) -> bytes:
    """
    Read exactly *n* bytes from a TCP connection, tolerating partial reads.

    TCP delivers a continuous byte stream.  A single recv() call can return
    anywhere from 1 to n bytes depending on network/OS buffering.  This
    function loops, appending chunks, until the full message is assembled.

    Args:
        conn:       A connected, accepted TCP socket.
        n:          Number of bytes to receive.
        stop_event: Set this to abort the read during server shutdown.

    Returns:
        A bytes object of exactly *n* bytes.

    Raises:
        ConnectionError:       The peer closed the connection before n bytes arrived.
        ConnectionAbortedError: stop_event was set while waiting for data.
        OSError:               Unrecoverable socket error.
    """
    buf = bytearray()
    while len(buf) < n:
        if stop_event.is_set():
            raise ConnectionAbortedError("Server is shutting down.")
        try:
            chunk = conn.recv(n - len(buf))
        except socket.timeout:
            # Timeout just means no data yet — loop and re-check stop_event.
            continue
        if not chunk:
            # recv() returned empty bytes → the peer performed an orderly shutdown.
            raise ConnectionError(
                f"Peer closed the connection after {len(buf)}/{n} bytes."
            )
        buf.extend(chunk)
    return bytes(buf)


# ── TCP server ──────────────────────────────────────────────────────────────

def run_tcp_server(
    host: str,
    port: int,
    msg_size: int,
    stop_event: threading.Event,
) -> None:
    """
    TCP echo server loop.

    Lifecycle:
        1. Create socket, set SO_REUSEADDR, bind, listen.
        2. accept() in a loop (times out every _POLL_TIMEOUT seconds so
           stop_event is checked regularly).
        3. For each accepted client, run an inner recv/sendall loop:
               recv_exact(msg_size bytes) → sendall(same bytes)
           until the client disconnects or stop_event is set.
        4. Close the client socket, then go back to accept().
        5. On KeyboardInterrupt / stop_event, break out and close the
           listening socket via the finally block.

    Args:
        host:       Bind address.
        port:       TCP port.
        msg_size:   Fixed message size in bytes; recv_exact reads exactly this many.
        stop_event: Signal from main thread to stop the server.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR lets us re-bind the port immediately after shutdown,
    # avoiding "Address already in use" errors during repeated test runs.
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Timeout on the *listening* socket so accept() wakes up periodically.
    srv.settimeout(_POLL_TIMEOUT)

    try:
        srv.bind((host, port))
        srv.listen(1)  # backlog of 1: sequential clients, one at a time
        logging.info(
            "[TCP] Listening on %s:%d  (msg_size=%d bytes, header=%d bytes)",
            host, port, msg_size, SEQUENCE_HEADER_SIZE,
        )

        while not stop_event.is_set():
            # ── Wait for the next client ──────────────────────────────────
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                # No client connected within _POLL_TIMEOUT seconds — loop.
                continue

            logging.info("[TCP] Client connected: %s", addr)

            # Give the accepted connection its own short timeout so recv_exact
            # can periodically check stop_event without blocking forever.
            conn.settimeout(_POLL_TIMEOUT)

            try:
                # ── Inner echo loop for this client ───────────────────────
                while not stop_event.is_set():
                    try:
                        msg = recv_exact(conn, msg_size, stop_event)
                    except ConnectionError as exc:
                        # Orderly peer shutdown (client benchmark run finished).
                        logging.info("[TCP] %s disconnected: %s", addr, exc)
                        break
                    except ConnectionAbortedError:
                        # stop_event was set — exit the inner loop.
                        break

                    # Echo the full message back with a single send to avoid
                    # partial writes on the outgoing side.
                    conn.sendall(msg)

            except OSError as exc:
                logging.warning("[TCP] Socket error with %s: %s", addr, exc)
            finally:
                # Always release the client socket, even if an exception occurred.
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  # already closed by the peer
                conn.close()
                logging.info("[TCP] Connection to %s closed.", addr)

    except OSError as exc:
        logging.error("[TCP] Fatal error: %s", exc)
    finally:
        srv.close()
        logging.info("[TCP] Listening socket released.")


# ── UDP server ──────────────────────────────────────────────────────────────

def run_udp_server(
    host: str,
    port: int,
    stop_event: threading.Event,
) -> None:
    """
    UDP echo server loop.

    UDP is connectionless and message-oriented:
      - Each recvfrom() returns exactly one datagram — no framing needed.
      - sendto() echoes the datagram back to the sender's address.
      - The payload (including the 4-byte sequence number header) is returned
        byte-for-byte unchanged, so the client can match request/response pairs.

    Args:
        host:       Bind address.
        port:       UDP port.
        stop_event: Signal from main thread to stop the server.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Short timeout so the loop can re-check stop_event between datagrams.
    srv.settimeout(_POLL_TIMEOUT)

    try:
        srv.bind((host, port))
        logging.info("[UDP] Listening on %s:%d", host, port)

        while not stop_event.is_set():
            try:
                # 65535 is the maximum UDP payload size.  In practice the
                # benchmark sends much smaller messages (default: 64 bytes).
                data, addr = srv.recvfrom(65535)
            except socket.timeout:
                # No datagram arrived within _POLL_TIMEOUT — loop.
                continue

            # Echo the datagram back to the sender unchanged.
            # sendto() is used because the socket is not connected.
            srv.sendto(data, addr)

            # Note: we intentionally do NOT log every datagram here because
            # the benchmark sends thousands per second; verbose logging would
            # add significant latency jitter to measurements.

    except OSError as exc:
        logging.error("[UDP] Fatal error: %s", exc)
    finally:
        srv.close()
        logging.info("[UDP] Server socket released.")


# ── Argument parser ─────────────────────────────────────────────────────────

def _validate_message_size(value: str) -> int:
    """
    Argparse type validator for --size.
    Rejects values smaller than SEQUENCE_HEADER_SIZE.
    """
    try:
        size = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid integer: {value!r}")
    if size < SEQUENCE_HEADER_SIZE:
        raise argparse.ArgumentTypeError(
            f"--size must be at least {SEQUENCE_HEADER_SIZE} bytes "
            f"(the sequence number header). Got: {size}"
        )
    return size


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the argument parser for server.py."""
    parser = argparse.ArgumentParser(
        prog="server.py",
        description=(
            "XBench echo server.\n"
            "Starts a TCP server and/or a UDP server that echo every received\n"
            "message back to the sender unchanged. Press Ctrl+C to stop."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        metavar="HOST",
        help="IP address to bind the servers to.",
    )
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=5000,
        metavar="PORT",
        help="Port for the TCP echo server.",
    )
    parser.add_argument(
        "--udp-port",
        type=int,
        default=5001,
        metavar="PORT",
        help="Port for the UDP echo server.",
    )
    parser.add_argument(
        "--mode",
        choices=["tcp", "udp", "both"],
        default="both",
        help="Which server(s) to start.",
    )
    parser.add_argument(
        "--size",
        type=_validate_message_size,
        default=64,
        metavar="BYTES",
        help=(
            f"Expected message payload size in bytes (minimum {SEQUENCE_HEADER_SIZE}). "
            "The TCP server reads exactly this many bytes per message to handle "
            "TCP stream framing. Must match the --size passed to benchmark.py."
        ),
    )
    return parser


# ── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = build_arg_parser()
    args = parser.parse_args()

    # Shared stop signal: main thread sets this on Ctrl+C; server threads
    # check it on every poll-timeout cycle and exit cleanly.
    stop_event = threading.Event()
    threads: list[threading.Thread] = []

    if args.mode in ("tcp", "both"):
        threads.append(threading.Thread(
            target=run_tcp_server,
            args=(args.host, args.tcp_port, args.size, stop_event),
            name="tcp-server",
            daemon=True,   # die automatically if the main thread crashes
        ))

    if args.mode in ("udp", "both"):
        threads.append(threading.Thread(
            target=run_udp_server,
            args=(args.host, args.udp_port, stop_event),
            name="udp-server",
            daemon=True,
        ))

    for t in threads:
        t.start()

    logging.info(
        "XBench server started (mode=%s). Press Ctrl+C to stop.", args.mode
    )

    try:
        # Block the main thread until KeyboardInterrupt (Ctrl+C).
        # join() with a timeout loop keeps the thread interruptible on Windows.
        while True:
            for t in threads:
                t.join(timeout=0.5)
            if not any(t.is_alive() for t in threads):
                break  # all threads exited on their own (shouldn't happen normally)

    except KeyboardInterrupt:
        logging.info("Ctrl+C received — shutting down …")
        stop_event.set()
        for t in threads:
            t.join(timeout=5.0)
        logging.info("XBench server stopped cleanly.")


if __name__ == "__main__":
    main()
