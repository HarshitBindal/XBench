"""
benchmark.py — XBench benchmark client.

Phase 3 / 4: Full TCP and UDP client with RTT measurement and metric analytics.

Measurement design
==================
* time.perf_counter_ns() is used for all timing.  It is a monotonic,
  high-resolution clock whose nanosecond values have no defined epoch —
  only differences between two readings are meaningful.  We convert each
  RTT to microseconds (ns / 1000) before storing it.

* Only one request is outstanding at a time (synchronous send/recv loop).
  This measures pure round-trip latency, not aggregate throughput under load.

* Warm-up phase: the first --warmup requests are sent and received but their
  RTT samples are discarded.  Warm-up hides connection-setup latency (TCP
  handshake, OS socket buffer allocation) and JIT-compilation effects that
  would skew the first few samples.

* Failure handling rules (from spec):
    - Timeout:             counted as a failure; the timeout value is NOT
                           recorded as an RTT sample.
    - Socket error:        counted as a failure; the run is aborted.
    - Wrong sequence (UDP): counted as a failure; the mismatched datagram is
                           dropped rather than attributed to a later request.
  "Record unsuccessful requests separately; never include timeout values as
   successful RTT samples."  — context.md

TCP framing
===========
TCP is a byte stream.  A single recv() may return fewer bytes than the message
size.  recv_exact() loops until exactly msg_size bytes are collected.

UDP sequence validation
=======================
UDP datagrams can be lost, duplicated, or arrive out of order.  After sending
request N, the client calls recvfrom() with a timeout.  If the datagram that
arrives carries a sequence number other than N, the datagram is discarded and
the request is recorded as a failure.  It is NOT credited to a later request.
"""

import argparse
import csv
import os
import socket
import struct
import time
from datetime import datetime

import analyze

# ── Payload layout constants ────────────────────────────────────────────────
#
#   Byte layout of every message:
#       [0 : 4]       — sequence number  (struct ">I", big-endian uint32)
#       [4 : size]    — zero-padding bytes (0x00)
#
SEQUENCE_HEADER_FMT  = ">I"                                   # big-endian uint32
SEQUENCE_HEADER_SIZE = struct.calcsize(SEQUENCE_HEADER_FMT)   # 4 bytes
PADDING_BYTE         = b"\x00"


# ── Result record ────────────────────────────────────────────────────────────

class SampleResult:
    """
    Holds the outcome of a single message exchange.

    Attributes:
        sequence:  The request sequence number (0-indexed over the entire run,
                   including warm-up).
        success:   True if the echo was received and validated successfully.
        rtt_us:    Round-trip time in microseconds, or None on failure.
        is_warmup: True if this sample belongs to the warm-up phase.
    """

    __slots__ = ("sequence", "success", "rtt_us", "is_warmup")

    def __init__(
        self,
        sequence: int,
        success: bool,
        rtt_us: float | None,
        is_warmup: bool,
    ) -> None:
        self.sequence  = sequence
        self.success   = success
        self.rtt_us    = rtt_us
        self.is_warmup = is_warmup

    def __repr__(self) -> str:
        state = f"rtt={self.rtt_us:.1f}us" if self.success else "FAILED"
        phase = "warmup" if self.is_warmup else "measured"
        return f"SampleResult(seq={self.sequence}, {state}, {phase})"


# ── Payload helpers ──────────────────────────────────────────────────────────

def build_payload(sequence: int, size: int) -> bytes:
    """
    Build a fixed-size request payload.

    Args:
        sequence: Zero-indexed request number packed into bytes 0-3.
        size:     Total payload size in bytes (must be >= SEQUENCE_HEADER_SIZE).

    Returns:
        A bytes object of exactly *size* bytes.

    Example:
        build_payload(42, 8) -> b'\\x00\\x00\\x00\\x2a\\x00\\x00\\x00\\x00'
    """
    if size < SEQUENCE_HEADER_SIZE:
        raise ValueError(
            f"Payload size {size} is smaller than the sequence header "
            f"({SEQUENCE_HEADER_SIZE} bytes)."
        )
    header  = struct.pack(SEQUENCE_HEADER_FMT, sequence)
    padding = PADDING_BYTE * (size - SEQUENCE_HEADER_SIZE)
    return header + padding


def parse_sequence(payload: bytes) -> int:
    """Extract the sequence number from the first 4 bytes of a payload."""
    return struct.unpack(SEQUENCE_HEADER_FMT, payload[:SEQUENCE_HEADER_SIZE])[0]


# ── TCP helper ───────────────────────────────────────────────────────────────

def recv_exact(conn: socket.socket, n: int) -> bytes:
    """
    Read exactly *n* bytes from a TCP connection, tolerating partial reads.

    TCP delivers a continuous byte stream; a single recv() can return anywhere
    from 1 to n bytes.  This function loops, appending chunks, until the full
    message is assembled.

    Args:
        conn: A connected TCP socket (must have a timeout set by the caller).
        n:    Number of bytes to receive.

    Returns:
        bytes of length exactly *n*.

    Raises:
        ConnectionError: The server closed the connection before n bytes arrived.
        socket.timeout:  The socket's timeout expired — caller handles this.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f"Server closed the connection after {len(buf)}/{n} bytes."
            )
        buf.extend(chunk)
    return bytes(buf)


# ── TCP benchmark ────────────────────────────────────────────────────────────

def run_tcp_benchmark(
    host: str,
    port: int,
    count: int,
    size: int,
    warmup: int,
    timeout: float,
) -> tuple[list[SampleResult], float]:
    """
    Run a TCP echo benchmark and return per-request SampleResult objects.

    One TCP connection is opened and kept alive for the entire run (warm-up
    + measured requests).  The connection is closed once when the run ends.

    Per-request loop:
        1. Build payload with the current sequence number.
        2. Record t_start = time.perf_counter_ns() immediately before sendall().
        3. sendall(payload).
        4. recv_exact(size bytes) — reassembles TCP stream into one message.
        5. Record t_end = time.perf_counter_ns() immediately after recv_exact.
        6. Validate: echoed sequence number must equal sent sequence number.
        7. RTT = (t_end - t_start) / 1000  [microseconds]

    Failures (timeout, wrong seq, socket error) are recorded as failed samples.
    On an unrecoverable socket error the remaining requests are also recorded
    as failures and the function returns early.

    Args:
        host:    Server IP address or hostname.
        port:    TCP server port.
        count:   Number of *measured* requests (after warm-up).
        size:    Message payload size in bytes.
        warmup:  Number of warm-up requests (discarded from results).
        timeout: Socket receive timeout in seconds.

    Returns:
        Tuple of (list[SampleResult], float) where the float is the wall-clock
        duration in seconds of the measurement window (warm-up excluded).
    """
    total   = warmup + count
    results: list[SampleResult] = []

    # Elapsed-time tracking for the measurement window.
    # t_measure_start is set at the beginning of the first measured request;
    # t_measure_end is updated after every measured request (success or fail).
    # Using time.perf_counter() (seconds) keeps elapsed separate from the
    # nanosecond per-RTT clock used inside the loop.
    t_measure_start: float | None = None
    t_measure_end:   float | None = None

    print(
        f"[TCP] Connecting to {host}:{port} "
        f"(size={size}B, warmup={warmup}, count={count}, timeout={timeout}s) ..."
    )

    try:
        conn = socket.create_connection((host, port), timeout=timeout)
    except OSError as exc:
        print(f"[TCP] Connection failed: {exc}")
        # All requests count as failures if we cannot connect at all.
        for seq in range(total):
            results.append(SampleResult(seq, False, None, seq < warmup))
        return results, 0.0

    conn.settimeout(timeout)

    try:
        for seq in range(total):
            is_warmup = seq < warmup

            # Mark the start of the measurement window on the first measured req.
            if seq == warmup:
                t_measure_start = time.perf_counter()

            payload = build_payload(seq, size)

            # ── Send and time the round trip ──────────────────────────────
            t_start = time.perf_counter_ns()       # ← start timer
            try:
                conn.sendall(payload)
                echo = recv_exact(conn, size)
            except socket.timeout:
                results.append(SampleResult(seq, False, None, is_warmup))
                label = "warmup" if is_warmup else f"seq={seq - warmup}"
                print(f"[TCP] Timeout on {label}")
                continue
            except (ConnectionError, OSError) as exc:
                results.append(SampleResult(seq, False, None, is_warmup))
                print(f"[TCP] Socket error on seq={seq}: {exc} -- aborting run.")
                # Mark all remaining requests as failures without attempting them.
                if not is_warmup:
                    t_measure_end = time.perf_counter()
                for remaining in range(seq + 1, total):
                    is_w = remaining < warmup
                    results.append(SampleResult(remaining, False, None, is_w))
                elapsed_s = _elapsed(t_measure_start, t_measure_end)
                return results, elapsed_s
            t_end = time.perf_counter_ns()         # ← stop timer

            rtt_us = (t_end - t_start) / 1_000    # nanoseconds → microseconds

            # ── Validate the echoed sequence number ───────────────────────
            echoed_seq = parse_sequence(echo)
            if echoed_seq != seq:
                results.append(SampleResult(seq, False, None, is_warmup))
                print(
                    f"[TCP] Sequence mismatch on seq={seq}: "
                    f"expected {seq}, got {echoed_seq}"
                )
                continue

            results.append(SampleResult(seq, True, rtt_us, is_warmup))

            # Update measurement window end time after every measured request.
            if not is_warmup:
                t_measure_end = time.perf_counter()

            # Light progress reporting every 100 measured requests.
            measured_seq = seq - warmup
            if not is_warmup and measured_seq % 100 == 0:
                print(f"[TCP] Progress: {measured_seq}/{count}  rtt={rtt_us:.1f}us")

    finally:
        conn.close()
        print(f"[TCP] Connection closed.")

    elapsed_s = _elapsed(t_measure_start, t_measure_end)
    return results, elapsed_s


# ── UDP benchmark ────────────────────────────────────────────────────────────

def run_udp_benchmark(
    host: str,
    port: int,
    count: int,
    size: int,
    warmup: int,
    timeout: float,
) -> tuple[list[SampleResult], float]:
    """
    Run a UDP echo benchmark and return per-request SampleResult objects.

    A single unconnected UDP socket is used for the entire run.  For each
    request the client:
        1. Builds a payload with the current sequence number.
        2. Records t_start = time.perf_counter_ns() immediately before sendto().
        3. sendto(payload, server_address).
        4. recvfrom() -- UDP preserves datagram boundaries, so exactly one
           datagram is returned per call.
        5. Records t_end = time.perf_counter_ns() immediately after recvfrom().
        6. Validates the sequence number in the response.
            - If it matches: success, RTT = (t_end - t_start) / 1000.
            - If it does NOT match (stale or wrong datagram): the request is
              recorded as a failure.  The datagram is NOT credited to any
              future request (doing so would produce incorrect RTTs).
        7. Timeout: the request is recorded as a failure; the timeout value
           is never stored as an RTT sample.

    Note on UDP ordering: the spec says "validate UDP response sequence numbers
    so delayed replies are not incorrectly attributed to a later request."
    We enforce this strictly: any datagram with the wrong sequence number
    is treated as a failure for the current request and discarded.

    Args:
        host:    Server IP address or hostname.
        port:    UDP server port.
        count:   Number of *measured* requests (after warm-up).
        size:    Message payload size in bytes.
        warmup:  Number of warm-up requests (discarded from results).
        timeout: Socket receive timeout in seconds.

    Returns:
        Tuple of (list[SampleResult], float) where the float is the wall-clock
        duration in seconds of the measurement window (warm-up excluded).
    """
    total   = warmup + count
    results: list[SampleResult] = []
    server  = (host, port)

    # Elapsed-time tracking for the measurement window (see run_tcp_benchmark).
    t_measure_start: float | None = None
    t_measure_end:   float | None = None

    print(
        f"[UDP] Target {host}:{port} "
        f"(size={size}B, warmup={warmup}, count={count}, timeout={timeout}s) ..."
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)

    try:
        for seq in range(total):
            is_warmup = seq < warmup

            # Mark the start of the measurement window on the first measured req.
            if seq == warmup:
                t_measure_start = time.perf_counter()

            payload = build_payload(seq, size)

            # ── Send and time the round trip ──────────────────────────────
            t_start = time.perf_counter_ns()       # ← start timer
            try:
                sock.sendto(payload, server)
                data, _ = sock.recvfrom(65535)
            except socket.timeout:
                results.append(SampleResult(seq, False, None, is_warmup))
                label = "warmup" if is_warmup else f"seq={seq - warmup}"
                print(f"[UDP] Timeout on {label}")
                continue
            except OSError as exc:
                results.append(SampleResult(seq, False, None, is_warmup))
                print(f"[UDP] Socket error on seq={seq}: {exc} -- aborting run.")
                if not is_warmup:
                    t_measure_end = time.perf_counter()
                for remaining in range(seq + 1, total):
                    is_w = remaining < warmup
                    results.append(SampleResult(remaining, False, None, is_w))
                elapsed_s = _elapsed(t_measure_start, t_measure_end)
                return results, elapsed_s
            t_end = time.perf_counter_ns()         # ← stop timer

            rtt_us = (t_end - t_start) / 1_000    # nanoseconds → microseconds

            # ── Validate the echoed sequence number ───────────────────────
            # Critical for UDP: a datagram from a *previous* request that was
            # delayed might arrive here.  Crediting it to the current request
            # would produce a falsely short RTT for the current message and
            # silently skip the actual loss of the current datagram.
            echoed_seq = parse_sequence(data)
            if echoed_seq != seq:
                results.append(SampleResult(seq, False, None, is_warmup))
                print(
                    f"[UDP] Sequence mismatch on seq={seq}: "
                    f"expected {seq}, got {echoed_seq} — recording as failure."
                )
                continue

            results.append(SampleResult(seq, True, rtt_us, is_warmup))

            # Update measurement window end time after every measured request.
            if not is_warmup:
                t_measure_end = time.perf_counter()

            # Light progress reporting every 100 measured requests.
            measured_seq = seq - warmup
            if not is_warmup and measured_seq % 100 == 0:
                print(f"[UDP] Progress: {measured_seq}/{count}  rtt={rtt_us:.1f}us")

    finally:
        sock.close()
        print(f"[UDP] Socket closed.")

    elapsed_s = _elapsed(t_measure_start, t_measure_end)
    return results, elapsed_s


# -- Elapsed helper ---------------------------------------------------------

def _elapsed(
    start: float | None,
    end: float | None,
) -> float:
    """
    Return wall-clock seconds between two perf_counter() readings.
    Returns 0.0 when either value is None (no measured requests ran).
    """
    if start is None or end is None:
        return 0.0
    return max(0.0, end - start)


# ── CSV output (Phase 5) ─────────────────────────────────────────────────────

def write_results_csv(
    results: list[SampleResult],
    protocol: str,
    results_dir: str,
    timestamp: str,
) -> str:
    """
    Write per-request results to a CSV file.

    Columns: protocol, sequence, is_warmup, success, rtt_us
    rtt_us is empty for failed requests (never a timeout placeholder).

    Args:
        results:     List of SampleResult objects from the benchmark run.
        protocol:    "tcp" or "udp".
        results_dir: Directory to save the CSV in (created if missing).
        timestamp:   ISO-style timestamp string used in the filename.

    Returns:
        The absolute path of the written CSV file.
    """
    os.makedirs(results_dir, exist_ok=True)
    filename = f"{protocol}_results_{timestamp}.csv"
    filepath = os.path.join(results_dir, filename)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["protocol", "sequence", "is_warmup", "success", "rtt_us"])
        for r in results:
            writer.writerow([
                protocol,
                r.sequence,
                r.is_warmup,
                r.success,
                f"{r.rtt_us:.2f}" if r.rtt_us is not None else "",
            ])

    print(f"[CSV] Per-request results -> {filepath}")
    return filepath


def write_summary_csv(
    metrics: "analyze.BenchmarkMetrics",
    protocol: str,
    host: str,
    port: int,
    size: int,
    count: int,
    warmup: int,
    timeout: float,
    results_dir: str,
    timestamp: str,
) -> str:
    """
    Write run configuration and aggregate metrics to a summary CSV.

    The file contains two-column key/value rows so it is easy to read
    both programmatically and in a spreadsheet.

    Args:
        metrics:     Computed BenchmarkMetrics.
        protocol:    "tcp" or "udp".
        host, port, size, count, warmup, timeout: Run configuration.
        results_dir: Directory to save the CSV in.
        timestamp:   ISO-style timestamp string used in the filename.

    Returns:
        The absolute path of the written CSV file.
    """
    os.makedirs(results_dir, exist_ok=True)
    filename = f"{protocol}_summary_{timestamp}.csv"
    filepath = os.path.join(results_dir, filename)

    def _val(v: float | None, fmt: str = ".2f") -> str:
        return f"{v:{fmt}}" if v is not None else ""

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "value"])
        # ── Run configuration ──
        writer.writerow(["protocol", protocol])
        writer.writerow(["host", host])
        writer.writerow(["port", port])
        writer.writerow(["size_bytes", size])
        writer.writerow(["count", count])
        writer.writerow(["warmup", warmup])
        writer.writerow(["timeout_s", timeout])
        writer.writerow(["timestamp", timestamp])
        # ── Aggregate metrics ──
        writer.writerow(["elapsed_s", _val(metrics.elapsed_s, ".3f")])
        writer.writerow(["attempted", metrics.attempted])
        writer.writerow(["successful", metrics.successful])
        writer.writerow(["failed", metrics.failed])
        writer.writerow(["avg_rtt_us", _val(metrics.avg_rtt_us)])
        writer.writerow(["median_rtt_us", _val(metrics.median_rtt_us)])
        writer.writerow(["p95_rtt_us", _val(metrics.p95_rtt_us)])
        writer.writerow(["p99_rtt_us", _val(metrics.p99_rtt_us)])
        writer.writerow(["min_rtt_us", _val(metrics.min_rtt_us)])
        writer.writerow(["max_rtt_us", _val(metrics.max_rtt_us)])
        writer.writerow(["throughput_msg_per_sec", _val(metrics.throughput_msg_per_sec, ".1f")])
        writer.writerow(["response_loss_pct", _val(metrics.response_loss_pct)])

    print(f"[CSV] Summary -> {filepath}")
    return filepath


# ── Argument validators ──────────────────────────────────────────────────────

def _validate_message_size(value: str) -> int:
    """Reject --size values smaller than SEQUENCE_HEADER_SIZE at parse time."""
    try:
        size = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid integer: {value!r}")
    if size < SEQUENCE_HEADER_SIZE:
        raise argparse.ArgumentTypeError(
            f"--size must be at least {SEQUENCE_HEADER_SIZE} bytes "
            f"(the sequence number header occupies bytes 0-3). Got: {size}"
        )
    return size


# ── Argument parser ──────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    """Return the argument parser for benchmark.py."""
    parser = argparse.ArgumentParser(
        prog="benchmark.py",
        description=(
            "XBench benchmark client.\n"
            "Sends fixed-size messages to an echo server over TCP or UDP,\n"
            "measures application-level round-trip latency, and reports results."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--protocol",
        choices=["tcp", "udp"],
        required=True,
        help="Transport protocol to benchmark (tcp or udp).",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        metavar="HOST",
        help="Server IP address or hostname.",
    )
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=5000,
        metavar="PORT",
        help="TCP server port.",
    )
    parser.add_argument(
        "--udp-port",
        type=int,
        default=5001,
        metavar="PORT",
        help="UDP server port.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        metavar="N",
        help="Number of messages to send after the warm-up phase.",
    )
    parser.add_argument(
        "--size",
        type=_validate_message_size,
        default=64,
        metavar="BYTES",
        help=(
            f"Message payload size in bytes. "
            f"Minimum is {SEQUENCE_HEADER_SIZE} (the 4-byte sequence header)."
        ),
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=100,
        metavar="N",
        help=(
            "Number of warm-up messages sent before recording measurements. "
            "Warm-up samples are discarded."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=2.0,
        metavar="SECS",
        help="Socket receive timeout in seconds. Exceeded timeouts count as failures.",
    )
    parser.add_argument(
        "--results-dir",
        default="results",
        metavar="DIR",
        help="Directory where CSV result files are saved.",
    )
    return parser


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_arg_parser()
    args   = parser.parse_args()

    port = args.tcp_port if args.protocol == "tcp" else args.udp_port

    print(f"XBench Benchmark Client")
    print(f"  Protocol : {args.protocol.upper()}")
    print(f"  Target   : {args.host}:{port}")
    print(f"  Messages : {args.count}  (+{args.warmup} warm-up, discarded)")
    print(f"  Size     : {args.size} bytes")
    print(f"  Timeout  : {args.timeout} s")
    print()

    if args.protocol == "tcp":
        results, elapsed_s = run_tcp_benchmark(
            host    = args.host,
            port    = args.tcp_port,
            count   = args.count,
            size    = args.size,
            warmup  = args.warmup,
            timeout = args.timeout,
        )
    else:
        results, elapsed_s = run_udp_benchmark(
            host    = args.host,
            port    = args.udp_port,
            count   = args.count,
            size    = args.size,
            warmup  = args.warmup,
            timeout = args.timeout,
        )

    metrics = analyze.compute_metrics(
        results      = results,
        elapsed_s    = elapsed_s,
        protocol     = args.protocol,
        warmup_count = args.warmup,
    )
    analyze.print_summary(metrics)

    # ── Write CSV output (Phase 5) ────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    write_results_csv(results, args.protocol, args.results_dir, ts)
    write_summary_csv(
        metrics     = metrics,
        protocol    = args.protocol,
        host        = args.host,
        port        = port,
        size        = args.size,
        count       = args.count,
        warmup      = args.warmup,
        timeout     = args.timeout,
        results_dir = args.results_dir,
        timestamp   = ts,
    )


if __name__ == "__main__":
    main()
