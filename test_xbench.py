"""
test_xbench.py -- Phase 6 comprehensive test suite for XBench.

Covers every test case required by Phase 6:
    1.  TCP and UDP echo success.
    2.  Multiple message sizes and counts.
    3.  Very small allowed payload (size == SEQUENCE_HEADER_SIZE == 4).
    4.  Server unavailable.
    5.  Socket timeout.
    6.  UDP response mismatch.
    7.  Empty successful-sample set (all requests fail -> metrics safe).
    8.  TCP connection closed during a run.
    9.  Valid CSV generation.
    10. Both localhost and configurable host support.

Run with:
    python -m unittest test_xbench -v
    python test_xbench.py               (runs all tests)
"""

import csv
import os
import shutil
import socket
import struct
import threading
import time
import unittest

# ── Project imports ──────────────────────────────────────────────────────────
from server import (
    run_tcp_server,
    run_udp_server,
    SEQUENCE_HEADER_FMT,
    SEQUENCE_HEADER_SIZE,
)
from benchmark import (
    build_payload,
    parse_sequence,
    recv_exact,
    run_tcp_benchmark,
    run_udp_benchmark,
    write_results_csv,
    write_summary_csv,
    SampleResult,
)
import analyze


# ── Helpers ──────────────────────────────────────────────────────────────────

def _free_port() -> int:
    """Return an available TCP/UDP port by briefly binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── Shared test fixtures ────────────────────────────────────────────────────

class _ServerMixin:
    """
    Mixin that starts TCP + UDP echo servers on random ports for a test class.
    Ports are stored in cls.tcp_port / cls.udp_port.
    """

    HOST = "127.0.0.1"
    MSG_SIZE = 64

    @classmethod
    def start_servers(cls, msg_size: int = 64) -> None:
        cls.MSG_SIZE = msg_size
        cls.tcp_port = _free_port()
        cls.udp_port = _free_port()
        cls.stop_event = threading.Event()

        cls.tcp_thread = threading.Thread(
            target=run_tcp_server,
            args=(cls.HOST, cls.tcp_port, cls.MSG_SIZE, cls.stop_event),
            daemon=True,
        )
        cls.udp_thread = threading.Thread(
            target=run_udp_server,
            args=(cls.HOST, cls.udp_port, cls.stop_event),
            daemon=True,
        )
        cls.tcp_thread.start()
        cls.udp_thread.start()
        time.sleep(0.3)  # allow servers to bind

    @classmethod
    def stop_servers(cls) -> None:
        cls.stop_event.set()
        cls.tcp_thread.join(timeout=3.0)
        cls.udp_thread.join(timeout=3.0)


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 1 — TCP and UDP echo success
# ═════════════════════════════════════════════════════════════════════════════

class TestEchoSuccess(unittest.TestCase, _ServerMixin):
    """Verify that both TCP and UDP echoes succeed with default settings."""

    @classmethod
    def setUpClass(cls):
        cls.start_servers(msg_size=64)

    @classmethod
    def tearDownClass(cls):
        cls.stop_servers()

    def test_tcp_echo_all_succeed(self):
        results, elapsed = run_tcp_benchmark(
            host=self.HOST, port=self.tcp_port,
            count=20, size=64, warmup=5, timeout=2.0,
        )
        measured = [r for r in results if not r.is_warmup]
        self.assertEqual(len(measured), 20)
        self.assertTrue(all(r.success for r in measured),
                        "All measured TCP requests should succeed")
        self.assertTrue(all(r.rtt_us is not None and r.rtt_us > 0
                            for r in measured))

    def test_udp_echo_all_succeed(self):
        results, elapsed = run_udp_benchmark(
            host=self.HOST, port=self.udp_port,
            count=20, size=64, warmup=5, timeout=2.0,
        )
        measured = [r for r in results if not r.is_warmup]
        self.assertEqual(len(measured), 20)
        self.assertTrue(all(r.success for r in measured),
                        "All measured UDP requests should succeed")


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 2 — Multiple message sizes and counts
# ═════════════════════════════════════════════════════════════════════════════

class TestMultipleSizesAndCounts(unittest.TestCase):
    """Run TCP benchmarks with various sizes and counts."""

    HOST = "127.0.0.1"

    def _run_with(self, size: int, count: int):
        """Start a server matching *size*, run benchmark, stop server."""
        tcp_port = _free_port()
        stop = threading.Event()
        t = threading.Thread(
            target=run_tcp_server,
            args=(self.HOST, tcp_port, size, stop),
            daemon=True,
        )
        t.start()
        time.sleep(0.2)
        try:
            results, _ = run_tcp_benchmark(
                host=self.HOST, port=tcp_port,
                count=count, size=size, warmup=0, timeout=2.0,
            )
        finally:
            stop.set()
            t.join(timeout=3.0)
        return results

    def test_size_4_count_10(self):
        results = self._run_with(size=4, count=10)
        self.assertTrue(all(r.success for r in results))
        self.assertEqual(len(results), 10)

    def test_size_128_count_50(self):
        results = self._run_with(size=128, count=50)
        self.assertTrue(all(r.success for r in results))
        self.assertEqual(len(results), 50)

    def test_size_1024_count_30(self):
        results = self._run_with(size=1024, count=30)
        self.assertTrue(all(r.success for r in results))
        self.assertEqual(len(results), 30)


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 3 — Very small allowed payload (size == 4)
# ═════════════════════════════════════════════════════════════════════════════

class TestMinimumPayload(unittest.TestCase):
    """Payload of exactly SEQUENCE_HEADER_SIZE (4 bytes) must work."""

    HOST = "127.0.0.1"

    @classmethod
    def setUpClass(cls):
        cls.tcp_port = _free_port()
        cls.udp_port = _free_port()
        cls.stop_event = threading.Event()
        cls.tcp_thread = threading.Thread(
            target=run_tcp_server,
            args=(cls.HOST, cls.tcp_port, SEQUENCE_HEADER_SIZE, cls.stop_event),
            daemon=True,
        )
        cls.udp_thread = threading.Thread(
            target=run_udp_server,
            args=(cls.HOST, cls.udp_port, cls.stop_event),
            daemon=True,
        )
        cls.tcp_thread.start()
        cls.udp_thread.start()
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.stop_event.set()
        cls.tcp_thread.join(timeout=3.0)
        cls.udp_thread.join(timeout=3.0)

    def test_tcp_minimum_size(self):
        results, _ = run_tcp_benchmark(
            host=self.HOST, port=self.tcp_port,
            count=10, size=SEQUENCE_HEADER_SIZE, warmup=2, timeout=2.0,
        )
        measured = [r for r in results if not r.is_warmup]
        self.assertTrue(all(r.success for r in measured))

    def test_udp_minimum_size(self):
        results, _ = run_udp_benchmark(
            host=self.HOST, port=self.udp_port,
            count=10, size=SEQUENCE_HEADER_SIZE, warmup=2, timeout=2.0,
        )
        measured = [r for r in results if not r.is_warmup]
        self.assertTrue(all(r.success for r in measured))

    def test_payload_builder_minimum(self):
        payload = build_payload(42, SEQUENCE_HEADER_SIZE)
        self.assertEqual(len(payload), SEQUENCE_HEADER_SIZE)
        self.assertEqual(parse_sequence(payload), 42)

    def test_payload_builder_rejects_too_small(self):
        with self.assertRaises(ValueError):
            build_payload(0, SEQUENCE_HEADER_SIZE - 1)


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 4 — Server unavailable
# ═════════════════════════════════════════════════════════════════════════════

class TestServerUnavailable(unittest.TestCase):
    """Benchmark must handle connection failure gracefully."""

    def test_tcp_no_server(self):
        port = _free_port()
        results, elapsed = run_tcp_benchmark(
            host="127.0.0.1", port=port,
            count=5, size=64, warmup=0, timeout=1.0,
        )
        # All requests should fail because no server is listening.
        self.assertTrue(all(not r.success for r in results))
        self.assertEqual(len(results), 5)
        self.assertEqual(elapsed, 0.0)

    def test_udp_no_server(self):
        port = _free_port()
        results, elapsed = run_udp_benchmark(
            host="127.0.0.1", port=port,
            count=3, size=64, warmup=0, timeout=0.5,
        )
        # UDP has no connection handshake.  Sends will succeed but
        # recvfrom will time out because nobody is echoing.
        self.assertTrue(all(not r.success for r in results))
        self.assertEqual(len(results), 3)


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 5 — Socket timeout
# ═════════════════════════════════════════════════════════════════════════════

class TestSocketTimeout(unittest.TestCase):
    """A very short timeout should cause failures without crashing."""

    def test_udp_timeout_causes_failures(self):
        """Send to a port where nobody listens with 0.1 s timeout."""
        port = _free_port()
        results, _ = run_udp_benchmark(
            host="127.0.0.1", port=port,
            count=3, size=64, warmup=0, timeout=0.1,
        )
        self.assertTrue(all(not r.success for r in results))
        # RTT should be None for every timed-out request.
        self.assertTrue(all(r.rtt_us is None for r in results))


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 6 — UDP response mismatch
# ═════════════════════════════════════════════════════════════════════════════

class TestUdpResponseMismatch(unittest.TestCase):
    """
    A rogue UDP server that always echoes sequence=9999 should cause
    every request to be recorded as a failure (sequence mismatch).
    """

    HOST = "127.0.0.1"

    @staticmethod
    def _rogue_udp_server(host: str, port: int, stop: threading.Event):
        """Echo every datagram but replace the sequence number with 9999."""
        srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.settimeout(1.0)
        srv.bind((host, port))
        while not stop.is_set():
            try:
                data, addr = srv.recvfrom(65535)
            except socket.timeout:
                continue
            # Replace the 4-byte sequence header with 9999.
            bad = struct.pack(">I", 9999) + data[4:]
            srv.sendto(bad, addr)
        srv.close()

    def test_mismatch_recorded_as_failure(self):
        port = _free_port()
        stop = threading.Event()
        t = threading.Thread(
            target=self._rogue_udp_server,
            args=(self.HOST, port, stop),
            daemon=True,
        )
        t.start()
        time.sleep(0.2)

        try:
            results, _ = run_udp_benchmark(
                host=self.HOST, port=port,
                count=5, size=64, warmup=0, timeout=2.0,
            )
        finally:
            stop.set()
            t.join(timeout=3.0)

        self.assertEqual(len(results), 5)
        self.assertTrue(all(not r.success for r in results),
                        "All requests should fail due to sequence mismatch")
        self.assertTrue(all(r.rtt_us is None for r in results))


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 7 — Empty successful-sample set
# ═════════════════════════════════════════════════════════════════════════════

class TestEmptySuccessSamples(unittest.TestCase):
    """
    When every request fails, compute_metrics must not crash
    (no division by zero) and RTT/throughput must be None.
    """

    def test_metrics_all_failed(self):
        # Build a list of all-failed SampleResult objects.
        results = [
            SampleResult(sequence=i, success=False, rtt_us=None, is_warmup=False)
            for i in range(10)
        ]
        metrics = analyze.compute_metrics(
            results=results, elapsed_s=1.0,
            protocol="tcp", warmup_count=0,
        )
        self.assertEqual(metrics.attempted, 10)
        self.assertEqual(metrics.successful, 0)
        self.assertEqual(metrics.failed, 10)
        self.assertIsNone(metrics.avg_rtt_us)
        self.assertIsNone(metrics.median_rtt_us)
        self.assertIsNone(metrics.p95_rtt_us)
        self.assertIsNone(metrics.p99_rtt_us)
        self.assertIsNone(metrics.throughput_msg_per_sec)
        self.assertAlmostEqual(metrics.response_loss_pct, 100.0)

    def test_metrics_zero_attempted(self):
        """No requests at all (warmup-only run)."""
        results = [
            SampleResult(sequence=i, success=True, rtt_us=50.0, is_warmup=True)
            for i in range(5)
        ]
        metrics = analyze.compute_metrics(
            results=results, elapsed_s=0.0,
            protocol="udp", warmup_count=5,
        )
        self.assertEqual(metrics.attempted, 0)
        self.assertEqual(metrics.successful, 0)
        self.assertAlmostEqual(metrics.response_loss_pct, 0.0)

    def test_print_summary_no_crash(self):
        """print_summary must handle all-None RTTs without raising."""
        results = [
            SampleResult(sequence=0, success=False, rtt_us=None, is_warmup=False)
        ]
        metrics = analyze.compute_metrics(
            results=results, elapsed_s=0.0,
            protocol="tcp", warmup_count=0,
        )
        # Should not raise any exception.
        analyze.print_summary(metrics)


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 8 — TCP connection closed during a run
# ═════════════════════════════════════════════════════════════════════════════

class TestTcpConnectionClosedMidRun(unittest.TestCase):
    """
    A server that closes the connection after N messages should cause
    remaining requests to be recorded as failures, not crash the client.
    """

    HOST = "127.0.0.1"
    MSG_SIZE = 64
    ECHO_COUNT = 3   # server echoes exactly this many then closes

    @classmethod
    def _limited_tcp_server(cls, host, port, stop):
        """Accept one client, echo ECHO_COUNT messages, then close."""
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.settimeout(5.0)
        srv.bind((host, port))
        srv.listen(1)
        try:
            conn, _ = srv.accept()
            conn.settimeout(2.0)
            for _ in range(cls.ECHO_COUNT):
                buf = bytearray()
                while len(buf) < cls.MSG_SIZE:
                    chunk = conn.recv(cls.MSG_SIZE - len(buf))
                    if not chunk:
                        break
                    buf.extend(chunk)
                if len(buf) == cls.MSG_SIZE:
                    conn.sendall(buf)
            # Abruptly close without reading the rest.
            conn.close()
        except Exception:
            pass
        finally:
            srv.close()

    def test_remaining_requests_are_failures(self):
        port = _free_port()
        stop = threading.Event()
        t = threading.Thread(
            target=self._limited_tcp_server,
            args=(self.HOST, port, stop),
            daemon=True,
        )
        t.start()
        time.sleep(0.2)

        results, elapsed = run_tcp_benchmark(
            host=self.HOST, port=port,
            count=10, size=self.MSG_SIZE, warmup=0, timeout=2.0,
        )
        t.join(timeout=3.0)

        # First ECHO_COUNT requests should succeed; rest should fail.
        successes = [r for r in results if r.success]
        failures  = [r for r in results if not r.success]

        self.assertEqual(len(successes), self.ECHO_COUNT,
                         f"Expected exactly {self.ECHO_COUNT} successes")
        self.assertEqual(len(results), 10,
                         "All 10 requests must be accounted for")
        self.assertTrue(len(failures) > 0,
                        "Some requests must have failed after server closed")


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 9 — Valid CSV generation
# ═════════════════════════════════════════════════════════════════════════════

class TestCsvGeneration(unittest.TestCase, _ServerMixin):
    """Verify per-request and summary CSVs are created and well-formed."""

    TEST_DIR = os.path.join("results", "_test_csv")

    @classmethod
    def setUpClass(cls):
        cls.start_servers(msg_size=64)

    @classmethod
    def tearDownClass(cls):
        cls.stop_servers()
        if os.path.isdir(cls.TEST_DIR):
            shutil.rmtree(cls.TEST_DIR)

    def test_csv_files_created_and_readable(self):
        results, elapsed = run_tcp_benchmark(
            host=self.HOST, port=self.tcp_port,
            count=10, size=64, warmup=2, timeout=2.0,
        )
        metrics = analyze.compute_metrics(
            results=results, elapsed_s=elapsed,
            protocol="tcp", warmup_count=2,
        )
        ts = "test_run"

        # Write CSVs.
        res_path = write_results_csv(results, "tcp", self.TEST_DIR, ts)
        sum_path = write_summary_csv(
            metrics, "tcp", "127.0.0.1", self.tcp_port,
            64, 10, 2, 2.0, self.TEST_DIR, ts,
        )

        self.assertTrue(os.path.isfile(res_path))
        self.assertTrue(os.path.isfile(sum_path))

        # ── Validate per-request CSV ──
        with open(res_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        expected_cols = {"protocol", "sequence", "is_warmup", "success", "rtt_us"}
        self.assertEqual(set(reader.fieldnames), expected_cols)
        # 2 warmup + 10 measured = 12 rows
        self.assertEqual(len(rows), 12)

        # Successful rows must have a numeric rtt_us.
        for row in rows:
            if row["success"] == "True":
                self.assertTrue(float(row["rtt_us"]) > 0)

        # ── Validate summary CSV ──
        with open(sum_path, newline="") as f:
            reader = csv.DictReader(f)
            summary_rows = {row["key"]: row["value"] for row in reader}

        self.assertEqual(summary_rows["protocol"], "tcp")
        self.assertEqual(summary_rows["count"], "10")
        self.assertEqual(summary_rows["warmup"], "2")
        self.assertTrue(float(summary_rows["avg_rtt_us"]) > 0)

    def test_csv_roundtrip_via_analyzer(self):
        """Write CSVs, then read them back with analyze.read_results_csv."""
        results, elapsed = run_tcp_benchmark(
            host=self.HOST, port=self.tcp_port,
            count=5, size=64, warmup=0, timeout=2.0,
        )
        ts = "roundtrip"
        res_path = write_results_csv(results, "tcp", self.TEST_DIR, ts)
        loaded = analyze.read_results_csv(res_path)

        self.assertEqual(len(loaded), len(results))
        for orig, loaded_s in zip(results, loaded):
            self.assertEqual(orig.sequence, loaded_s.sequence)
            self.assertEqual(orig.success, loaded_s.success)
            self.assertEqual(orig.is_warmup, loaded_s.is_warmup)
            if orig.rtt_us is not None:
                self.assertAlmostEqual(orig.rtt_us, loaded_s.rtt_us, places=1)


# ═════════════════════════════════════════════════════════════════════════════
# Test Case 10 — Both localhost and configurable host support
# ═════════════════════════════════════════════════════════════════════════════

class TestConfigurableHost(unittest.TestCase):
    """CLI parsers must accept --host with various values."""

    def test_server_parser_accepts_custom_host(self):
        from server import build_arg_parser
        parser = build_arg_parser()

        args = parser.parse_args(["--host", "0.0.0.0"])
        self.assertEqual(args.host, "0.0.0.0")

        args = parser.parse_args(["--host", "192.168.1.100"])
        self.assertEqual(args.host, "192.168.1.100")

    def test_benchmark_parser_accepts_custom_host(self):
        from benchmark import build_arg_parser
        parser = build_arg_parser()

        args = parser.parse_args(["--protocol", "tcp", "--host", "10.0.0.1"])
        self.assertEqual(args.host, "10.0.0.1")

    def test_default_is_localhost(self):
        from server import build_arg_parser as srv_parser
        from benchmark import build_arg_parser as bench_parser

        self.assertEqual(srv_parser().parse_args([]).host, "127.0.0.1")
        self.assertEqual(bench_parser().parse_args(["--protocol", "tcp"]).host,
                         "127.0.0.1")

    def test_server_binds_to_localhost(self):
        """Server must bind successfully on 127.0.0.1 (basic sanity)."""
        port = _free_port()
        stop = threading.Event()
        t = threading.Thread(
            target=run_tcp_server,
            args=("127.0.0.1", port, 64, stop),
            daemon=True,
        )
        t.start()
        time.sleep(0.2)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                pass  # connection succeeded
        finally:
            stop.set()
            t.join(timeout=3.0)


# ═════════════════════════════════════════════════════════════════════════════
# Runner
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
