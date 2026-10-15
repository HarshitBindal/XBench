"""
analyze.py — XBench results analyzer.

Phase 4: Metric calculation functions and terminal summary.
Phase 5: CSV reading, metric recomputation, and matplotlib graph generation.

Metrics computed
================
  Metric              Calculation
  ──────────────────  ──────────────────────────────────────────────────────
  Average RTT         sum(successful RTTs) / successful count
  Median RTT          median of sorted successful RTTs
  P95 RTT             95th percentile of sorted successful RTTs (nearest-rank)
  P99 RTT             99th percentile of sorted successful RTTs (nearest-rank)
  Min / Max RTT       first / last value in sorted successful RTTs
  Throughput          successful exchanges / elapsed measurement seconds
  Response loss       failed / attempted × 100  (application-layer metric)

Division-by-zero safety
=======================
  Every metric that requires at least one successful sample returns None
  rather than raising an exception when the sample set is empty.
  Response loss returns 0.0 when no requests were attempted.
  Throughput returns None when elapsed_s is zero.

Note on "response loss"
=======================
  Response loss counts requests that did not receive a valid, sequence-matched
  echo within the configured timeout.  It is an APPLICATION-LAYER metric.
  It is NOT physical network packet loss (which would require ICMP tools such
  as ping or a packet capture utility).
  On localhost both values should be nearly zero under normal conditions.
"""

import argparse
import csv
import glob
import math
import os
import statistics
from dataclasses import dataclass


# ── Percentile helper ────────────────────────────────────────────────────────

def percentile(sorted_values: list[float], p: float) -> float:
    """
    Compute the p-th percentile from a pre-sorted list using the nearest-rank method.

    Formula: index = ceil(p / 100 × n) − 1   (clamped to [0, n−1])

    The nearest-rank method is simple, deterministic, and always returns a value
    that actually appears in the data (no interpolation).

    Args:
        sorted_values: Ascending-sorted list of floats.
        p:             Percentile to compute, e.g. 95 for the 95th percentile.
                       Must satisfy 0 < p <= 100.

    Returns:
        The nearest-rank p-th percentile value.

    Raises:
        ValueError: sorted_values is empty, or p is out of range.
    """
    if not sorted_values:
        raise ValueError("Cannot compute percentile of an empty list.")
    if not (0 < p <= 100):
        raise ValueError(f"p must be in the range (0, 100]; got {p}.")

    n   = len(sorted_values)
    idx = max(0, min(n - 1, math.ceil(p / 100.0 * n) - 1))
    return sorted_values[idx]


# ── Metrics dataclass ────────────────────────────────────────────────────────

@dataclass
class BenchmarkMetrics:
    """
    All computed performance metrics for one benchmark run.

    RTT fields are in microseconds (us).
    Throughput is in messages per second.
    Response loss is a percentage in [0.0, 100.0].

    Fields set to None indicate that the metric could not be computed
    (e.g., no successful samples exist for RTT fields, or elapsed_s == 0
    for throughput).
    """
    # ── Run identity ──────────────────────────────────────────────────────────
    protocol:               str          # "tcp" or "udp"
    warmup_count:           int          # warm-up requests excluded from metrics
    elapsed_s:              float        # wall-clock seconds of measurement window

    # ── Request counts ────────────────────────────────────────────────────────
    attempted:              int          # total measured (non-warmup) requests
    successful:             int          # echo received and sequence validated
    failed:                 int          # timeout + sequence-mismatch + socket error

    # ── Latency (all in microseconds; None if no successful samples) ──────────
    avg_rtt_us:             float | None
    median_rtt_us:          float | None
    p95_rtt_us:             float | None
    p99_rtt_us:             float | None
    min_rtt_us:             float | None
    max_rtt_us:             float | None

    # ── Throughput (None if elapsed_s == 0 or no successful samples) ─────────
    throughput_msg_per_sec: float | None

    # ── Response loss (always defined) ───────────────────────────────────────
    response_loss_pct:      float


# ── Metric computation ───────────────────────────────────────────────────────

def compute_metrics(
    results: list,          # list[SampleResult] imported from benchmark.py
    elapsed_s: float,
    protocol: str,
    warmup_count: int,
) -> BenchmarkMetrics:
    """
    Derive all performance metrics from a list of SampleResult objects.

    Only MEASURED (non-warmup) samples contribute to any metric.
    This function never raises an exception on empty or all-failed sample sets.

    Args:
        results:      List of SampleResult from run_tcp_benchmark or
                      run_udp_benchmark.  Each has fields:
                          .is_warmup : bool
                          .success   : bool
                          .rtt_us    : float | None
        elapsed_s:    Wall-clock duration of the measurement window in seconds
                      (excludes warm-up time).
        protocol:     "tcp" or "udp", stored for display purposes.
        warmup_count: Number of warm-up requests in the run.

    Returns:
        A fully populated BenchmarkMetrics instance.
    """
    # ── Split into measured / success / failed ────────────────────────────────
    measured   = [r for r in results if not r.is_warmup]
    succeeded  = [r for r in measured if r.success]
    failed_lst = [r for r in measured if not r.success]

    attempted  = len(measured)
    n_success  = len(succeeded)
    n_failed   = len(failed_lst)

    # ── Build sorted RTT list from successful samples only ───────────────────
    # rtt_us is guaranteed non-None for all successful samples (set in benchmark.py).
    rtts: list[float] = sorted(r.rtt_us for r in succeeded)

    # ── RTT metrics ──────────────────────────────────────────────────────────
    if rtts:
        avg_rtt    = sum(rtts) / n_success          # mean
        median_rtt = statistics.median(rtts)         # 50th percentile (std lib)
        p95_rtt    = percentile(rtts, 95)            # nearest-rank 95th
        p99_rtt    = percentile(rtts, 99)            # nearest-rank 99th
        min_rtt    = rtts[0]                         # first after ascending sort
        max_rtt    = rtts[-1]                        # last after ascending sort
    else:
        # No successful samples — all RTT metrics are undefined.
        avg_rtt = median_rtt = p95_rtt = p99_rtt = min_rtt = max_rtt = None

    # ── Throughput ────────────────────────────────────────────────────────────
    # Throughput = successful_exchanges / elapsed_measurement_seconds
    # Undefined when there are no successes or elapsed time is zero/negative.
    if n_success > 0 and elapsed_s > 0:
        throughput: float | None = n_success / elapsed_s
    else:
        throughput = None

    # ── Response loss ─────────────────────────────────────────────────────────
    # loss = (timed-out + sequence-mismatched + socket-errors) / attempted × 100
    # Returns 0.0 when no requests were attempted (avoids division by zero).
    response_loss = (100.0 * n_failed / attempted) if attempted > 0 else 0.0

    return BenchmarkMetrics(
        protocol               = protocol,
        warmup_count           = warmup_count,
        elapsed_s              = elapsed_s,
        attempted              = attempted,
        successful             = n_success,
        failed                 = n_failed,
        avg_rtt_us             = avg_rtt,
        median_rtt_us          = median_rtt,
        p95_rtt_us             = p95_rtt,
        p99_rtt_us             = p99_rtt,
        min_rtt_us             = min_rtt,
        max_rtt_us             = max_rtt,
        throughput_msg_per_sec = throughput,
        response_loss_pct      = response_loss,
    )


# ── Terminal summary ─────────────────────────────────────────────────────────

def print_summary(m: BenchmarkMetrics) -> None:
    """
    Print a compact, clearly labelled terminal summary of benchmark metrics.

    Gracefully handles:
      - Zero successful samples (RTT and throughput fields display "N/A").
      - 100% failure rate (loss displayed as 100.00%).
      - Zero elapsed time (throughput displays "N/A").

    Includes an explicit note distinguishing application response loss from
    physical network packet loss, as required by Phase 4 spec.
    """
    SEP  = "-" * 52
    RSEP = "=" * 52
    proto = m.protocol.upper()

    def _fmt(value: float | None, unit: str = "", decimals: int = 2) -> str:
        """Format a nullable float for display."""
        if value is None:
            return "N/A"
        return f"{value:.{decimals}f}{(' ' + unit) if unit else ''}"

    print()
    print(RSEP)
    print(f"  XBench Results  [{proto}]")
    print(RSEP)

    # ── Run configuration ──────────────────────────────────────────────────
    print(f"  Protocol        : {proto}")
    print(f"  Warm-up (excl.) : {m.warmup_count} requests")
    print(f"  Measurement     : {m.attempted} requests")
    print(f"  Elapsed         : {m.elapsed_s:.3f} s")

    print(SEP)

    # ── Request counts ────────────────────────────────────────────────────
    print(f"  Attempted       : {m.attempted}")
    print(f"  Successful      : {m.successful}")
    print(f"  Failed          : {m.failed}")

    print(SEP)

    # ── Latency ───────────────────────────────────────────────────────────
    print(f"  Avg RTT         : {_fmt(m.avg_rtt_us)} us")
    print(f"  Median RTT      : {_fmt(m.median_rtt_us)} us")
    print(f"  P95 RTT         : {_fmt(m.p95_rtt_us)} us")
    print(f"  P99 RTT         : {_fmt(m.p99_rtt_us)} us")
    print(f"  Min RTT         : {_fmt(m.min_rtt_us)} us")
    print(f"  Max RTT         : {_fmt(m.max_rtt_us)} us")

    print(SEP)

    # ── Throughput & loss ──────────────────────────────────────────────────
    print(f"  Throughput      : {_fmt(m.throughput_msg_per_sec, 'msg/s', 1)}")
    print(f"  Response loss   : {m.response_loss_pct:.2f}%")

    print(SEP)

    # ── Response-loss clarification note ──────────────────────────────────
    print(
        "  NOTE: Response loss = requests with no valid echo within the\n"
        "  timeout window.  This is an APPLICATION-LAYER metric measured\n"
        "  by the client.  It is NOT physical network packet loss (ICMP\n"
        "  ping, traceroute, or packet capture tools measure that).\n"
        "  On localhost both values are normally 0%."
    )

    print(RSEP)
    print()


# ── CSV reading (Phase 5) ──────────────────────────────────────────────────────

@dataclass
class CsvSample:
    """
    Lightweight stand-in for benchmark.SampleResult, reconstructed from CSV.

    Used by read_results_csv() so that compute_metrics() can recompute
    statistics from saved per-request data without importing benchmark.py.
    """
    sequence:  int
    is_warmup: bool
    success:   bool
    rtt_us:    float | None


def read_results_csv(filepath: str) -> list[CsvSample]:
    """
    Read a per-request CSV written by benchmark.write_results_csv().

    Expected columns: protocol, sequence, is_warmup, success, rtt_us

    Args:
        filepath: Path to the CSV file.

    Returns:
        List of CsvSample objects.
    """
    samples: list[CsvSample] = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rtt_str = row["rtt_us"].strip()
            samples.append(CsvSample(
                sequence  = int(row["sequence"]),
                is_warmup = row["is_warmup"].strip() == "True",
                success   = row["success"].strip() == "True",
                rtt_us    = float(rtt_str) if rtt_str else None,
            ))
    return samples


def read_summary_csv(filepath: str) -> dict[str, str]:
    """
    Read a summary CSV written by benchmark.write_summary_csv().

    Returns a dict of {key: value} strings.  The caller can cast
    individual values to the appropriate types as needed.
    """
    data: dict[str, str] = {}
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data[row["key"]] = row["value"]
    return data


def find_latest_csv(results_dir: str, protocol: str) -> str | None:
    """
    Find the most recently modified per-request CSV for *protocol*
    in the given results directory.

    Naming convention: {protocol}_results_*.csv

    Returns:
        Absolute path of the latest file, or None if none found.
    """
    pattern = os.path.join(results_dir, f"{protocol}_results_*.csv")
    matches = glob.glob(pattern)
    if not matches:
        return None
    # Sort by last-modified time (most recent last) and return the newest.
    matches.sort(key=os.path.getmtime)
    return matches[-1]


# ── Graph generation (Phase 5) ─────────────────────────────────────────────────

def generate_rtt_chart(
    tcp_metrics: BenchmarkMetrics,
    udp_metrics: BenchmarkMetrics,
    output_dir: str,
) -> str:
    """
    Create a grouped bar chart comparing TCP vs UDP latency.

    Bars: Average RTT, P95 RTT, P99 RTT for each protocol.
    Y-axis: microseconds.

    Returns:
        The path of the saved PNG file.
    """
    import matplotlib.pyplot as plt

    labels = ["Avg RTT", "P95 RTT", "P99 RTT"]
    tcp_vals = [
        tcp_metrics.avg_rtt_us    or 0,
        tcp_metrics.p95_rtt_us    or 0,
        tcp_metrics.p99_rtt_us    or 0,
    ]
    udp_vals = [
        udp_metrics.avg_rtt_us    or 0,
        udp_metrics.p95_rtt_us    or 0,
        udp_metrics.p99_rtt_us    or 0,
    ]

    x = range(len(labels))
    bar_width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))
    bars_tcp = ax.bar(
        [i - bar_width / 2 for i in x], tcp_vals,
        bar_width, label="TCP", color="#2196F3",
    )
    bars_udp = ax.bar(
        [i + bar_width / 2 for i in x], udp_vals,
        bar_width, label="UDP", color="#FF9800",
    )

    ax.set_ylabel("Round-Trip Time (µs)")
    ax.set_title("XBench — TCP vs UDP Latency")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.legend()

    # Add value labels on top of each bar.
    for bar in bars_tcp:
        h = bar.get_height()
        if h > 0:
            ax.annotate(f"{h:.1f}",
                        xy=(bar.get_x() + bar.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8)
    for bar in bars_udp:
        h = bar.get_height()
        if h > 0:
            ax.annotate(f"{h:.1f}",
                        xy=(bar.get_x() + bar.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8)

    fig.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, "rtt_comparison.png")
    fig.savefig(filepath, dpi=150)
    plt.close(fig)
    print(f"[Graph] RTT comparison chart -> {filepath}")
    return filepath


def generate_throughput_chart(
    tcp_metrics: BenchmarkMetrics,
    udp_metrics: BenchmarkMetrics,
    output_dir: str,
) -> str:
    """
    Create a bar chart comparing TCP vs UDP throughput.

    Y-axis: completed exchanges per second (msg/s).

    Returns:
        The path of the saved PNG file.
    """
    import matplotlib.pyplot as plt

    protocols = ["TCP", "UDP"]
    values = [
        tcp_metrics.throughput_msg_per_sec or 0,
        udp_metrics.throughput_msg_per_sec or 0,
    ]
    colors = ["#2196F3", "#FF9800"]

    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(protocols, values, color=colors, width=0.5)

    ax.set_ylabel("Throughput (msg/s)")
    ax.set_title("XBench — TCP vs UDP Throughput")

    for bar in bars:
        h = bar.get_height()
        if h > 0:
            ax.annotate(f"{h:.1f}",
                        xy=(bar.get_x() + bar.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9)

    fig.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, "throughput_comparison.png")
    fig.savefig(filepath, dpi=150)
    plt.close(fig)
    print(f"[Graph] Throughput comparison chart -> {filepath}")
    return filepath


# ── Argument parser ─────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    """Return the argument parser for analyze.py."""
    parser = argparse.ArgumentParser(
        prog="analyze.py",
        description=(
            "XBench results analyzer.\n"
            "Reads benchmark CSV output, calculates latency statistics,\n"
            "and generates TCP-vs-UDP comparison graphs."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--results-dir",
        default="results",
        metavar="DIR",
        help="Directory that contains benchmark CSV files produced by benchmark.py.",
    )
    parser.add_argument(
        "--tcp-csv",
        default=None,
        metavar="FILE",
        help=(
            "Explicit path to the TCP per-request CSV file. "
            "If omitted, analyze.py will look for the latest TCP CSV in --results-dir."
        ),
    )
    parser.add_argument(
        "--udp-csv",
        default=None,
        metavar="FILE",
        help=(
            "Explicit path to the UDP per-request CSV file. "
            "If omitted, analyze.py will look for the latest UDP CSV in --results-dir."
        ),
    )
    parser.add_argument(
        "--no-graphs",
        action="store_true",
        default=False,
        help="Print statistics to the terminal only; skip matplotlib graph generation.",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        metavar="DIR",
        help="Directory where generated PNG graph files will be saved.",
    )
    return parser


# ── Helper: load one protocol's data and compute metrics ─────────────────────

def _load_and_compute(
    csv_path: str,
    protocol: str,
) -> BenchmarkMetrics | None:
    """
    Read a per-request CSV and recompute metrics from it.

    The elapsed_s value is estimated from the corresponding summary CSV
    if it exists alongside the results CSV.  When the summary CSV cannot
    be found, elapsed_s is approximated from the RTT samples (sum of
    successful RTTs converted to seconds).

    Returns:
        BenchmarkMetrics if the CSV was read successfully, else None.
    """
    try:
        samples = read_results_csv(csv_path)
    except (OSError, KeyError, ValueError) as exc:
        print(f"  [!] Could not read {csv_path}: {exc}")
        return None

    if not samples:
        print(f"  [!] {csv_path} contains no data rows.")
        return None

    warmup_count = sum(1 for s in samples if s.is_warmup)

    # Try to recover elapsed_s from the companion summary CSV.
    # Convention: results CSV is  {proto}_results_{ts}.csv
    #             summary CSV is  {proto}_summary_{ts}.csv
    elapsed_s = 0.0
    results_basename = os.path.basename(csv_path)
    summary_basename = results_basename.replace("_results_", "_summary_")
    summary_path = os.path.join(os.path.dirname(csv_path), summary_basename)
    if os.path.isfile(summary_path):
        try:
            summary = read_summary_csv(summary_path)
            elapsed_s = float(summary.get("elapsed_s", "0"))
        except (OSError, ValueError):
            pass

    # Fallback: estimate elapsed_s from RTT sum when summary is missing.
    if elapsed_s <= 0:
        measured_rtts = [s.rtt_us for s in samples
                        if not s.is_warmup and s.success and s.rtt_us is not None]
        elapsed_s = sum(measured_rtts) / 1_000_000 if measured_rtts else 0.0

    return compute_metrics(
        results      = samples,
        elapsed_s    = elapsed_s,
        protocol     = protocol,
        warmup_count = warmup_count,
    )


# ── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_arg_parser()
    args   = parser.parse_args()

    print("XBench Analyzer")
    print(f"  Results dir  : {args.results_dir}")
    print(f"  TCP CSV      : {args.tcp_csv or '(auto-detect from results dir)'}")
    print(f"  UDP CSV      : {args.udp_csv or '(auto-detect from results dir)'}")
    print(f"  Graphs       : {'disabled (--no-graphs)' if args.no_graphs else 'enabled'}")
    print(f"  Output dir   : {args.output_dir}")
    print()

    # ── Resolve CSV paths ───────────────────────────────────────────────
    tcp_csv = args.tcp_csv or find_latest_csv(args.results_dir, "tcp")
    udp_csv = args.udp_csv or find_latest_csv(args.results_dir, "udp")

    if not tcp_csv and not udp_csv:
        print("No benchmark CSV files found.")
        print(f"Run benchmark.py first, or specify --tcp-csv / --udp-csv.")
        return

    # ── Load and compute metrics ────────────────────────────────────────
    tcp_metrics: BenchmarkMetrics | None = None
    udp_metrics: BenchmarkMetrics | None = None

    if tcp_csv:
        print(f"Loading TCP data: {tcp_csv}")
        tcp_metrics = _load_and_compute(tcp_csv, "tcp")
        if tcp_metrics:
            print_summary(tcp_metrics)

    if udp_csv:
        print(f"Loading UDP data: {udp_csv}")
        udp_metrics = _load_and_compute(udp_csv, "udp")
        if udp_metrics:
            print_summary(udp_metrics)

    # ── Generate comparison graphs ──────────────────────────────────────
    if args.no_graphs:
        print("Graph generation skipped (--no-graphs).")
        return

    if tcp_metrics and udp_metrics:
        print()
        print("Generating comparison graphs ...")
        generate_rtt_chart(tcp_metrics, udp_metrics, args.output_dir)
        generate_throughput_chart(tcp_metrics, udp_metrics, args.output_dir)
        print("Done.")
    else:
        missing = []
        if not tcp_metrics:
            missing.append("TCP")
        if not udp_metrics:
            missing.append("UDP")
        print(
            f"Comparison graphs require both TCP and UDP data.  "
            f"Missing: {', '.join(missing)}.  Skipping graph generation."
        )


if __name__ == "__main__":
    main()
