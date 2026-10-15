# XBench

A lightweight command-line tool for comparing **TCP vs UDP** network performance through configurable client-server benchmarks.

---

## Overview and Motivation

Understanding the performance differences between TCP and UDP is fundamental to network engineering.  XBench provides a hands-on way to measure and compare:

- **Round-trip latency** (average, median, tail percentiles)
- **Throughput** (message exchanges per second)
- **Application-level response loss** (timed-out or mismatched replies)

The tool is designed to demonstrate core networking concepts — socket programming, transport-layer protocol differences, byte-stream framing, and performance analysis — through a clean, minimal Python codebase.

---

## Architecture

```
XBench/
├── server.py         # TCP + UDP echo servers
├── benchmark.py      # Benchmark client (RTT measurement + CSV export)
├── analyze.py        # Statistics, CSV reading, and graph generation
├── test_xbench.py    # Comprehensive test suite (23 tests)
├── requirements.txt
├── README.md
└── results/          # Auto-created at runtime; CSV and PNG outputs
```

### Data Flow

```
                    ┌─────────────────────┐
                    │   server.py          │
                    │  TCP echo (port 5000)│
   benchmark.py ──>│  UDP echo (port 5001)│──> echo response
   (send message)  └─────────────────────┘    (same payload)
        │
        ▼
   Measure RTT per request
        │
        ├──> Terminal summary
        ├──> results/{protocol}_results_{ts}.csv   (per-request)
        └──> results/{protocol}_summary_{ts}.csv   (aggregate)

   analyze.py
        │
        ├──> Re-read CSVs, recompute metrics, print summaries
        └──> Generate PNG comparison charts (RTT + throughput)
```

1. Start the TCP and/or UDP echo server (`server.py`).
2. Run the benchmark client (`benchmark.py`) — it sends fixed-size messages, records RTT, and writes CSV files.
3. Run the analyzer (`analyze.py`) — it reads the CSV output and produces statistics and comparison graphs.

---

## Tech Stack

- **Python 3** — core implementation
- `socket` — TCP (`AF_INET + SOCK_STREAM`) and UDP (`AF_INET + SOCK_DGRAM`) communication
- `struct` — binary payload encoding (4-byte big-endian sequence number header)
- `time.perf_counter_ns()` — high-resolution monotonic RTT measurement
- `statistics` — mean, median, and percentile calculations
- `argparse` — command-line configuration
- `csv` — result export and import
- `matplotlib` — graph generation (only external dependency)
- `unittest` — comprehensive test suite

---

## Installation (Windows 10)

```powershell
# 1. Clone or download the project
cd D:\XBench

# 2. (Optional) Create a virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# 3. Install the only external dependency
pip install -r requirements.txt
```

---

## Usage

### Start the servers

```powershell
# Start both TCP (port 5000) and UDP (port 5001) echo servers
python server.py

# Start TCP only on a custom port
python server.py --mode tcp --tcp-port 6000

# Bind to a specific network interface (e.g. for LAN testing)
python server.py --host 0.0.0.0
```

### Run a benchmark

```powershell
# TCP benchmark — 1,000 messages, 64-byte payloads (defaults)
python benchmark.py --protocol tcp

# UDP benchmark — 500 messages, 128-byte payloads, 50 warm-ups
python benchmark.py --protocol udp --count 500 --size 128 --warmup 50

# Connect to a remote server
python benchmark.py --protocol tcp --host 192.168.1.42 --tcp-port 5000
```

### Analyze results

```powershell
# Auto-detect the latest CSVs in the results/ directory and generate graphs
python analyze.py

# Point to specific CSV files
python analyze.py --tcp-csv results/tcp_results_20261015_115104.csv --udp-csv results/udp_results_20261015_115119.csv

# Print stats only, skip graph generation
python analyze.py --no-graphs
```

### Run the test suite

```powershell
# Run all 23 tests with verbose output
python -m unittest test_xbench -v
```

---

## Payload Format

Every message carries a **4-byte big-endian unsigned 32-bit sequence number** followed by zero-padding bytes to reach the configured size:

```
[0:4]    — sequence number  (uint32, big-endian, struct ">I")
[4:size] — zero-padding     (0x00 bytes)
```

The sequence number allows the client to validate that the echoed response matches the request it sent. This is especially important for UDP, where datagrams can arrive out of order, be duplicated, or be lost.

---

## Metrics and Formulas

| Metric | Formula |
|---|---|
| Average RTT | sum(successful RTTs) / successful count |
| Median RTT | middle value of sorted successful RTTs |
| P95 RTT | value at index ceil(0.95 x n) in sorted successful RTTs |
| P99 RTT | value at index ceil(0.99 x n) in sorted successful RTTs |
| Throughput | successful exchanges / elapsed seconds |
| Response loss | (timed-out + mismatched) / attempted x 100 % |

RTT units: **microseconds (us)**. Throughput units: **messages/second**.

> **Note:** Response loss is an *application-level* metric. It counts requests that did not receive a valid echo within the configured timeout. It is **not** the same as physical network packet loss (which requires ICMP tools like `ping` or packet capture utilities).

---

## Example Output

### TCP Benchmark (1,000 messages, 64 bytes, localhost)

```
XBench Benchmark Client
  Protocol : TCP
  Target   : 127.0.0.1:5000
  Messages : 1000  (+100 warm-up, discarded)
  Size     : 64 bytes
  Timeout  : 2.0 s

[TCP] Connecting to 127.0.0.1:5000 (size=64B, warmup=100, count=1000, timeout=2.0s) ...
[TCP] Progress: 0/1000  rtt=59.4us
[TCP] Progress: 100/1000  rtt=54.2us
[TCP] Progress: 200/1000  rtt=48.7us
...
[TCP] Connection closed.

====================================================
  XBench Results  [TCP]
====================================================
  Protocol        : TCP
  Warm-up (excl.) : 100 requests
  Measurement     : 1000 requests
  Elapsed         : 0.058 s
----------------------------------------------------
  Attempted       : 1000
  Successful      : 1000
  Failed          : 0
----------------------------------------------------
  Avg RTT         : 52.34 us
  Median RTT      : 49.80 us
  P95 RTT         : 72.10 us
  P99 RTT         : 95.50 us
  Min RTT         : 35.20 us
  Max RTT         : 150.30 us
----------------------------------------------------
  Throughput      : 17241.4 msg/s
  Response loss   : 0.00%
----------------------------------------------------
  NOTE: Response loss = requests with no valid echo within the
  timeout window.  This is an APPLICATION-LAYER metric measured
  by the client.  It is NOT physical network packet loss (ICMP
  ping, traceroute, or packet capture tools measure that).
  On localhost both values are normally 0%.
====================================================

[CSV] Per-request results -> results\tcp_results_20261015_115104.csv
[CSV] Summary -> results\tcp_summary_20261015_115104.csv
```

### Generated CSV (per-request excerpt)

```csv
protocol,sequence,is_warmup,success,rtt_us
tcp,0,True,True,336.90
tcp,1,True,True,78.50
tcp,2,True,True,57.50
...
tcp,100,False,True,59.40
tcp,101,False,True,54.20
```

### Generated CSV (summary)

```csv
key,value
protocol,tcp
host,127.0.0.1
port,5000
size_bytes,64
count,1000
warmup,100
timeout_s,2.0
elapsed_s,0.058
attempted,1000
successful,1000
failed,0
avg_rtt_us,52.34
median_rtt_us,49.80
p95_rtt_us,72.10
p99_rtt_us,95.50
throughput_msg_per_sec,17241.4
response_loss_pct,0.00
```

---

## TCP vs UDP — Key Differences

| Property | TCP | UDP |
|---|---|---|
| Connection | Yes (3-way handshake, teardown) | No (connectionless) |
| Reliability | Guaranteed delivery + ordering | Best-effort, no guarantees |
| Framing | Byte stream (must re-frame manually) | Datagram boundaries preserved |
| Typical RTT | Slightly higher (connection overhead) | Slightly lower (no handshake) |
| Loss handling | Retransmitted automatically by OS | Lost silently -> response loss |
| Use cases | HTTP, file transfer, databases | DNS, video streaming, games |

On localhost, TCP and UDP latencies are very similar because the loopback interface has negligible delay. The key differences become more visible on real networks with packet loss and congestion.

---

## Limitations

- **Localhost only (initially):** All measurements are performed over the loopback interface (`127.0.0.1`), which bypasses the physical network stack. Real-world latency will be higher and more variable.
- **Python timing resolution:** `time.perf_counter_ns()` has sub-microsecond resolution on modern hardware, but Python's GIL and OS scheduling add jitter. Results reflect application-layer RTT, not raw network latency.
- **Single-threaded, synchronous:** One request is outstanding at a time. This is intentional — the goal is latency measurement, not maximum throughput stress-testing.
- **No TLS/encryption:** Plain sockets only.
- **Windows focus:** Developed and tested on Windows 10. Should work on other platforms but is not tested.

---

## Development Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Project setup and CLI scaffolding | Done |
| 2 | TCP and UDP echo servers | Done |
| 3 | Benchmark client and RTT measurement | Done |
| 4 | Performance analytics (percentiles, throughput, loss) | Done |
| 5 | CSV export and matplotlib graphs | Done |
| 6 | Testing, documentation, and GitHub | Done |
