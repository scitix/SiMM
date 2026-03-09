#!/usr/bin/env python3
"""simm_analyse_metrics.py

A small, dependency-free helper to summarize SiMM Prometheus-style metrics.

It fetches the Prometheus text exposition from a /metrics endpoint and prints:
- module list
- per-module totals (requests/errors/read/written)
- top N request types and (if present) latency quantiles
- top N error types

It can also compute per-second rates (IOPS / throughput) by doing two scrapes.
Defaults to 1 second between scrapes. Can be configured.

Usage:
  ./tools/simm_analyse_metrics.py
  ./tools/simm_analyse_metrics.py --url http://127.0.0.1:9464/metrics
  ./tools/simm_analyse_metrics.py --rate --interval 1.0
  TOPN=20 ./tools/simm_analyse_metrics.py

No third-party packages required.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_URL = "http://127.0.0.1:9464/metrics"

# SiMM standard metric suffixes we care about.
SIMM_SUFFIXES = (
    "requests_total",
    "errors_total",
    "read_bytes_total",
    "written_bytes_total",
    "request_duration_microseconds",
)

# Precompile a conservative metric-name regex (Prometheus allows more, but this is fine here).
_METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


@dataclass(frozen=True)
class Sample:
    name: str
    labels: Dict[str, str]
    value: float


def fetch(url: str, timeout_s: float = 3.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="replace")


def parse_labels(label_text: str) -> Dict[str, str]:
    """Parse labels like: instance="1.2.3.4",type="get"."""
    labels: Dict[str, str] = {}
    if not label_text:
        return labels

    # This is not a full Prometheus parser, but good enough for typical output.
    # Handles escaped quotes and backslashes.
    i = 0
    n = len(label_text)
    while i < n:
        # key
        j = label_text.find("=\"", i)
        if j == -1:
            break
        key = label_text[i:j].strip().rstrip(",")
        i = j + 2

        # value until unescaped quote
        val_chars: List[str] = []
        esc = False
        while i < n:
            ch = label_text[i]
            i += 1
            if esc:
                val_chars.append(ch)
                esc = False
                continue
            if ch == "\\":
                esc = True
                continue
            if ch == '"':
                break
            val_chars.append(ch)
        labels[key] = "".join(val_chars)

        # skip comma + spaces
        while i < n and label_text[i] in ", ":
            i += 1

    return labels


def parse_exposition(text: str) -> Iterable[Sample]:
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        # Split at first whitespace (metric+labels then value).
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue

        metric_and_labels, value_str = parts

        # Extract labels if present
        if "{" in metric_and_labels and metric_and_labels.endswith("}"):
            name, label_part = metric_and_labels.split("{", 1)
            label_part = label_part[:-1]
            labels = parse_labels(label_part)
        else:
            name = metric_and_labels
            labels = {}

        if not name or not _METRIC_NAME_RE.match(name):
            continue

        # Prometheus exposition sometimes prints NaN
        try:
            value = float(value_str)
        except ValueError:
            try:
                value = float(value_str.lower())
            except Exception:
                continue

        yield Sample(name=name, labels=labels, value=value)


def module_from_metric(metric_name: str) -> Optional[str]:
    for suf in SIMM_SUFFIXES:
        tail = "_" + suf
        if metric_name.endswith(tail):
            return metric_name[: -len(tail)]
    return None


def top_n(counter_map: Dict[str, float], n: int) -> List[Tuple[str, float]]:
    return sorted(counter_map.items(), key=lambda kv: (-kv[1], kv[0]))[:n]


def mib_per_s(bytes_per_s: float) -> float:
    return bytes_per_s / (1024.0 * 1024.0)


def aggregate(text: str):
    """Aggregate exposition text into structures used by both snapshot and rate modes."""
    modules = set()
    requests_total = defaultdict(float)
    errors_total = defaultdict(float)
    read_bytes_total = defaultdict(float)
    written_bytes_total = defaultdict(float)

    req_type = defaultdict(lambda: defaultdict(float))  # module -> type -> value
    err_type = defaultdict(lambda: defaultdict(float))

    # latency quantiles: module -> type -> quantile -> value
    lat_q: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))

    for s in parse_exposition(text):
        mod = module_from_metric(s.name)
        if not mod:
            continue
        modules.add(mod)

        if s.name.endswith("_requests_total"):
            requests_total[mod] += s.value
            t = s.labels.get("type")
            if t:
                req_type[mod][t] += s.value

        elif s.name.endswith("_errors_total"):
            errors_total[mod] += s.value
            t = s.labels.get("type")
            if t:
                err_type[mod][t] += s.value

        elif s.name.endswith("_read_bytes_total"):
            read_bytes_total[mod] += s.value

        elif s.name.endswith("_written_bytes_total"):
            written_bytes_total[mod] += s.value

        elif s.name.endswith("_request_duration_microseconds"):
            t = s.labels.get("type")
            q = s.labels.get("quantile")
            if t and q:
                lat_q[mod][t][q] = s.value

    return modules, requests_total, errors_total, read_bytes_total, written_bytes_total, req_type, err_type, lat_q


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="Summarize SiMM /metrics output")
    parser.add_argument(
        "--url",
        default=os.environ.get("SIMM_METRICS_URL", DEFAULT_URL),
        help=f"metrics endpoint url (default: {DEFAULT_URL} or $SIMM_METRICS_URL)",
    )
    parser.add_argument(
        "--topn",
        type=int,
        default=int(os.environ.get("TOPN", "10")),
        help="top N request/error types to show per module (default: 10 or $TOPN)",
    )
    parser.add_argument(
        "--rate",
        action="store_true",
        help="compute per-second rates by doing two scrapes (IOPS/throughput)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("INTERVAL", "1.0")),
        help="seconds between two scrapes for --rate (default: 1.0 or $INTERVAL)",
    )
    args = parser.parse_args(argv)

    if args.interval <= 0:
        print("error: --interval must be > 0", file=sys.stderr)
        return 2

    try:
        text1 = fetch(args.url)
    except Exception as e:
        print(f"error: failed to fetch metrics from {args.url}: {e}", file=sys.stderr)
        return 2

    if args.rate:
        t1 = time.time()
        time.sleep(args.interval)
        try:
            text2 = fetch(args.url)
        except Exception as e:
            print(f"error: failed to fetch metrics from {args.url}: {e}", file=sys.stderr)
            return 2
        t2 = time.time()
        dt = max(1e-6, t2 - t1)

        (m1, req1, err1, rb1, wb1, reqt1, errt1, _lat1) = aggregate(text1)
        (m2, req2, err2, rb2, wb2, reqt2, errt2, _lat2) = aggregate(text2)

        modules_sorted = sorted(m1 | m2)

        print("== SiMM metrics rates ==")
        print(f"source: {args.url}")
        print(f"interval: {dt:.3f}s")
        print(f"TOPN: {args.topn}")
        print(f"modules: {len(modules_sorted)}")

        for mod in modules_sorted:
            d_req = req2[mod] - req1[mod]
            d_err = err2[mod] - err1[mod]
            d_rb = rb2[mod] - rb1[mod]
            d_wb = wb2[mod] - wb1[mod]

            # Handle restarts / counter reset
            if d_req < 0:
                d_req = 0.0
            if d_err < 0:
                d_err = 0.0
            if d_rb < 0:
                d_rb = 0.0
            if d_wb < 0:
                d_wb = 0.0

            iops = d_req / dt
            eps = d_err / dt
            r_mibs = mib_per_s(d_rb / dt)
            w_mibs = mib_per_s(d_wb / dt)

            print(f"\n== {mod} ==")
            print(f"  IOPS(req/s): {iops:.2f}")
            print(f"  errors/s: {eps:.2f}")
            print(f"  read:  {r_mibs:.2f} MiB/s")
            print(f"  write: {w_mibs:.2f} MiB/s")

            # Per-type request rates (topn by rate)
            per_type_rate: Dict[str, float] = {}
            for t in set(reqt1.get(mod, {}).keys()) | set(reqt2.get(mod, {}).keys()):
                dv = reqt2[mod].get(t, 0.0) - reqt1[mod].get(t, 0.0)
                if dv < 0:
                    dv = 0.0
                per_type_rate[t] = dv / dt

            top_req_rate = top_n(per_type_rate, args.topn)
            print("-- Top request types (req/s) --")
            if not top_req_rate:
                print("  (none)")
            else:
                for t, v in top_req_rate:
                    print(f"  {t:<32} {v:12.2f}")

            per_err_rate: Dict[str, float] = {}
            for t in set(errt1.get(mod, {}).keys()) | set(errt2.get(mod, {}).keys()):
                dv = errt2[mod].get(t, 0.0) - errt1[mod].get(t, 0.0)
                if dv < 0:
                    dv = 0.0
                per_err_rate[t] = dv / dt

            top_err_rate = top_n(per_err_rate, args.topn)
            print("-- Top error types (err/s) --")
            if not top_err_rate:
                print("  (none)")
            else:
                for t, v in top_err_rate:
                    print(f"  {t:<32} {v:12.2f}")

        return 0

    # Snapshot mode (existing behavior)
    (modules, requests_total, errors_total, read_bytes_total, written_bytes_total, req_type, err_type, lat_q) = aggregate(text1)

    modules_sorted = sorted(modules)

    print("== SiMM metrics summary ==")
    print(f"source: {args.url}")
    print(f"TOPN: {args.topn}")
    print(f"modules: {len(modules_sorted)}")

    for mod in modules_sorted:
        print(f"\n== {mod} ==")
        print(f"  requests_total: {requests_total[mod]:.0f}")
        print(f"  errors_total: {errors_total[mod]:.0f}")
        print(f"  read_bytes_total: {read_bytes_total[mod]:.0f}")
        print(f"  written_bytes_total: {written_bytes_total[mod]:.0f}")

        # Top requests
        print("-- Top request types --")
        top_req = top_n(req_type[mod], args.topn)
        if not top_req:
            print("  (none)")
        else:
            for t, v in top_req:
                print(f"  {t:<32} {v:12.0f}")

        # Latency for those request types
        print("-- Latency for top request types (us) --")
        if not top_req:
            print("  (none)")
        else:
            for t, _ in top_req:
                print(f"  {t}")
                qs = lat_q.get(mod, {}).get(t, {})

                def qv(q: str) -> str:
                    return str(qs.get(q, "-"))

                print(
                    "    "
                    + f"p50={qv('0.5')}  "
                    + f"p90={qv('0.9')}  "
                    + f"p95={qv('0.95')}  "
                    + f"p99={qv('0.99')}  "
                    + f"p999={qv('0.999')}"
                )

        # Top errors
        print("-- Top error types --")
        top_err = top_n(err_type[mod], args.topn)
        if not top_err:
            print("  (none)")
        else:
            for t, v in top_err:
                print(f"  {t:<32} {v:12.0f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
