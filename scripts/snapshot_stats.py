#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

WEI_PER_ETH = 10**18
WEI_PER_ETH_DEC = Decimal(WEI_PER_ETH)
DEFAULT_THRESHOLDS_ETH = [
    Decimal("1"),
    Decimal("10"),
    Decimal("100"),
    Decimal("1000"),
    Decimal("10000"),
    Decimal("100000"),
]


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Offline analytics for balances_snapshot.json. "
            "Reads local snapshot data only (no RPC calls)."
        )
    )
    parser.add_argument(
        "--snapshot",
        default="balances_snapshot.json",
        help="Path to snapshot JSON file. Default: balances_snapshot.json",
    )
    parser.add_argument(
        "--thresholds",
        help=(
            "Optional comma-separated ETH thresholds. "
            "Example: --thresholds 1,10,100,1000,100000"
        ),
    )
    parser.add_argument(
        "--csv-out",
        help="Optional CSV output path for threshold stats.",
    )
    parser.add_argument(
        "--less-than-eth",
        type=Decimal,
        default=Decimal("200000"),
        help=(
            "Count how many addresses have balance less than this ETH value. "
            "Default: 200000"
        ),
    )
    return parser.parse_args()


def parse_eth_thresholds(raw: str | None) -> list[Decimal]:
    if raw is None:
        return DEFAULT_THRESHOLDS_ETH.copy()

    parts = [part.strip() for part in raw.split(",")]
    if not parts or any(part == "" for part in parts):
        fail("Invalid --thresholds format. Use comma-separated ETH values like 1,10,100")

    parsed: list[Decimal] = []
    for part in parts:
        try:
            value = Decimal(part)
        except InvalidOperation as exc:
            fail(f"Invalid ETH threshold value: {part} ({exc})")
        if value < 0:
            fail(f"ETH threshold cannot be negative: {part}")
        parsed.append(value)

    unique_sorted = sorted(set(parsed))
    return unique_sorted


def eth_decimal_to_wei(value_eth: Decimal) -> int:
    wei_dec = value_eth * WEI_PER_ETH_DEC
    wei_integral = wei_dec.to_integral_value()
    if wei_dec != wei_integral:
        fail(
            f"ETH value {value_eth} cannot be represented exactly in wei. "
            "Use up to 18 decimal places."
        )
    if wei_integral < 0:
        fail(f"ETH value must be non-negative: {value_eth}")
    return int(wei_integral)


def load_snapshot(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        fail(f"Could not read snapshot file '{path}': {exc}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        fail(f"Snapshot file is not valid JSON: {exc}")

    if "balances" not in data or "address_counts" not in data:
        fail("Snapshot must contain top-level keys 'balances' and 'address_counts'.")
    if not isinstance(data["balances"], dict):
        fail("Snapshot 'balances' must be a JSON object.")
    if not isinstance(data["address_counts"], dict):
        fail("Snapshot 'address_counts' must be a JSON object.")

    return data


def parse_balances(raw_balances: dict[str, Any]) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for address, value in raw_balances.items():
        if not isinstance(address, str):
            fail(f"Snapshot 'balances' contains non-string address key: {address!r}")
        if not isinstance(value, str):
            fail(f"Balance for address {address} must be a decimal string, got {type(value).__name__}")
        try:
            wei = int(value)
        except ValueError as exc:
            fail(f"Balance for address {address} is not a valid decimal integer string: {value} ({exc})")
        if wei < 0:
            fail(f"Balance for address {address} cannot be negative: {wei}")
        parsed[address] = wei
    return parsed


def format_eth_from_wei(wei: int, places: int = 6) -> str:
    eth = Decimal(wei) / WEI_PER_ETH_DEC
    return f"{eth:.{places}f}"


def format_eth_decimal(value_eth: Decimal, places: int = 6) -> str:
    return f"{value_eth:.{places}f}"


def format_share(share: float) -> str:
    return f"{share:.6f} ({share * 100:.2f}%)"


def compute_gini(values: list[int]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    total = sum(values)
    if total == 0:
        return 0.0
    sorted_vals = sorted(values)
    weighted_sum = 0
    for idx, value in enumerate(sorted_vals, start=1):
        weighted_sum += idx * value
    gini = (2 * weighted_sum) / (n * total) - (n + 1) / n
    if gini < 0:
        return 0.0
    if gini > 1:
        return 1.0
    return float(gini)


def compute_top_share(sorted_balances_desc: list[int], k: int, total_supply_wei: int) -> float:
    if k <= 0 or total_supply_wei == 0:
        return 0.0
    capped = min(k, len(sorted_balances_desc))
    supply = sum(sorted_balances_desc[:capped])
    return supply / total_supply_wei


def compute_threshold_rows(
    balances_wei: list[int],
    thresholds_eth: list[Decimal],
    total_supply_wei: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for threshold_eth in thresholds_eth:
        threshold_wei = eth_decimal_to_wei(threshold_eth)
        address_count = 0
        total_balance_wei = 0
        for balance in balances_wei:
            if balance >= threshold_wei:
                address_count += 1
                total_balance_wei += balance
        share = (total_balance_wei / total_supply_wei) if total_supply_wei > 0 else 0.0
        rows.append(
            {
                "threshold_eth": threshold_eth,
                "address_count": address_count,
                "total_balance_wei": total_balance_wei,
                "total_balance_eth": Decimal(total_balance_wei) / WEI_PER_ETH_DEC,
                "share_of_supply": share,
            }
        )
    return rows


def compute_histogram(balances_wei: list[int]) -> list[dict[str, Any]]:
    bucket_specs: list[tuple[str, int | None, int | None]] = [
        ("== 0 ETH", None, 0),
        ("(0, 1e-6] ETH", 0, eth_decimal_to_wei(Decimal("0.000001"))),
        ("(1e-6, 1e-3] ETH", eth_decimal_to_wei(Decimal("0.000001")), eth_decimal_to_wei(Decimal("0.001"))),
        ("(1e-3, 1] ETH", eth_decimal_to_wei(Decimal("0.001")), eth_decimal_to_wei(Decimal("1"))),
        ("(1, 10] ETH", eth_decimal_to_wei(Decimal("1")), eth_decimal_to_wei(Decimal("10"))),
        ("(10, 100] ETH", eth_decimal_to_wei(Decimal("10")), eth_decimal_to_wei(Decimal("100"))),
        ("(100, 1000] ETH", eth_decimal_to_wei(Decimal("100")), eth_decimal_to_wei(Decimal("1000"))),
        ("(1000, 10000] ETH", eth_decimal_to_wei(Decimal("1000")), eth_decimal_to_wei(Decimal("10000"))),
        ("> 10000 ETH", eth_decimal_to_wei(Decimal("10000")), None),
    ]
    buckets: list[dict[str, Any]] = [
        {"label": label, "count": 0, "total_wei": 0, "lower_exclusive": low, "upper_inclusive": high}
        for label, low, high in bucket_specs
    ]

    for value in balances_wei:
        for bucket in buckets:
            low = bucket["lower_exclusive"]
            high = bucket["upper_inclusive"]
            if low is None and high == 0:
                if value == 0:
                    bucket["count"] += 1
                    bucket["total_wei"] += value
                    break
                continue
            if high is None:
                if value > low:
                    bucket["count"] += 1
                    bucket["total_wei"] += value
                    break
            else:
                if value > low and value <= high:
                    bucket["count"] += 1
                    bucket["total_wei"] += value
                    break

    return buckets


def write_threshold_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "threshold_eth",
                    "address_count",
                    "total_balance_wei",
                    "total_balance_eth",
                    "share_of_supply",
                ],
            )
            writer.writeheader()
            for row in rows:
                threshold_eth_str = format(row["threshold_eth"], "f")
                writer.writerow(
                    {
                        "threshold_eth": threshold_eth_str.rstrip("0").rstrip(".")
                        if "." in threshold_eth_str
                        else threshold_eth_str,
                        "address_count": row["address_count"],
                        "total_balance_wei": row["total_balance_wei"],
                        "total_balance_eth": format(row["total_balance_eth"], "f"),
                        "share_of_supply": f"{row['share_of_supply']:.12f}",
                    }
                )
    except OSError as exc:
        fail(f"Could not write CSV output '{path}': {exc}")


def main() -> None:
    args = parse_args()

    snapshot_path = Path(args.snapshot)
    snapshot = load_snapshot(snapshot_path)
    balances = parse_balances(snapshot["balances"])
    balances_values = list(balances.values())

    failed_raw = snapshot.get("address_counts", {}).get("failed", 0)
    try:
        failed_count = int(failed_raw)
    except (TypeError, ValueError):
        failed_count = 0
    if failed_count > 0:
        print(
            "WARNING: snapshot has failed addresses (address_counts.failed > 0). "
            "Stats are computed only over the 'balances' map.",
            file=sys.stderr,
        )

    thresholds_eth = parse_eth_thresholds(args.thresholds)
    if args.less_than_eth < 0:
        fail("--less-than-eth must be non-negative.")
    less_than_wei = eth_decimal_to_wei(args.less_than_eth)

    total_addresses = len(balances_values)
    total_supply_wei = sum(balances_values)
    non_zero_addresses = sum(1 for value in balances_values if value > 0)
    zero_addresses = total_addresses - non_zero_addresses

    threshold_rows = compute_threshold_rows(balances_values, thresholds_eth, total_supply_wei)

    sorted_balances_desc = sorted(balances_values, reverse=True)
    top_1_share = compute_top_share(sorted_balances_desc, 1, total_supply_wei)
    top_10_share = compute_top_share(sorted_balances_desc, 10, total_supply_wei)
    top_100_share = compute_top_share(sorted_balances_desc, 100, total_supply_wei)
    top_1pct_share = None
    top_1pct_k = None
    if total_addresses >= 100:
        top_1pct_k = max(1, math.floor(total_addresses * 0.01))
        top_1pct_share = compute_top_share(sorted_balances_desc, top_1pct_k, total_supply_wei)

    gini = compute_gini(balances_values)
    histogram = compute_histogram(balances_values)

    lt_count = sum(1 for value in balances_values if value < less_than_wei)
    lt_share = (lt_count / total_addresses) if total_addresses > 0 else 0.0

    timestamp_unix = snapshot.get("timestamp_unix")
    timestamp_text = None
    if isinstance(timestamp_unix, int):
        timestamp_text = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc).isoformat()

    block = snapshot.get("block") if isinstance(snapshot.get("block"), dict) else {}

    print("=== Snapshot metadata ===")
    print(f"snapshot_file: {snapshot_path}")
    print(f"source_rpc: {snapshot.get('source_rpc')}")
    print(f"chain_id: {snapshot.get('chain_id')}")
    print(f"block_number: {block.get('number')}")
    print(f"block_tag: {block.get('tag')}")
    print(f"block_hash: {block.get('hash')}")
    print(f"block_resolved_from: {block.get('resolved_from')}")
    print(f"timestamp_unix: {timestamp_unix}")
    if timestamp_text:
        print(f"timestamp_utc: {timestamp_text}")
    print()

    print("=== Basic supply stats ===")
    print(f"total_supply_wei: {total_supply_wei}")
    print(f"total_supply_eth: {format_eth_from_wei(total_supply_wei, places=6)}")
    print(f"total_addresses: {total_addresses}")
    print(f"non_zero_addresses: {non_zero_addresses}")
    print(f"zero_addresses: {zero_addresses}")
    print(
        "addresses_below_threshold: "
        f"{lt_count} addresses (< {format_eth_decimal(args.less_than_eth, places=6)} ETH, "
        f"{lt_share * 100:.2f}% of addresses)"
    )
    print()

    print("=== Threshold stats ===")
    for row in threshold_rows:
        print(
            f">= {format_eth_decimal(row['threshold_eth'], places=6)} ETH: "
            f"{row['address_count']} addresses, "
            f"{format(row['total_balance_eth'], 'f')} ETH, "
            f"{row['share_of_supply'] * 100:.2f}% of supply"
        )
    print()

    print("=== Top-holder concentration ===")
    print(f"top_1_share: {format_share(top_1_share)}")
    print(f"top_10_share: {format_share(top_10_share)}")
    print(f"top_100_share: {format_share(top_100_share)}")
    if top_1pct_share is None:
        print("top_1_percent_share: N/A (requires at least 100 addresses)")
    else:
        print(f"top_1_percent_share (top {top_1pct_k}): {format_share(top_1pct_share)}")
    print()

    print("=== Inequality metrics ===")
    print(f"gini_coefficient: {gini:.4f}")
    print()

    print("=== Balance histogram (ETH) ===")
    for bucket in histogram:
        print(
            f"{bucket['label']}: "
            f"count={bucket['count']}, "
            f"total_balance_wei={bucket['total_wei']}, "
            f"total_balance_eth={format_eth_from_wei(bucket['total_wei'], places=6)}"
        )

    if args.csv_out:
        csv_path = Path(args.csv_out)
        write_threshold_csv(csv_path, threshold_rows)
        print()
        print(f"threshold_csv_written: {csv_path}")


if __name__ == "__main__":
    main()
