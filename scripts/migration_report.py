#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

WEI_PER_ETH_DEC = Decimal(10**18)
TX_LOG_HEADER = [
    "snapshot_block_number",
    "snapshot_block_tag",
    "snapshot_block_hash",
    "snapshot_chain_id",
    "index",
    "address",
    "expected_balance_wei",
    "current_balance_before_wei",
    "delta_sent_wei",
    "admin_address",
    "admin_nonce",
    "tx_hash",
    "tx_block_number",
    "gas_price_wei",
    "gas_limit",
    "gas_used",
    "tx_status",
]


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Offline migration report generator from snapshot JSON and migration tx CSV log. "
            "No RPC calls are performed."
        )
    )
    parser.add_argument(
        "--snapshot",
        default="balances_snapshot.json",
        help="Path to balances_snapshot.json. Default: balances_snapshot.json",
    )
    parser.add_argument(
        "--tx-log-csv",
        required=True,
        help="Path to migration tx log CSV produced by migrate_balances.py --tx-log-csv.",
    )
    parser.add_argument(
        "--tex-out",
        help="Optional output path for a LaTeX fragment report.",
    )
    return parser.parse_args()


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

    block = data.get("block")
    if not isinstance(block, dict):
        fail("Snapshot must contain a 'block' object.")
    for key in ("number", "tag", "hash"):
        if key not in block:
            fail(f"Snapshot 'block' is missing '{key}'.")
    if "chain_id" not in data:
        fail("Snapshot is missing 'chain_id'.")

    return data


def parse_snapshot_balances(raw_balances: dict[str, Any]) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for address, value in raw_balances.items():
        if not isinstance(address, str):
            fail(f"Snapshot 'balances' contains non-string address key: {address!r}")
        if not isinstance(value, str):
            fail(
                "Balance for address "
                f"{address} must be a decimal string, got {type(value).__name__}"
            )
        try:
            wei = int(value)
        except ValueError as exc:
            fail(
                "Balance for address "
                f"{address} is not a valid decimal integer string: {value} ({exc})"
            )
        if wei < 0:
            fail(f"Balance for address {address} cannot be negative: {wei}")
        parsed[address] = wei
    return parsed


def parse_row_int(value: str, field_name: str, row_number: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        fail(f"Invalid integer in tx log row {row_number}, field '{field_name}': {value!r} ({exc})")


def load_tx_log_rows(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                fail(f"Tx log CSV '{path}' is empty or missing header row.")
            if reader.fieldnames != TX_LOG_HEADER:
                fail(
                    "Tx log CSV header mismatch. "
                    f"Expected {TX_LOG_HEADER}, got {reader.fieldnames}"
                )

            rows: list[dict[str, Any]] = []
            for row_number, row in enumerate(reader, start=2):
                rows.append(
                    {
                        "row_number": row_number,
                        "snapshot_block_number": row["snapshot_block_number"],
                        "snapshot_block_tag": row["snapshot_block_tag"],
                        "snapshot_block_hash": row["snapshot_block_hash"],
                        "snapshot_chain_id": row["snapshot_chain_id"],
                        "index": parse_row_int(row["index"], "index", row_number),
                        "address": row["address"],
                        "expected_balance_wei": parse_row_int(
                            row["expected_balance_wei"], "expected_balance_wei", row_number
                        ),
                        "current_balance_before_wei": parse_row_int(
                            row["current_balance_before_wei"],
                            "current_balance_before_wei",
                            row_number,
                        ),
                        "delta_sent_wei": parse_row_int(
                            row["delta_sent_wei"], "delta_sent_wei", row_number
                        ),
                        "admin_address": row["admin_address"],
                        "admin_nonce": parse_row_int(row["admin_nonce"], "admin_nonce", row_number),
                        "tx_hash": row["tx_hash"],
                        "tx_block_number": parse_row_int(
                            row["tx_block_number"], "tx_block_number", row_number
                        ),
                        "gas_price_wei": parse_row_int(
                            row["gas_price_wei"], "gas_price_wei", row_number
                        ),
                        "gas_limit": parse_row_int(row["gas_limit"], "gas_limit", row_number),
                        "gas_used": parse_row_int(row["gas_used"], "gas_used", row_number),
                        "tx_status": parse_row_int(row["tx_status"], "tx_status", row_number),
                    }
                )
            return rows
    except OSError as exc:
        fail(f"Could not read tx log CSV '{path}': {exc}")


def snapshot_metadata_for_compare(snapshot: dict[str, Any]) -> dict[str, str]:
    block = snapshot["block"]
    return {
        "snapshot_block_number": str(block["number"]),
        "snapshot_block_tag": str(block["tag"]),
        "snapshot_block_hash": str(block["hash"]),
        "snapshot_chain_id": str(snapshot["chain_id"]),
    }


def validate_tx_row_snapshot_metadata(
    rows: list[dict[str, Any]],
    expected_meta: dict[str, str],
) -> None:
    mismatches: list[str] = []
    for row in rows:
        for field, expected in expected_meta.items():
            got = str(row[field])
            if got != expected:
                mismatches.append(
                    f"row {row['row_number']} field {field}: expected {expected!r}, got {got!r}"
                )

    if mismatches:
        print("ERROR: tx log snapshot metadata mismatch detected:", file=sys.stderr)
        for message in mismatches[:20]:
            print(f"  {message}", file=sys.stderr)
        if len(mismatches) > 20:
            print(f"  ... and {len(mismatches) - 20} more mismatches", file=sys.stderr)
        raise SystemExit(1)


def format_eth_from_wei(wei: int, places: int = 6) -> str:
    eth = Decimal(wei) / WEI_PER_ETH_DEC
    return f"{eth:.{places}f}"


def latex_escape(value: str) -> str:
    escaped = value
    escaped = escaped.replace("\\", r"\textbackslash{}")
    escaped = escaped.replace("&", r"\&")
    escaped = escaped.replace("%", r"\%")
    escaped = escaped.replace("$", r"\$")
    escaped = escaped.replace("#", r"\#")
    escaped = escaped.replace("_", r"\_")
    escaped = escaped.replace("{", r"\{")
    escaped = escaped.replace("}", r"\}")
    escaped = escaped.replace("~", r"\textasciitilde{}")
    escaped = escaped.replace("^", r"\textasciicircum{}")
    return escaped


def write_latex_fragment(
    path: Path,
    *,
    snapshot_block_number: str,
    snapshot_chain_id: str,
    total_tx_count: int,
    unique_recipient_addresses: int,
    total_delta_wei: int,
) -> None:
    total_delta_eth = format_eth_from_wei(total_delta_wei, places=6)

    lines = [
        "% Auto generated by scripts/migration_report.py",
        "% Do not edit by hand. Re generate from snapshot and migration tx log CSV.",
        "",
        f"\\newcommand{{\\MigrationSnapshotBlockNumber}}{{{latex_escape(snapshot_block_number)}}}",
        f"\\newcommand{{\\MigrationSnapshotChainId}}{{{latex_escape(snapshot_chain_id)}}}",
        f"\\newcommand{{\\MigrationTotalTxCount}}{{{total_tx_count}}}",
        f"\\newcommand{{\\MigrationUniqueAddresses}}{{{unique_recipient_addresses}}}",
        f"\\newcommand{{\\MigrationTotalDeltaETH}}{{{latex_escape(total_delta_eth)}}}",
        "",
        "\\begin{table}[h]",
        "  \\centering",
        "  \\caption{Migration transaction summary}",
        "  \\begin{tabular}{l l}",
        "    \\hline",
        "    Metric & Value \\\\",
        "    \\hline",
        f"    Total tx count & {total_tx_count} \\\\",
        f"    Unique recipient addresses & {unique_recipient_addresses} \\\\",
        f"    Total delta (ETH) & {latex_escape(total_delta_eth)} \\\\",
        f"    Snapshot block & {latex_escape(snapshot_block_number)} \\\\",
        f"    Snapshot chain ID & {latex_escape(snapshot_chain_id)} \\\\",
        "    \\hline",
        "  \\end{tabular}",
        "\\end{table}",
        "",
    ]

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines), encoding="utf-8")
    except OSError as exc:
        fail(f"Could not write LaTeX output '{path}': {exc}")


def main() -> None:
    args = parse_args()

    snapshot_path = Path(args.snapshot)
    tx_log_csv_path = Path(args.tx_log_csv)

    snapshot = load_snapshot(snapshot_path)
    snapshot_balances = parse_snapshot_balances(snapshot["balances"])
    tx_rows = load_tx_log_rows(tx_log_csv_path)

    expected_meta = snapshot_metadata_for_compare(snapshot)
    validate_tx_row_snapshot_metadata(tx_rows, expected_meta)

    total_snapshot_addresses = len(snapshot_balances)
    total_migrated_tx_rows = len(tx_rows)
    unique_addresses_in_tx_log_set = {str(row["address"]) for row in tx_rows}
    unique_addresses_in_tx_log = len(unique_addresses_in_tx_log_set)
    total_delta_wei = sum(int(row["delta_sent_wei"]) for row in tx_rows)
    total_delta_eth = format_eth_from_wei(total_delta_wei, places=6)

    snapshot_addresses = set(snapshot_balances.keys())
    snapshot_addresses_present = len(snapshot_addresses.intersection(unique_addresses_in_tx_log_set))
    tx_log_addresses_not_in_snapshot = sorted(unique_addresses_in_tx_log_set - snapshot_addresses)

    invariant_errors: list[str] = []
    for row in tx_rows:
        lhs = int(row["current_balance_before_wei"]) + int(row["delta_sent_wei"])
        rhs = int(row["expected_balance_wei"])
        if lhs != rhs:
            invariant_errors.append(
                "row "
                f"{row['row_number']} address={row['address']} "
                f"current_before + delta ({lhs}) != expected ({rhs})"
            )

    print("=== Migration metadata ===")
    print(f"snapshot_file: {snapshot_path}")
    print(f"tx_log_file: {tx_log_csv_path}")
    print(f"snapshot_block_number: {expected_meta['snapshot_block_number']}")
    print(f"snapshot_block_hash: {expected_meta['snapshot_block_hash']}")
    print(f"snapshot_chain_id: {expected_meta['snapshot_chain_id']}")
    print(f"total_snapshot_addresses: {total_snapshot_addresses}")
    print(f"total_migrated_tx_rows: {total_migrated_tx_rows}")
    print(f"unique_addresses_in_tx_log: {unique_addresses_in_tx_log}")
    print(f"total_delta_wei: {total_delta_wei}")
    print(f"total_delta_eth: {total_delta_eth}")

    print("=== Sanity checks ===")
    print(f"snapshot_addresses_present_in_tx_log: {snapshot_addresses_present}")
    print(f"tx_log_addresses_not_in_snapshot: {len(tx_log_addresses_not_in_snapshot)}")
    print(f"row_invariant_failures: {len(invariant_errors)}")

    if tx_log_addresses_not_in_snapshot:
        print(
            "WARNING: tx log contains addresses not present in snapshot "
            f"(count={len(tx_log_addresses_not_in_snapshot)}).",
            file=sys.stderr,
        )
        for address in tx_log_addresses_not_in_snapshot[:20]:
            print(f"  {address}", file=sys.stderr)
        if len(tx_log_addresses_not_in_snapshot) > 20:
            print(
                f"  ... and {len(tx_log_addresses_not_in_snapshot) - 20} more addresses",
                file=sys.stderr,
            )

    if invariant_errors:
        print("ERROR: broken per-row invariant(s) detected in tx log:", file=sys.stderr)
        for message in invariant_errors[:20]:
            print(f"  {message}", file=sys.stderr)
        if len(invariant_errors) > 20:
            print(f"  ... and {len(invariant_errors) - 20} more invariant errors", file=sys.stderr)
        raise SystemExit(1)

    if args.tex_out:
        tex_path = Path(args.tex_out)
        write_latex_fragment(
            tex_path,
            snapshot_block_number=expected_meta["snapshot_block_number"],
            snapshot_chain_id=expected_meta["snapshot_chain_id"],
            total_tx_count=total_migrated_tx_rows,
            unique_recipient_addresses=unique_addresses_in_tx_log,
            total_delta_wei=total_delta_wei,
        )
        print(f"LaTeX fragment written to {tex_path}")


if __name__ == "__main__":
    main()
