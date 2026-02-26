#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def load_snapshot(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Could not read snapshot file {path}: {exc}") from exc

    try:
        snapshot = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Snapshot file is not valid JSON: {exc}") from exc

    balances = snapshot.get("balances")
    if not isinstance(balances, dict):
        raise RuntimeError("Snapshot JSON missing 'balances' object.")

    return snapshot


def normalize_address(addr: str) -> str:
    if not (addr.startswith("0x") or addr.startswith("0X")) or len(addr) != 42:
        raise ValueError("must be a 20-byte hex address with 0x prefix")
    try:
        int(addr[2:], 16)
    except ValueError as exc:
        raise ValueError("contains non-hex characters") from exc
    return "0x" + addr[2:].lower()


def load_exclude_set(path: str) -> set[str]:
    excluded: set[str] = set()
    invalid_lines: list[str] = []

    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            for line_number, raw_line in enumerate(file_handle, start=1):
                raw = raw_line.strip()
                if not raw or raw.startswith("#"):
                    continue
                try:
                    normalized = normalize_address(raw)
                except ValueError as exc:
                    invalid_lines.append(f"line {line_number}: {raw} ({exc})")
                    continue
                excluded.add(normalized)
    except OSError as exc:
        print(f"ERROR: failed to read --exclude-addresses-file '{path}': {exc}", file=sys.stderr)
        sys.exit(1)

    if invalid_lines:
        print("ERROR: invalid addresses found in --exclude-addresses-file:", file=sys.stderr)
        for line in invalid_lines[:20]:
            print(f"  {line}", file=sys.stderr)
        if len(invalid_lines) > 20:
            print(f"  ... and {len(invalid_lines) - 20} more invalid lines", file=sys.stderr)
        sys.exit(1)

    return excluded


def import_web3() -> tuple[Any, Any]:
    try:
        from web3 import Web3
        from web3.middleware import geth_poa_middleware
    except ImportError as exc:
        raise RuntimeError(
            "web3 is not installed. Install dependencies with: pip install -r requirements.txt"
        ) from exc
    return Web3, geth_poa_middleware


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify target-chain balances match snapshot exactly."
    )
    parser.add_argument(
        "--snapshot",
        required=True,
        help="Path to balances_snapshot.json",
    )
    parser.add_argument(
        "--min-balance-wei",
        type=int,
        default=0,
        help="Skip addresses with expected snapshot balance below this value. Default: 0",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Print progress every N addresses. Set 0 to disable. Default: 100",
    )
    parser.add_argument(
        "--max-report",
        type=int,
        default=50,
        help="Max mismatch rows to print. Default: 50",
    )
    parser.add_argument(
        "--allow-failed-snapshot",
        action="store_true",
        help="Allow verify even when snapshot has failed_addresses.",
    )
    parser.add_argument(
        "--exclude-addresses-file",
        help=(
            "Optional path to a text file with addresses to exclude from verification "
            "(same format as snapshot input)."
        ),
    )
    args = parser.parse_args()

    if args.min_balance_wei < 0:
        print("ERROR: --min-balance-wei must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.progress_every < 0:
        print("ERROR: --progress-every must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.max_report < 0:
        print("ERROR: --max-report must be >= 0.", file=sys.stderr)
        sys.exit(1)

    try:
        Web3, geth_poa_middleware = import_web3()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    target_rpc = os.getenv("TARGET_RPC_URL")
    if not target_rpc:
        print("ERROR: TARGET_RPC_URL env var is not set.", file=sys.stderr)
        sys.exit(1)

    snapshot_path = Path(args.snapshot)
    try:
        snapshot = load_snapshot(snapshot_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.exclude_addresses_file:
        exclude_set = load_exclude_set(args.exclude_addresses_file)
    else:
        exclude_set = set()

    failed_addresses = snapshot.get("failed_addresses", {})
    if (
        isinstance(failed_addresses, dict)
        and failed_addresses
        and not args.allow_failed_snapshot
    ):
        print(
            "ERROR: snapshot has failed_addresses. Refusing strict verification. "
            "Use --allow-failed-snapshot to override.",
            file=sys.stderr,
        )
        sys.exit(2)

    balances: dict[str, Any] = snapshot["balances"]
    block_meta = snapshot.get("block")

    w3 = Web3(Web3.HTTPProvider(target_rpc))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    if not w3.is_connected():
        print(f"ERROR: failed to connect to target RPC: {target_rpc}", file=sys.stderr)
        sys.exit(1)

    print(f"Verifying against snapshot: {snapshot_path}")
    print(f"Snapshot block metadata: {block_meta}")
    print(f"Target RPC: {target_rpc}")

    total = len(balances)
    checked = 0
    skipped_threshold = 0
    skipped_excluded = 0
    mismatches: list[tuple[str, int, int]] = []
    errors: list[tuple[str, str]] = []

    for idx, (addr, expected_str) in enumerate(balances.items(), start=1):
        if exclude_set and addr in exclude_set:
            skipped_excluded += 1
            if args.progress_every and idx % args.progress_every == 0:
                print(f"  checked {idx}/{total} addresses...")
            continue
        try:
            expected = int(expected_str)
        except (TypeError, ValueError):
            errors.append((addr, f"invalid expected balance value {expected_str!r}"))
            continue
        if expected < 0:
            errors.append((addr, f"invalid negative expected balance {expected}"))
            continue

        if expected < args.min_balance_wei:
            skipped_threshold += 1
            if args.progress_every and idx % args.progress_every == 0:
                print(f"  checked {idx}/{total} addresses...")
            continue

        try:
            got = int(w3.eth.get_balance(addr))
        except Exception as exc:
            errors.append((addr, f"RPC error: {exc}"))
            continue

        checked += 1
        if got != expected:
            mismatches.append((addr, expected, got))

        if args.progress_every and idx % args.progress_every == 0:
            print(f"  checked {idx}/{total} addresses...")

    print("Verification complete.")
    print(f"Total addresses in snapshot: {total}")
    print(f"Checked addresses: {checked}")
    print(f"Skipped (below threshold): {skipped_threshold}")
    print(f"Skipped (excluded-addresses-file): {skipped_excluded}")
    print(f"Mismatches: {len(mismatches)}")
    print(f"Errors: {len(errors)}")

    if mismatches:
        print(f"First {min(args.max_report, len(mismatches))} mismatches:")
        for addr, exp, got in mismatches[: args.max_report]:
            print(f"  {addr} expected {exp}, got {got}")

    if errors:
        print(f"First {min(args.max_report, len(errors))} errors:")
        for addr, message in errors[: args.max_report]:
            print(f"  {addr}: {message}")

    if not mismatches and not errors:
        print("All checked balances match the snapshot exactly.")
        sys.exit(0)

    sys.exit(3)


if __name__ == "__main__":
    main()
