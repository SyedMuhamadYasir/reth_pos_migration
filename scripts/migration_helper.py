#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any


def load_snapshot(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Could not read snapshot file {path}: {exc}") from exc

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Snapshot file is not valid JSON: {exc}") from exc

    balances = data.get("balances")
    if not isinstance(balances, dict):
        raise RuntimeError("Snapshot JSON missing 'balances' object.")

    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize total required balance from balances_snapshot.json."
    )
    parser.add_argument(
        "snapshot",
        help="Path to balances_snapshot.json",
    )
    parser.add_argument(
        "--allow-failed-snapshot",
        action="store_true",
        help="Allow summary even if snapshot contains failed_addresses.",
    )
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot)
    try:
        data = load_snapshot(snapshot_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    balances: dict[str, Any] = data["balances"]
    failed_addresses = data.get("failed_addresses", {})
    if isinstance(failed_addresses, dict) and failed_addresses and not args.allow_failed_snapshot:
        print(
            "ERROR: snapshot has failed_addresses. Refusing to continue. "
            "Use --allow-failed-snapshot to override.",
            file=sys.stderr,
        )
        sys.exit(2)

    total = 0
    for addr, value in balances.items():
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            print(
                f"ERROR: balance for {addr} is not an integer-like value: {value} ({exc})",
                file=sys.stderr,
            )
            sys.exit(1)
        if parsed < 0:
            print(
                f"ERROR: balance for {addr} is negative ({parsed}); snapshot is invalid.",
                file=sys.stderr,
            )
            sys.exit(1)
        total += parsed

    print(f"Snapshot file: {snapshot_path}")
    print(f"Address count: {len(balances)}")
    if isinstance(failed_addresses, dict):
        print(f"Failed addresses in snapshot: {len(failed_addresses)}")
    print(f"Total required balance (wei): {total}")
    print(f"Hex: {hex(total)}")


if __name__ == "__main__":
    main()
