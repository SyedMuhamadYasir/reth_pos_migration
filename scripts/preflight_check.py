#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def warn(message: str) -> None:
    print(f"WARN: {message}", file=sys.stderr)


def rpc_call(url: str, method: str, params: list[Any], timeout_seconds: float) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": time.time_ns(),
        "method": method,
        "params": params,
    }
    encoded = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"{method} RPC transport error: {exc}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{method} returned invalid JSON") from exc

    if "error" in data:
        error = data["error"]
        raise RuntimeError(
            f"{method} RPC error code={error.get('code')} message={error.get('message')}"
        )
    if "result" not in data:
        raise RuntimeError(f"{method} RPC response missing 'result'")
    return data["result"]


def normalize_address(addr: str) -> str:
    if not (addr.startswith("0x") or addr.startswith("0X")) or len(addr) != 42:
        raise ValueError("must be a 20-byte hex address with 0x prefix")
    try:
        int(addr[2:], 16)
    except ValueError as exc:
        raise ValueError("contains non-hex characters") from exc
    return "0x" + addr[2:].lower()


def parse_int(value: str, name: str) -> int:
    try:
        out = int(value, 0)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer. Got: {value}") from exc
    if out < 0:
        raise RuntimeError(f"{name} must be non-negative. Got: {value}")
    return out


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_run_config_fingerprint(
    snapshot_path: Path,
    exclude_addresses_file: str | None,
    min_balance_wei: int,
    max_balance_wei: int | None,
) -> tuple[str, str | None, str | None]:
    exclude_path_resolved: str | None = None
    exclude_file_sha256: str | None = None
    if exclude_addresses_file:
        exclude_path = Path(exclude_addresses_file)
        exclude_path_resolved = str(exclude_path.resolve())
        try:
            exclude_file_sha256 = file_sha256(exclude_path)
        except OSError as exc:
            raise RuntimeError(
                f"Could not hash --exclude-addresses-file '{exclude_addresses_file}': {exc}"
            ) from exc

    payload = {
        "snapshot_path": str(snapshot_path.resolve()),
        "exclude_addresses_file": exclude_path_resolved,
        "exclude_addresses_file_sha256": exclude_file_sha256,
        "min_balance_wei": min_balance_wei,
        "max_balance_wei": max_balance_wei,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest(), exclude_path_resolved, exclude_file_sha256


def snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = {
        "source_rpc": snapshot.get("source_rpc"),
        "chain_id": snapshot.get("chain_id"),
        "block": snapshot.get("block"),
        "balances": snapshot["balances"],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def load_snapshot(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Could not read snapshot file {path}: {exc}") from exc

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Snapshot file is not valid JSON: {exc}") from exc

    if not isinstance(data.get("balances"), dict):
        raise RuntimeError("Snapshot JSON missing 'balances' object.")
    if not isinstance(data.get("address_counts"), dict):
        raise RuntimeError("Snapshot JSON missing 'address_counts' object.")
    return data


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
                    excluded.add(normalize_address(raw))
                except ValueError as exc:
                    invalid_lines.append(f"line {line_number}: {raw} ({exc})")
    except OSError as exc:
        raise RuntimeError(f"Could not read --exclude-addresses-file '{path}': {exc}") from exc

    if invalid_lines:
        lines = "\n".join(f"  {line}" for line in invalid_lines[:20])
        extra = ""
        if len(invalid_lines) > 20:
            extra = f"\n  ... and {len(invalid_lines) - 20} more invalid lines"
        raise RuntimeError(
            "Invalid addresses found in --exclude-addresses-file:\n"
            f"{lines}{extra}"
        )

    return excluded


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Could not load state file {path}: {exc}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preflight checks for migration safety and auditability. "
            "Validates RPC availability, snapshot quality, env vars, and state consistency."
        )
    )
    parser.add_argument(
        "--snapshot",
        default="balances_snapshot.json",
        help="Path to balances_snapshot.json. Default: balances_snapshot.json",
    )
    parser.add_argument(
        "--state-file",
        default="migration_state.json",
        help="Path to migration state file. Default: migration_state.json",
    )
    parser.add_argument(
        "--exclude-addresses-file",
        help="Optional path to addresses to exclude. Must match migration run config.",
    )
    parser.add_argument(
        "--min-balance-wei",
        type=int,
        default=0,
        help="Expected min balance threshold for migration/verify scope. Default: 0",
    )
    parser.add_argument(
        "--max-balance-wei",
        type=int,
        default=None,
        help="Expected max balance threshold for migration/verify scope.",
    )
    parser.add_argument(
        "--source-rpc",
        help="Optional SOURCE RPC URL override. Defaults to SOURCE_RPC_URL env var.",
    )
    parser.add_argument(
        "--target-rpc",
        help="Optional TARGET RPC URL override. Defaults to TARGET_RPC_URL env var.",
    )
    parser.add_argument(
        "--probe-block-tag",
        default="latest",
        help="Block tag used for probe calls. Default: latest",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="RPC timeout in seconds. Default: 20",
    )
    parser.add_argument(
        "--require-strict-capability",
        action="store_true",
        help="Fail if debug_accountRange does not return page data.",
    )
    parser.add_argument(
        "--allow-failed-snapshot",
        action="store_true",
        help="Allow snapshot that contains failed_addresses.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.min_balance_wei < 0:
        fail("--min-balance-wei must be >= 0")
    if args.max_balance_wei is not None and args.max_balance_wei < 0:
        fail("--max-balance-wei must be >= 0")
    if args.max_balance_wei is not None and args.max_balance_wei < args.min_balance_wei:
        fail("--max-balance-wei must be >= --min-balance-wei")
    if args.timeout_seconds <= 0:
        fail("--timeout-seconds must be > 0")

    snapshot_path = Path(args.snapshot)
    state_file = Path(args.state_file)

    try:
        snapshot = load_snapshot(snapshot_path)
    except RuntimeError as exc:
        fail(str(exc))

    failed_addresses = snapshot.get("failed_addresses", {})
    if isinstance(failed_addresses, dict) and failed_addresses and not args.allow_failed_snapshot:
        fail(
            "Snapshot contains failed_addresses. Re-snapshot, or pass --allow-failed-snapshot "
            "for an intentional partial run."
        )

    if args.exclude_addresses_file:
        try:
            exclude_set = load_exclude_set(args.exclude_addresses_file)
        except RuntimeError as exc:
            fail(str(exc))
    else:
        exclude_set = set()

    try:
        run_config_fingerprint, exclude_path_resolved, exclude_file_sha256 = compute_run_config_fingerprint(
            snapshot_path,
            args.exclude_addresses_file,
            args.min_balance_wei,
            args.max_balance_wei,
        )
    except RuntimeError as exc:
        fail(str(exc))

    snapshot_fp = snapshot_fingerprint(snapshot)
    snapshot_sha = file_sha256(snapshot_path)

    target_rpc = args.target_rpc or os.getenv("TARGET_RPC_URL")
    if not target_rpc:
        fail("TARGET_RPC_URL env var is not set and --target-rpc not provided")

    chain_id_env = os.getenv("CHAIN_ID")
    if not chain_id_env:
        fail("CHAIN_ID env var is not set")
    try:
        expected_chain_id = parse_int(chain_id_env, "CHAIN_ID")
    except RuntimeError as exc:
        fail(str(exc))

    admin_private_key = os.getenv("ADMIN_PRIVATE_KEY")
    if not admin_private_key:
        fail("ADMIN_PRIVATE_KEY env var is not set")

    source_rpc = args.source_rpc or os.getenv("SOURCE_RPC_URL")

    print("=== Snapshot checks ===")
    print(f"snapshot_file: {snapshot_path.resolve()}")
    print(f"snapshot_sha256: {snapshot_sha}")
    print(f"snapshot_fingerprint: {snapshot_fp}")
    print(f"snapshot_balances_count: {len(snapshot['balances'])}")
    print(f"snapshot_failed_addresses: {len(failed_addresses) if isinstance(failed_addresses, dict) else 0}")
    print()

    print("=== Run config checks ===")
    print(f"min_balance_wei: {args.min_balance_wei}")
    print(f"max_balance_wei: {args.max_balance_wei}")
    print(f"exclude_addresses_count: {len(exclude_set)}")
    print(f"exclude_addresses_file: {exclude_path_resolved}")
    print(f"exclude_addresses_file_sha256: {exclude_file_sha256}")
    print(f"run_config_fingerprint: {run_config_fingerprint}")
    print()

    print("=== Target RPC checks ===")
    try:
        modules = rpc_call(target_rpc, "rpc_modules", [], args.timeout_seconds)
        chain_id_hex = rpc_call(target_rpc, "eth_chainId", [], args.timeout_seconds)
        block_number_hex = rpc_call(target_rpc, "eth_blockNumber", [], args.timeout_seconds)
        _ = rpc_call(
            target_rpc,
            "eth_getBalance",
            ["0x0000000000000000000000000000000000000000", args.probe_block_tag],
            args.timeout_seconds,
        )
        _ = rpc_call(
            target_rpc,
            "eth_getCode",
            ["0x0000000000000000000000000000000000000000", args.probe_block_tag],
            args.timeout_seconds,
        )
    except RuntimeError as exc:
        fail(f"Target RPC check failed: {exc}")

    if not isinstance(modules, dict):
        fail("Target rpc_modules returned non-object response")

    missing_target_modules = [name for name in ("eth", "rpc") if name not in modules]
    if missing_target_modules:
        fail(f"Target RPC missing required modules: {', '.join(missing_target_modules)}")

    try:
        network_chain_id = int(chain_id_hex, 16)
    except ValueError as exc:
        fail(f"Target eth_chainId returned non-hex value: {chain_id_hex} ({exc})")

    if network_chain_id != expected_chain_id:
        fail(f"CHAIN_ID mismatch: env={expected_chain_id}, target_rpc={network_chain_id}")

    print(f"target_rpc: {target_rpc}")
    print(f"target_chain_id: {network_chain_id}")
    print(f"target_head_block: {int(block_number_hex, 16)}")
    print("target_method_probes: rpc_modules, eth_chainId, eth_blockNumber, eth_getBalance, eth_getCode")
    print()

    print("=== Source RPC checks ===")
    if not source_rpc:
        warn("SOURCE_RPC_URL not set and --source-rpc not provided; skipping source RPC probes")
        strict_available = False
    else:
        try:
            source_modules = rpc_call(source_rpc, "rpc_modules", [], args.timeout_seconds)
            source_block_hex = rpc_call(source_rpc, "eth_blockNumber", [], args.timeout_seconds)
            _ = rpc_call(
                source_rpc,
                "reth_getBalanceChangesInBlock",
                [args.probe_block_tag],
                args.timeout_seconds,
            )
            debug_result = rpc_call(
                source_rpc,
                "debug_accountRange",
                [
                    args.probe_block_tag,
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                    1,
                    True,
                    True,
                    False,
                ],
                args.timeout_seconds,
            )
        except RuntimeError as exc:
            fail(f"Source RPC check failed: {exc}")

        if not isinstance(source_modules, dict):
            fail("Source rpc_modules returned non-object response")

        missing_source_modules = [name for name in ("eth", "debug", "reth", "rpc") if name not in source_modules]
        if missing_source_modules:
            fail(f"Source RPC missing required modules: {', '.join(missing_source_modules)}")

        strict_available = debug_result is not None
        print(f"source_rpc: {source_rpc}")
        print(f"source_head_block: {int(source_block_hex, 16)}")
        print("source_method_probes: rpc_modules, eth_blockNumber, reth_getBalanceChangesInBlock, debug_accountRange")
        if strict_available:
            print("strict_discovery_capability: available (debug_accountRange returned page data)")
        else:
            warn("debug_accountRange returned null page data on source RPC")
            print("strict_discovery_capability: unavailable (debug_accountRange returned null)")
        print()

    if args.require_strict_capability and not strict_available:
        fail("--require-strict-capability set, but strict discovery capability is unavailable")

    print("=== State consistency checks ===")
    try:
        state = load_state(state_file)
    except RuntimeError as exc:
        fail(str(exc))

    if not state:
        print(f"state_file: {state_file} (not found or empty -> fresh run)")
        print("state_consistency: OK (fresh run)")
    else:
        state_snapshot_fingerprint = state.get("snapshot_fingerprint")
        if state_snapshot_fingerprint != snapshot_fp:
            fail(
                "State snapshot_fingerprint does not match provided snapshot. "
                "Use --reset-state with migrate_balances.py if this is intentional."
            )

        state_run_config_fingerprint = state.get("run_config_fingerprint")
        if state_run_config_fingerprint != run_config_fingerprint:
            fail(
                "State run_config_fingerprint does not match current config "
                "(snapshot path/exclude/min/max). Use --reset-state if intentional."
            )

        print(f"state_file: {state_file.resolve()}")
        print(f"state_next_index: {state.get('next_index')}")
        print(f"state_next_nonce: {state.get('next_nonce')}")
        print("state_consistency: OK")

    print()
    print("Preflight checks passed.")


if __name__ == "__main__":
    main()
