#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from web3 import Web3
from web3.exceptions import TimeExhausted, TransactionNotFound
from web3.middleware import geth_poa_middleware


def load_snapshot(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError(f"Could not read snapshot file {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Snapshot file is not valid JSON: {exc}") from exc

    balances = data.get("balances")
    if not isinstance(balances, dict):
        raise RuntimeError("Snapshot JSON missing 'balances' object.")

    return data


def snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = {
        "source_rpc": snapshot.get("source_rpc"),
        "chain_id": snapshot.get("chain_id"),
        "block": snapshot.get("block"),
        "balances": snapshot["balances"],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Could not load state file {path}: {exc}") from exc


def save_state(path: Path, state: dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def parse_int(value: str, name: str) -> int:
    try:
        out = int(value, 0)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer. Got: {value}") from exc
    if out < 0:
        raise RuntimeError(f"{name} must be non-negative. Got: {value}")
    return out


def validate_and_get_env() -> tuple[str, str, int]:
    target_rpc = os.getenv("TARGET_RPC_URL")
    if not target_rpc:
        raise RuntimeError("TARGET_RPC_URL env var is not set.")

    admin_priv = os.getenv("ADMIN_PRIVATE_KEY")
    if not admin_priv:
        raise RuntimeError("ADMIN_PRIVATE_KEY env var is not set.")

    chain_id_env = os.getenv("CHAIN_ID")
    if not chain_id_env:
        raise RuntimeError("CHAIN_ID env var is not set.")
    chain_id = parse_int(chain_id_env, "CHAIN_ID")

    return target_rpc, admin_priv, chain_id


def reconcile_in_flight_tx(
    w3: Web3,
    state_file: Path,
    state: dict[str, Any],
    receipt_timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    in_flight = state.get("in_flight")
    if not isinstance(in_flight, dict):
        return state

    tx_hash_hex = in_flight.get("tx_hash")
    if not isinstance(tx_hash_hex, str):
        raise RuntimeError(
            "State contains in_flight tx without tx_hash. "
            "Inspect state file and resolve manually."
        )

    print(f"Reconciling in-flight tx: {tx_hash_hex}")
    tx_hash = Web3.to_bytes(hexstr=tx_hash_hex)

    receipt = None
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
    except TransactionNotFound:
        pass

    if receipt is None:
        try:
            receipt = w3.eth.wait_for_transaction_receipt(
                tx_hash,
                timeout=receipt_timeout_seconds,
                poll_latency=poll_interval_seconds,
            )
        except TimeExhausted as exc:
            raise RuntimeError(
                "In-flight tx still pending or missing. Re-run later, or reset state only after "
                "verifying tx status manually."
            ) from exc

    if receipt.status != 1:
        raise RuntimeError(
            f"In-flight tx failed on-chain: {tx_hash_hex}. "
            "Fix root cause before retrying migration."
        )

    inflight_index = in_flight.get("index")
    inflight_nonce = in_flight.get("nonce")
    if not isinstance(inflight_index, int) or not isinstance(inflight_nonce, int):
        raise RuntimeError(
            "State in_flight tx is missing index/nonce metadata. "
            "Inspect state file and resolve manually."
        )

    next_nonce = inflight_nonce + 1
    next_index = inflight_index + 1
    state["next_index"] = next_index
    state["next_nonce"] = next_nonce
    state["last_tx_hash"] = tx_hash_hex
    state["last_updated"] = int(time.time())
    state.pop("in_flight", None)
    save_state(state_file, state)
    print(
        "In-flight tx confirmed. "
        f"Resuming from index={next_index}, nonce={next_nonce}."
    )
    return state


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Replay balances from snapshot onto target chain using admin wallet. "
            "This tool is idempotent by sending only missing balance delta."
        )
    )
    parser.add_argument("--snapshot", required=True, help="Path to balances_snapshot.json")
    parser.add_argument(
        "--state-file",
        default="migration_state.json",
        help="Path to migration state file. Default: migration_state.json",
    )
    parser.add_argument(
        "--min-balance-wei",
        type=int,
        default=0,
        help="Skip addresses whose expected snapshot balance is below this value. Default: 0",
    )
    parser.add_argument(
        "--receipt-timeout-seconds",
        type=int,
        default=180,
        help="Timeout for waiting transaction receipt. Default: 180",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval while waiting for receipt. Default: 2.0",
    )
    parser.add_argument(
        "--gas-limit",
        type=int,
        default=21000,
        help="Gas limit for transfer txs. Default: 21000",
    )
    parser.add_argument(
        "--gas-price-wei",
        type=int,
        help="Override gas price in wei. If omitted, uses node gas price.",
    )
    parser.add_argument(
        "--max-gas-price-wei",
        type=int,
        help="Abort if effective gas price exceeds this value.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Print progress every N addresses. Set 0 to disable. Default: 50",
    )
    parser.add_argument(
        "--allow-failed-snapshot",
        action="store_true",
        help="Allow migration from snapshot that contains failed_addresses.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignore existing state file and start from index 0.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not send txs; print planned operations only.",
    )
    args = parser.parse_args()

    if args.min_balance_wei < 0:
        print("ERROR: --min-balance-wei must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.receipt_timeout_seconds <= 0:
        print("ERROR: --receipt-timeout-seconds must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.poll_interval_seconds <= 0:
        print("ERROR: --poll-interval-seconds must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.gas_limit <= 0:
        print("ERROR: --gas-limit must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.progress_every < 0:
        print("ERROR: --progress-every must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.max_gas_price_wei is not None and args.max_gas_price_wei < 0:
        print("ERROR: --max-gas-price-wei must be >= 0.", file=sys.stderr)
        sys.exit(1)

    try:
        target_rpc, admin_priv, expected_chain_id = validate_and_get_env()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    snapshot_path = Path(args.snapshot)
    state_file = Path(args.state_file)

    try:
        snapshot = load_snapshot(snapshot_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    failed_addresses = snapshot.get("failed_addresses", {})
    if (
        isinstance(failed_addresses, dict)
        and failed_addresses
        and not args.allow_failed_snapshot
    ):
        print(
            "ERROR: snapshot contains failed_addresses. "
            "Refusing to migrate; re-snapshot or use --allow-failed-snapshot.",
            file=sys.stderr,
        )
        sys.exit(2)

    balances: dict[str, Any] = snapshot["balances"]
    addresses = list(balances.keys())
    total = len(addresses)
    if total == 0:
        print("ERROR: snapshot contains no balances.", file=sys.stderr)
        sys.exit(1)

    snap_fingerprint = snapshot_fingerprint(snapshot)

    w3 = Web3(Web3.HTTPProvider(target_rpc))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    if not w3.is_connected():
        print(f"ERROR: failed to connect to target RPC: {target_rpc}", file=sys.stderr)
        sys.exit(1)

    admin_account = w3.eth.account.from_key(admin_priv)
    admin_address = admin_account.address

    network_chain_id = int(w3.eth.chain_id)
    if network_chain_id != expected_chain_id:
        print(
            "ERROR: CHAIN_ID mismatch. "
            f"Env={expected_chain_id}, network={network_chain_id}",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.reset_state:
        state: dict[str, Any] = {}
    else:
        try:
            state = load_state(state_file)
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    if state:
        state_fingerprint = state.get("snapshot_fingerprint")
        if state_fingerprint and state_fingerprint != snap_fingerprint:
            print(
                "ERROR: Existing state file belongs to a different snapshot. "
                "Use --reset-state to start over.",
                file=sys.stderr,
            )
            sys.exit(1)

    next_index = int(state.get("next_index", 0)) if state else 0
    if next_index < 0 or next_index > total:
        print(
            f"ERROR: Invalid next_index in state ({next_index}) for total {total}.",
            file=sys.stderr,
        )
        sys.exit(1)

    if state and not args.dry_run:
        try:
            state = reconcile_in_flight_tx(
                w3,
                state_file,
                state,
                args.receipt_timeout_seconds,
                args.poll_interval_seconds,
            )
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)
        next_index = int(state.get("next_index", next_index))

    nonce = state.get("next_nonce") if state else None
    if nonce is None:
        nonce = int(w3.eth.get_transaction_count(admin_address, "pending"))
    else:
        nonce = int(nonce)

    print(f"Admin address: {admin_address}")
    print(f"Target RPC: {target_rpc}")
    print(f"Network chain ID: {network_chain_id}")
    print(f"Snapshot file: {snapshot_path}")
    print(f"Snapshot block metadata: {snapshot.get('block')}")
    print(f"Total addresses in snapshot: {total}")
    print(f"Resuming at index: {next_index}, nonce: {nonce}")
    if args.dry_run:
        print("Dry-run mode enabled: no transactions will be sent and state will not be updated.")

    sent_count = 0
    skipped_threshold = 0
    skipped_already_funded = 0

    for idx in range(next_index, total):
        addr = addresses[idx]
        try:
            expected = int(balances[addr])
        except (TypeError, ValueError):
            print(
                f"ERROR: Invalid expected balance for {addr}: {balances[addr]}",
                file=sys.stderr,
            )
            sys.exit(1)
        if expected < 0:
            print(
                f"ERROR: Invalid negative expected balance for {addr}: {expected}",
                file=sys.stderr,
            )
            sys.exit(1)

        if expected < args.min_balance_wei:
            skipped_threshold += 1
            if args.progress_every and (idx + 1) % args.progress_every == 0:
                print(f"  processed {idx + 1}/{total} addresses...")
            if not args.dry_run:
                state = {
                    "snapshot_fingerprint": snap_fingerprint,
                    "snapshot_path": str(snapshot_path.resolve()),
                    "next_index": idx + 1,
                    "next_nonce": nonce,
                    "last_processed_address": addr,
                    "last_updated": int(time.time()),
                }
                save_state(state_file, state)
            continue

        current_balance = int(w3.eth.get_balance(addr))
        delta = expected - current_balance
        if delta <= 0:
            skipped_already_funded += 1
            if args.progress_every and (idx + 1) % args.progress_every == 0:
                print(f"  processed {idx + 1}/{total} addresses...")
            if not args.dry_run:
                state = {
                    "snapshot_fingerprint": snap_fingerprint,
                    "snapshot_path": str(snapshot_path.resolve()),
                    "next_index": idx + 1,
                    "next_nonce": nonce,
                    "last_processed_address": addr,
                    "last_updated": int(time.time()),
                }
                save_state(state_file, state)
            continue

        gas_price = int(args.gas_price_wei) if args.gas_price_wei is not None else int(w3.eth.gas_price)
        if args.max_gas_price_wei is not None and gas_price > args.max_gas_price_wei:
            print(
                f"ERROR: Gas price {gas_price} exceeds max {args.max_gas_price_wei}.",
                file=sys.stderr,
            )
            sys.exit(1)

        admin_balance = int(w3.eth.get_balance(admin_address))
        tx_fee_cost = args.gas_limit * gas_price
        if admin_balance < (delta + tx_fee_cost):
            print(
                "ERROR: Admin balance insufficient for next tx. "
                f"required={delta + tx_fee_cost}, current={admin_balance}",
                file=sys.stderr,
            )
            sys.exit(1)

        tx = {
            "chainId": expected_chain_id,
            "from": admin_address,
            "to": addr,
            "value": delta,
            "nonce": nonce,
            "gas": args.gas_limit,
            "gasPrice": gas_price,
        }

        if args.dry_run:
            print(
                f"[DRY RUN] idx={idx} addr={addr} expected={expected} "
                f"current={current_balance} delta={delta} nonce={nonce} gasPrice={gas_price}"
            )
            nonce += 1
            sent_count += 1
            if args.progress_every and (idx + 1) % args.progress_every == 0:
                print(f"  processed {idx + 1}/{total} addresses...")
            continue

        state = {
            "snapshot_fingerprint": snap_fingerprint,
            "snapshot_path": str(snapshot_path.resolve()),
            "next_index": idx,
            "next_nonce": nonce,
            "last_processed_address": addr,
            "in_flight": {
                "index": idx,
                "address": addr,
                "nonce": nonce,
                "value": delta,
                "gas_price": gas_price,
                "started_at": int(time.time()),
            },
            "last_updated": int(time.time()),
        }
        save_state(state_file, state)

        signed = admin_account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        print(
            f"Sent delta={delta} wei to {addr} (expected={expected}, current={current_balance}), "
            f"nonce={nonce}, tx={tx_hash_hex}"
        )

        state["in_flight"]["tx_hash"] = tx_hash_hex
        save_state(state_file, state)

        try:
            receipt = w3.eth.wait_for_transaction_receipt(
                tx_hash,
                timeout=args.receipt_timeout_seconds,
                poll_latency=args.poll_interval_seconds,
            )
        except TimeExhausted:
            print(
                f"ERROR: Timed out waiting for receipt for tx {tx_hash_hex}. "
                "State preserved with in_flight tx; rerun to reconcile.",
                file=sys.stderr,
            )
            sys.exit(1)

        if receipt.status != 1:
            print(
                f"ERROR: Tx failed on-chain. tx={tx_hash_hex}, status={receipt.status}",
                file=sys.stderr,
            )
            sys.exit(1)

        nonce += 1
        sent_count += 1
        state = {
            "snapshot_fingerprint": snap_fingerprint,
            "snapshot_path": str(snapshot_path.resolve()),
            "next_index": idx + 1,
            "next_nonce": nonce,
            "last_processed_address": addr,
            "last_tx_hash": tx_hash_hex,
            "last_tx_block": int(receipt.blockNumber),
            "last_updated": int(time.time()),
        }
        save_state(state_file, state)

        if args.progress_every and (idx + 1) % args.progress_every == 0:
            print(f"  processed {idx + 1}/{total} addresses...")

    print("Migration run finished.")
    print(f"Addresses total: {total}")
    print(f"Sent tx count: {sent_count}")
    print(f"Skipped (below threshold): {skipped_threshold}")
    print(f"Skipped (already funded): {skipped_already_funded}")
    if args.dry_run:
        print("Dry-run complete.")
    else:
        print(f"State file updated: {state_file}")


if __name__ == "__main__":
    main()
