#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any


def parse_block(value: str) -> int:
    try:
        block = int(value, 0)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid block value: {value}") from exc
    if block < 0:
        raise argparse.ArgumentTypeError("Block number must be non-negative.")
    return block


def normalize_address(addr: str) -> str:
    if not (addr.startswith("0x") or addr.startswith("0X")) or len(addr) != 42:
        raise ValueError("must be a 20-byte hex address with 0x prefix")
    try:
        int(addr[2:], 16)
    except ValueError as exc:
        raise ValueError("contains non-hex characters") from exc
    return "0x" + addr[2:].lower()


def rpc_call(
    url: str,
    method: str,
    params: list[Any],
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        payload = {
            "jsonrpc": "2.0",
            "id": time.time_ns(),
            "method": method,
            "params": params,
        }
        try:
            encoded = json.dumps(payload).encode("utf-8")
            request = urllib.request.Request(
                url,
                data=encoded,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
            data = json.loads(raw_body)
            if "error" in data:
                err = data["error"]
                raise RuntimeError(f"RPC error code={err.get('code')} message={err.get('message')}")
            if "result" not in data:
                raise RuntimeError("RPC response missing 'result' field")
            return data["result"]
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt == retries:
                break
            sleep_for = backoff_seconds * (2 ** (attempt - 1))
            print(
                f"[warn] {method} attempt {attempt}/{retries} failed: {exc}. "
                f"Retrying in {sleep_for:.2f}s...",
                file=sys.stderr,
            )
            time.sleep(sleep_for)

    raise RuntimeError(f"{method} failed after {retries} attempts: {last_error}")


def get_block(
    rpc_url: str,
    block_ref: str,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
) -> dict[str, Any]:
    block = rpc_call(
        rpc_url,
        "eth_getBlockByNumber",
        [block_ref, False],
        timeout_seconds,
        retries,
        backoff_seconds,
    )
    if not block:
        raise RuntimeError(f"eth_getBlockByNumber returned null for block_ref={block_ref}")
    number_hex = block.get("number")
    block_hash = block.get("hash")
    if not number_hex or not block_hash:
        raise RuntimeError(f"Block response missing number/hash for block_ref={block_ref}")
    return block


def resolve_snapshot_block(
    rpc_url: str,
    explicit_block: int | None,
    fallback_tags: list[str],
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
) -> tuple[int, str, str, str]:
    if explicit_block is not None:
        explicit_ref = hex(explicit_block)
        block = get_block(
            rpc_url,
            explicit_ref,
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        block_number = int(block["number"], 16)
        if block_number != explicit_block:
            raise RuntimeError(
                f"Node returned block {block_number} for requested block {explicit_block}"
            )
        return block_number, explicit_ref, block["hash"], "explicit"

    for tag in fallback_tags:
        try:
            block = get_block(
                rpc_url,
                tag,
                timeout_seconds,
                retries,
                backoff_seconds,
            )
            block_number = int(block["number"], 16)
            return block_number, hex(block_number), block["hash"], tag
        except Exception as exc:
            print(f"[warn] could not resolve '{tag}' block tag: {exc}", file=sys.stderr)

    raise RuntimeError(
        f"Unable to resolve snapshot block from fallback tags: {', '.join(fallback_tags)}"
    )


def load_addresses(addresses_file: str) -> tuple[list[str], list[str], int, int]:
    addresses: list[str] = []
    invalid_lines: list[str] = []
    duplicates_skipped = 0
    seen: set[str] = set()
    input_total = 0

    with open(addresses_file, "r", encoding="utf-8") as file_handle:
        for line_number, raw_line in enumerate(file_handle, start=1):
            raw = raw_line.strip()
            if not raw or raw.startswith("#"):
                continue
            input_total += 1
            try:
                normalized = normalize_address(raw)
            except ValueError as exc:
                invalid_lines.append(f"line {line_number}: {raw} ({exc})")
                continue
            if normalized in seen:
                duplicates_skipped += 1
                continue
            seen.add(normalized)
            addresses.append(normalized)

    return addresses, invalid_lines, duplicates_skipped, input_total


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Snapshot balances from a source chain at a single fixed block. "
            "Intended for migration-grade snapshots."
        )
    )
    parser.add_argument(
        "--addresses-file",
        required=True,
        help="Path to a text file with one address per line.",
    )
    parser.add_argument(
        "--out",
        default="balances_snapshot.json",
        help="Output JSON file. Default: balances_snapshot.json",
    )
    parser.add_argument(
        "--block",
        type=parse_block,
        help="Block number to snapshot at (decimal or 0x-prefixed hex).",
    )
    parser.add_argument(
        "--fallback-tags",
        default="finalized,safe,latest",
        help=(
            "Comma-separated tags to try when --block is omitted. "
            "Default: finalized,safe,latest"
        ),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="RPC timeout in seconds. Default: 30",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of RPC retries per call. Default: 3",
    )
    parser.add_argument(
        "--backoff-seconds",
        type=float,
        default=0.5,
        help="Initial retry backoff in seconds (exponential). Default: 0.5",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Print progress every N addresses. Set 0 to disable. Default: 100",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow writing output and exiting 0 even if some addresses fail.",
    )
    args = parser.parse_args()

    if args.retries < 1:
        print("ERROR: --retries must be >= 1.", file=sys.stderr)
        sys.exit(1)
    if args.timeout_seconds <= 0:
        print("ERROR: --timeout-seconds must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.backoff_seconds < 0:
        print("ERROR: --backoff-seconds must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.progress_every < 0:
        print("ERROR: --progress-every must be >= 0.", file=sys.stderr)
        sys.exit(1)

    source_rpc = os.getenv("SOURCE_RPC_URL")
    if not source_rpc:
        print("ERROR: SOURCE_RPC_URL env var is not set.", file=sys.stderr)
        sys.exit(1)

    addresses, invalid_lines, duplicates_skipped, input_total = load_addresses(
        args.addresses_file
    )
    if invalid_lines:
        print("ERROR: invalid addresses found in --addresses-file:", file=sys.stderr)
        for line in invalid_lines[:20]:
            print(f"  {line}", file=sys.stderr)
        if len(invalid_lines) > 20:
            print(f"  ... and {len(invalid_lines) - 20} more invalid lines", file=sys.stderr)
        sys.exit(1)
    if not addresses:
        print("ERROR: no valid addresses found in --addresses-file.", file=sys.stderr)
        sys.exit(1)

    fallback_tags = [tag.strip() for tag in args.fallback_tags.split(",") if tag.strip()]
    if args.block is None and not fallback_tags:
        print("ERROR: no fallback tags provided and --block not set.", file=sys.stderr)
        sys.exit(1)

    chain_id_hex = rpc_call(
        source_rpc,
        "eth_chainId",
        [],
        args.timeout_seconds,
        args.retries,
        args.backoff_seconds,
    )
    chain_id = int(chain_id_hex, 16)

    block_number, block_tag, block_hash, block_resolved_from = resolve_snapshot_block(
        source_rpc,
        args.block,
        fallback_tags,
        args.timeout_seconds,
        args.retries,
        args.backoff_seconds,
    )

    print(f"Source RPC: {source_rpc}")
    print(f"Chain ID: {chain_id}")
    print(
        "Snapshot block:"
        f" number={block_number} tag={block_tag} hash={block_hash}"
        f" resolved_from={block_resolved_from}"
    )
    print(
        "Addresses:"
        f" input_total={input_total} unique_valid={len(addresses)}"
        f" duplicates_skipped={duplicates_skipped}"
    )

    balances: dict[str, str] = {}
    failures: dict[str, str] = {}

    for index, address in enumerate(addresses, start=1):
        try:
            result = rpc_call(
                source_rpc,
                "eth_getBalance",
                [address, block_tag],
                args.timeout_seconds,
                args.retries,
                args.backoff_seconds,
            )
            balance_wei = int(result, 16)
            balances[address] = str(balance_wei)
        except Exception as exc:
            failures[address] = str(exc)

        if args.progress_every and index % args.progress_every == 0:
            print(f"  processed {index}/{len(addresses)} addresses...")

    snapshot = {
        "source_rpc": source_rpc,
        "chain_id": chain_id,
        "block": {
            "number": block_number,
            "tag": block_tag,
            "hash": block_hash,
            "resolved_from": block_resolved_from,
        },
        "address_counts": {
            "input_total": input_total,
            "unique_valid": len(addresses),
            "duplicates_skipped": duplicates_skipped,
            "succeeded": len(balances),
            "failed": len(failures),
        },
        "balances": balances,
        "failed_addresses": failures,
        "timestamp_unix": int(time.time()),
    }

    with open(args.out, "w", encoding="utf-8") as out_file:
        json.dump(snapshot, out_file, indent=2, sort_keys=True)

    print(f"Snapshot written to {args.out}")
    if failures:
        print(
            f"ERROR: failed to fetch {len(failures)} addresses; "
            "see failed_addresses in output JSON.",
            file=sys.stderr,
        )
        if not args.allow_partial:
            sys.exit(2)
    print("Done.")


if __name__ == "__main__":
    main()
