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


def resolve_target_block(
    rpc_url: str,
    explicit_block: int | None,
    fallback_tags: list[str],
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
) -> tuple[int, str, str, str, str]:
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
        state_root = block.get("stateRoot")
        if not isinstance(state_root, str) or not state_root:
            raise RuntimeError(f"Block response missing stateRoot for block_ref={explicit_ref}")
        return block_number, explicit_ref, block["hash"], "explicit", state_root

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
            state_root = block.get("stateRoot")
            if not isinstance(state_root, str) or not state_root:
                raise RuntimeError(f"Block response missing stateRoot for block_ref={tag}")
            return block_number, hex(block_number), block["hash"], tag, state_root
        except Exception as exc:
            print(f"[warn] could not resolve '{tag}' block tag: {exc}", file=sys.stderr)

    raise RuntimeError(
        f"Unable to resolve target block from fallback tags: {', '.join(fallback_tags)}"
    )


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


def _try_coerce_address(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    if value.startswith("0x") or value.startswith("0X"):
        candidate = value
    else:
        candidate = "0x" + value
    try:
        return normalize_address(candidate)
    except ValueError:
        return None


def _extract_address_from_entry(map_key: Any, map_value: Any) -> str | None:
    from_value = _try_coerce_address(map_value)
    if from_value is not None:
        return from_value

    if isinstance(map_value, dict):
        for field in (
            "address",
            "preimage",
            "addr",
            "account",
            "accountAddress",
            "keyPreimage",
        ):
            candidate = _try_coerce_address(map_value.get(field))
            if candidate is not None:
                return candidate

    from_key = _try_coerce_address(map_key)
    if from_key is not None:
        return from_key

    return None


def _is_zero_hex_key(value: str | None) -> bool:
    if value is None:
        return True
    text = value.strip().lower()
    if text == "":
        return True
    if text.startswith("0x"):
        text = text[2:]
    if text == "":
        return True
    try:
        return int(text, 16) == 0
    except ValueError:
        return False


def _normalize_page_start_key(value: str) -> str:
    text = value.strip()
    if text.startswith("0x") or text.startswith("0X"):
        return "0x" + text[2:]
    return "0x" + text


def _parse_account_range_page(result: Any) -> tuple[int, set[str], int, str | None]:
    if not isinstance(result, dict):
        raise RuntimeError(
            "debug_accountRange returned non-object response: "
            f"{type(result).__name__}"
        )

    next_key: str | None = None
    for key in ("nextKey", "next", "next_key"):
        candidate = result.get(key)
        if isinstance(candidate, str):
            next_key = candidate
            break

    entries: list[tuple[Any, Any]] = []
    address_map = result.get("addressMap")
    if isinstance(address_map, dict):
        entries.extend(address_map.items())

    accounts = result.get("accounts")
    if isinstance(accounts, dict):
        entries.extend(accounts.items())

    if not entries:
        for key, value in result.items():
            if key in {"nextKey", "next", "next_key"}:
                continue
            entries.append((key, value))

    discovered: set[str] = set()
    missing_preimage_entries = 0
    for map_key, map_value in entries:
        address = _extract_address_from_entry(map_key, map_value)
        if address is None:
            missing_preimage_entries += 1
            continue
        discovered.add(address)

    return len(entries), discovered, missing_preimage_entries, next_key


def enumerate_debug_account_range(
    rpc_url: str,
    block_tag: str,
    page_size: int,
    incompletes: bool,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> dict[str, Any]:
    start_key = "0x"
    pages = 0
    total_entries = 0
    missing_preimage_entries = 0
    addresses: set[str] = set()
    seen_starts: set[str] = set()

    while True:
        if start_key in seen_starts:
            raise RuntimeError(
                "debug_accountRange pagination loop detected "
                f"at start_key={start_key}."
            )
        seen_starts.add(start_key)

        page_result = rpc_call(
            rpc_url,
            "debug_accountRange",
            [block_tag, start_key, page_size, True, True, incompletes],
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        page_entries, page_addresses, page_missing, next_key = _parse_account_range_page(page_result)

        pages += 1
        total_entries += page_entries
        missing_preimage_entries += page_missing
        addresses.update(page_addresses)

        if progress_every and pages % progress_every == 0:
            print(
                "  accountRange pages"
                f" {pages} (entries={total_entries}, addresses={len(addresses)})"
            )

        if _is_zero_hex_key(next_key):
            break
        if next_key is None:
            break

        start_key = _normalize_page_start_key(next_key)

    return {
        "pages": pages,
        "total_entries": total_entries,
        "missing_preimage_entries": missing_preimage_entries,
        "addresses": addresses,
    }


def run_preimage_audit(
    rpc_url: str,
    block_tag: str,
    page_size: int,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> dict[str, Any]:
    print("Running preimage completeness audit with debug_accountRange...")

    with_incompletes = enumerate_debug_account_range(
        rpc_url,
        block_tag,
        page_size,
        True,
        timeout_seconds,
        retries,
        backoff_seconds,
        progress_every,
    )
    without_incompletes = enumerate_debug_account_range(
        rpc_url,
        block_tag,
        page_size,
        False,
        timeout_seconds,
        retries,
        backoff_seconds,
        progress_every,
    )

    missing_by_count = with_incompletes["total_entries"] - without_incompletes["total_entries"]
    missing_by_parse = with_incompletes["missing_preimage_entries"]

    return {
        "with_incompletes": {
            "pages": with_incompletes["pages"],
            "total_entries": with_incompletes["total_entries"],
            "missing_preimage_entries": with_incompletes["missing_preimage_entries"],
            "addresses_count": len(with_incompletes["addresses"]),
        },
        "without_incompletes": {
            "pages": without_incompletes["pages"],
            "total_entries": without_incompletes["total_entries"],
            "missing_preimage_entries": without_incompletes["missing_preimage_entries"],
            "addresses_count": len(without_incompletes["addresses"]),
        },
        "missing_preimages_by_count": missing_by_count,
        "missing_preimages_by_parse": missing_by_parse,
        "passed": missing_by_count == 0 and missing_by_parse == 0,
        "strict_discovered_addresses": without_incompletes["addresses"],
    }


def discover_with_reth_balance_changes(
    rpc_url: str,
    from_block: int,
    to_block: int,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> set[str]:
    discovered: set[str] = set()

    for number in range(from_block, to_block + 1):
        changes = rpc_call(
            rpc_url,
            "reth_getBalanceChangesInBlock",
            [hex(number)],
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        if not isinstance(changes, dict):
            raise RuntimeError(
                "reth_getBalanceChangesInBlock returned non-object response "
                f"for block {number}: {type(changes).__name__}"
            )
        for raw_addr in changes.keys():
            if not isinstance(raw_addr, str):
                continue
            try:
                discovered.add(normalize_address(raw_addr))
            except ValueError:
                continue

        if progress_every and (number - from_block + 1) % progress_every == 0:
            print(
                "  scanned blocks"
                f" {number - from_block + 1}/{to_block - from_block + 1}"
                f" (addresses={len(discovered)})"
            )

    return discovered


def discover_with_tx_scan(
    rpc_url: str,
    from_block: int,
    to_block: int,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> set[str]:
    discovered: set[str] = set()

    for number in range(from_block, to_block + 1):
        block = rpc_call(
            rpc_url,
            "eth_getBlockByNumber",
            [hex(number), True],
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        if not isinstance(block, dict):
            raise RuntimeError(
                f"eth_getBlockByNumber returned non-object for block {number}: {type(block).__name__}"
            )

        beneficiary = block.get("miner")
        if isinstance(beneficiary, str):
            try:
                discovered.add(normalize_address(beneficiary))
            except ValueError:
                pass

        txs = block.get("transactions")
        if isinstance(txs, list):
            for tx in txs:
                if not isinstance(tx, dict):
                    continue
                for key in ("from", "to"):
                    raw_addr = tx.get(key)
                    if isinstance(raw_addr, str):
                        try:
                            discovered.add(normalize_address(raw_addr))
                        except ValueError:
                            continue

        if progress_every and (number - from_block + 1) % progress_every == 0:
            print(
                "  scanned blocks"
                f" {number - from_block + 1}/{to_block - from_block + 1}"
                f" (addresses={len(discovered)})"
            )

    return discovered


def filter_nonzero_at_target(
    rpc_url: str,
    addresses: set[str],
    block_tag: str,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> tuple[set[str], int]:
    kept: set[str] = set()
    zero_count = 0

    ordered = sorted(addresses)
    for index, address in enumerate(ordered, start=1):
        result = rpc_call(
            rpc_url,
            "eth_getBalance",
            [address, block_tag],
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        balance = int(result, 16)
        if balance > 0:
            kept.add(address)
        else:
            zero_count += 1

        if progress_every and index % progress_every == 0:
            print(f"  checked balances {index}/{len(ordered)} (kept={len(kept)})")

    return kept, zero_count


def _is_empty_code(code_result: Any) -> bool:
    if not isinstance(code_result, str):
        return False
    text = code_result.strip().lower()
    if not text.startswith("0x"):
        return False
    payload = text[2:]
    if payload == "":
        return True
    try:
        return int(payload, 16) == 0
    except ValueError:
        return False


def filter_eoa_at_target(
    rpc_url: str,
    addresses: set[str],
    block_tag: str,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> tuple[set[str], int]:
    kept: set[str] = set()
    non_eoa_count = 0

    ordered = sorted(addresses)
    for index, address in enumerate(ordered, start=1):
        code_result = rpc_call(
            rpc_url,
            "eth_getCode",
            [address, block_tag],
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        if _is_empty_code(code_result):
            kept.add(address)
        else:
            non_eoa_count += 1

        if progress_every and index % progress_every == 0:
            print(f"  checked code {index}/{len(ordered)} (eoas={len(kept)})")

    return kept, non_eoa_count


def fetch_account_proofs(
    rpc_url: str,
    addresses: list[str],
    block_tag: str,
    timeout_seconds: float,
    retries: int,
    backoff_seconds: float,
    progress_every: int,
) -> list[dict[str, Any]]:
    proofs: list[dict[str, Any]] = []

    for index, address in enumerate(addresses, start=1):
        result = rpc_call(
            rpc_url,
            "eth_getProof",
            [address, [], block_tag],
            timeout_seconds,
            retries,
            backoff_seconds,
        )
        if not isinstance(result, dict):
            raise RuntimeError(
                "eth_getProof returned non-object response "
                f"for address {address}: {type(result).__name__}"
            )

        proof_address = _try_coerce_address(result.get("address"))
        if proof_address is not None and proof_address != address:
            raise RuntimeError(
                "eth_getProof returned mismatched address "
                f"for {address}: got {proof_address}"
            )

        proofs.append(
            {
                "address": address,
                "proof": result,
            }
        )

        if progress_every and index % progress_every == 0:
            print(f"  fetched proofs {index}/{len(addresses)}")

    return proofs


def write_addresses_output(
    out_path: str,
    *,
    source_rpc: str,
    target_block_number: int,
    target_block_hash: str,
    target_block_state_root: str,
    method_used: str,
    from_block: int,
    include_zero_balances: bool,
    raw_discovered: int,
    non_eoa_filtered: int,
    zero_filtered: int,
    excluded_count: int,
    final_addresses: list[str],
    preimage_audit: dict[str, Any] | None,
) -> None:
    try:
        with open(out_path, "w", encoding="utf-8") as file_handle:
            file_handle.write(
                "# Auto-generated by scripts/discover_addresses.py\n"
                f"# source_rpc={source_rpc}\n"
                f"# target_block_number={target_block_number}\n"
                f"# target_block_hash={target_block_hash}\n"
                f"# target_block_state_root={target_block_state_root}\n"
                f"# method={method_used}\n"
                f"# from_block={from_block}\n"
                f"# include_zero_balances={include_zero_balances}\n"
                f"# raw_discovered={raw_discovered}\n"
                f"# non_eoa_filtered={non_eoa_filtered}\n"
                f"# zero_balance_filtered={zero_filtered}\n"
                f"# excluded={excluded_count}\n"
                f"# final_count={len(final_addresses)}\n"
            )
            if preimage_audit is not None:
                file_handle.write(
                    f"# preimage_audit_passed={preimage_audit['passed']}\n"
                    f"# preimages_missing_by_count={preimage_audit['missing_preimages_by_count']}\n"
                    f"# preimages_missing_by_parse={preimage_audit['missing_preimages_by_parse']}\n"
                )
            for address in final_addresses:
                file_handle.write(address + "\n")
    except OSError as exc:
        print(f"ERROR: could not write output file '{out_path}': {exc}", file=sys.stderr)
        sys.exit(1)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_provenance_bundle(
    provenance_dir: Path,
    *,
    manifest: dict[str, Any],
    raw_discovered_addresses: set[str],
    eoa_addresses: set[str],
    nonzero_addresses: set[str],
    final_addresses: list[str],
    account_proofs_payload: dict[str, Any] | None,
) -> None:
    provenance_dir.mkdir(parents=True, exist_ok=True)

    raw_file = provenance_dir / "raw_discovered_addresses.txt"
    eoa_file = provenance_dir / "eoa_addresses.txt"
    nonzero_file = provenance_dir / "nonzero_addresses.txt"
    final_file = provenance_dir / "final_addresses.txt"
    proofs_file = provenance_dir / "account_proofs.json"

    raw_content = "\n".join(sorted(raw_discovered_addresses)) + "\n"
    eoa_content = "\n".join(sorted(eoa_addresses)) + "\n"
    nonzero_content = "\n".join(sorted(nonzero_addresses)) + "\n"
    final_content = "\n".join(final_addresses) + "\n"

    raw_file.write_text(raw_content, encoding="utf-8")
    eoa_file.write_text(eoa_content, encoding="utf-8")
    nonzero_file.write_text(nonzero_content, encoding="utf-8")
    final_file.write_text(final_content, encoding="utf-8")

    proofs_content: str | None = None
    proofs_sha: str | None = None
    if account_proofs_payload is not None:
        proofs_content = json.dumps(account_proofs_payload, indent=2, sort_keys=True) + "\n"
        proofs_file.write_text(proofs_content, encoding="utf-8")
        proofs_sha = _sha256_hex(proofs_content.encode("utf-8"))

    manifest["artifacts"] = {
        "raw_discovered_addresses_file": raw_file.name,
        "eoa_addresses_file": eoa_file.name,
        "nonzero_addresses_file": nonzero_file.name,
        "final_addresses_file": final_file.name,
        "raw_discovered_addresses_sha256": _sha256_hex(raw_content.encode("utf-8")),
        "eoa_addresses_sha256": _sha256_hex(eoa_content.encode("utf-8")),
        "nonzero_addresses_sha256": _sha256_hex(nonzero_content.encode("utf-8")),
        "final_addresses_sha256": _sha256_hex(final_content.encode("utf-8")),
    }
    if proofs_content is not None and proofs_sha is not None:
        manifest["artifacts"]["account_proofs_file"] = proofs_file.name
        manifest["artifacts"]["account_proofs_sha256"] = proofs_sha

    manifest_json = json.dumps(manifest, indent=2, sort_keys=True)
    manifest_content = manifest_json + "\n"
    manifest_file = provenance_dir / "manifest.json"
    manifest_file.write_text(manifest_content, encoding="utf-8")

    checksums_file = provenance_dir / "checksums.sha256"
    checksums_file.write_text(
        "\n".join(
            [
                f"{_sha256_hex(manifest_content.encode('utf-8'))}  {manifest_file.name}",
                f"{manifest['artifacts']['raw_discovered_addresses_sha256']}  {raw_file.name}",
                f"{manifest['artifacts']['eoa_addresses_sha256']}  {eoa_file.name}",
                f"{manifest['artifacts']['nonzero_addresses_sha256']}  {nonzero_file.name}",
                f"{manifest['artifacts']['final_addresses_sha256']}  {final_file.name}",
            ]
            + (
                [f"{manifest['artifacts']['account_proofs_sha256']}  {proofs_file.name}"]
                if proofs_content is not None
                else []
            )
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Automatically discover candidate addresses for migration from a Reth chain. "
            "Writes one normalized address per line to an output file."
        )
    )
    parser.add_argument(
        "--out",
        default="addresses_discovered.txt",
        help="Output text file. Default: addresses_discovered.txt",
    )
    parser.add_argument(
        "--block",
        type=parse_block,
        help="Target block number (decimal or 0x-prefixed hex).",
    )
    parser.add_argument(
        "--fallback-tags",
        default="finalized,safe,latest",
        help="Comma-separated tags to try when --block is omitted. Default: finalized,safe,latest",
    )
    parser.add_argument(
        "--from-block",
        type=parse_block,
        default=0,
        help="Start block for heuristic discovery scan. Default: 0",
    )
    parser.add_argument(
        "--discovery-mode",
        choices=["heuristic", "strict"],
        default="heuristic",
        help=(
            "Discovery mode. 'heuristic' scans balance changes/tx flow, 'strict' uses "
            "debug_accountRange at a fixed block. Default: heuristic"
        ),
    )
    parser.add_argument(
        "--method",
        choices=["auto", "reth-balance-changes", "tx-scan"],
        default="auto",
        help=(
            "Heuristic discovery method. 'auto' tries reth_getBalanceChangesInBlock first, "
            "then falls back to tx-scan if unsupported. Default: auto"
        ),
    )
    parser.add_argument(
        "--prove-preimages",
        action="store_true",
        help=(
            "Run debug_accountRange incompletes audit and fail if preimages are incomplete. "
            "Automatically enabled in strict mode."
        ),
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Page size for debug_accountRange pagination. Default: 1000",
    )
    parser.add_argument(
        "--provenance-dir",
        help=(
            "Optional directory for provenance artifacts (manifest, checksums, address files). "
            "In strict mode defaults to discovery_provenance_<block>."
        ),
    )
    parser.add_argument(
        "--proof-sample-size",
        type=int,
        default=0,
        help=(
            "If > 0, collect eth_getProof artifacts for the first N final addresses "
            "(deterministic sorted order). Default: 0"
        ),
    )
    parser.add_argument(
        "--proof-all",
        action="store_true",
        help="Collect eth_getProof artifacts for all final addresses.",
    )
    parser.add_argument(
        "--include-zero-balances",
        action="store_true",
        help="Include addresses with zero balance at target block (default filters them out).",
    )
    parser.add_argument(
        "--exclude-addresses-file",
        help=(
            "Optional path to a text file with addresses to exclude from output "
            "(same format as snapshot input files)."
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
        help="Print progress every N blocks/pages/addresses. Set 0 to disable. Default: 100",
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
    if args.page_size <= 0:
        print("ERROR: --page-size must be > 0.", file=sys.stderr)
        sys.exit(1)
    if args.proof_sample_size < 0:
        print("ERROR: --proof-sample-size must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.proof_all and args.proof_sample_size > 0:
        print("ERROR: use either --proof-all or --proof-sample-size, not both.", file=sys.stderr)
        sys.exit(1)

    source_rpc = os.getenv("SOURCE_RPC_URL")
    if not source_rpc:
        print("ERROR: SOURCE_RPC_URL env var is not set.", file=sys.stderr)
        sys.exit(1)

    fallback_tags = [tag.strip() for tag in args.fallback_tags.split(",") if tag.strip()]
    if args.block is None and not fallback_tags:
        print("ERROR: no fallback tags provided and --block not set.", file=sys.stderr)
        sys.exit(1)

    try:
        (
            target_block_number,
            target_block_tag,
            target_block_hash,
            resolved_from,
            target_block_state_root,
        ) = resolve_target_block(
            source_rpc,
            args.block,
            fallback_tags,
            args.timeout_seconds,
            args.retries,
            args.backoff_seconds,
        )
    except Exception as exc:
        print(f"ERROR: failed to resolve target block: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.from_block > target_block_number:
        print(
            "ERROR: --from-block must be <= resolved target block "
            f"({target_block_number}), got {args.from_block}.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.exclude_addresses_file:
        exclude_set = load_exclude_set(args.exclude_addresses_file)
    else:
        exclude_set = set()

    print(f"Source RPC: {source_rpc}")
    print(
        "Target block:"
        f" number={target_block_number} tag={target_block_tag} hash={target_block_hash}"
        f" state_root={target_block_state_root} resolved_from={resolved_from}"
    )

    must_prove_preimages = args.prove_preimages or args.discovery_mode == "strict"
    preimage_audit: dict[str, Any] | None = None

    if must_prove_preimages:
        try:
            preimage_audit = run_preimage_audit(
                source_rpc,
                target_block_tag,
                args.page_size,
                args.timeout_seconds,
                args.retries,
                args.backoff_seconds,
                args.progress_every,
            )
        except Exception as exc:
            print(f"ERROR: preimage audit failed: {exc}", file=sys.stderr)
            sys.exit(1)

        print(
            "Preimage audit summary:"
            f" with_incompletes_entries={preimage_audit['with_incompletes']['total_entries']}"
            f" without_incompletes_entries={preimage_audit['without_incompletes']['total_entries']}"
            f" missing_by_count={preimage_audit['missing_preimages_by_count']}"
            f" missing_by_parse={preimage_audit['missing_preimages_by_parse']}"
        )

        if not preimage_audit["passed"]:
            print(
                "ERROR: preimage completeness audit failed. "
                "Cannot claim full address-level completeness for this block.",
                file=sys.stderr,
            )
            sys.exit(2)

    discovered_raw: set[str]
    method_used: str

    if args.discovery_mode == "strict":
        method_used = "debug-account-range-strict"
        if preimage_audit is None:
            print("ERROR: strict mode requires preimage audit results.", file=sys.stderr)
            sys.exit(1)
        discovered_raw = set(preimage_audit["strict_discovered_addresses"])
        print(
            "Strict discovery:"
            f" pages={preimage_audit['without_incompletes']['pages']}"
            f" entries={preimage_audit['without_incompletes']['total_entries']}"
            f" addresses={len(discovered_raw)}"
        )
    else:
        print(f"Scan range: {args.from_block}..{target_block_number}")
        method_used = args.method

        if args.method == "auto":
            try:
                _ = rpc_call(
                    source_rpc,
                    "reth_getBalanceChangesInBlock",
                    [hex(args.from_block)],
                    args.timeout_seconds,
                    args.retries,
                    args.backoff_seconds,
                )
                method_used = "reth-balance-changes"
            except Exception as exc:
                print(
                    "[warn] reth_getBalanceChangesInBlock not available, "
                    f"falling back to tx-scan: {exc}",
                    file=sys.stderr,
                )
                method_used = "tx-scan"

        try:
            if method_used == "reth-balance-changes":
                discovered_raw = discover_with_reth_balance_changes(
                    source_rpc,
                    args.from_block,
                    target_block_number,
                    args.timeout_seconds,
                    args.retries,
                    args.backoff_seconds,
                    args.progress_every,
                )
            else:
                discovered_raw = discover_with_tx_scan(
                    source_rpc,
                    args.from_block,
                    target_block_number,
                    args.timeout_seconds,
                    args.retries,
                    args.backoff_seconds,
                    args.progress_every,
                )
        except Exception as exc:
            print(f"ERROR: discovery failed: {exc}", file=sys.stderr)
            sys.exit(1)

    print(f"Discovery method used: {method_used}")
    print(f"Raw discovered addresses: {len(discovered_raw)}")

    try:
        discovered_eoa, non_eoa_filtered = filter_eoa_at_target(
            source_rpc,
            discovered_raw,
            target_block_tag,
            args.timeout_seconds,
            args.retries,
            args.backoff_seconds,
            args.progress_every,
        )
    except Exception as exc:
        print(f"ERROR: EOA filtering failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"EOA candidates after code filter: {len(discovered_eoa)}")

    if args.include_zero_balances:
        discovered_filtered = discovered_eoa
        zero_filtered = 0
    else:
        try:
            discovered_filtered, zero_filtered = filter_nonzero_at_target(
                source_rpc,
                discovered_eoa,
                target_block_tag,
                args.timeout_seconds,
                args.retries,
                args.backoff_seconds,
                args.progress_every,
            )
        except Exception as exc:
            print(f"ERROR: non-zero filtering failed: {exc}", file=sys.stderr)
            sys.exit(1)

    excluded_count = len(discovered_filtered.intersection(exclude_set))
    final_addresses = sorted(addr for addr in discovered_filtered if addr not in exclude_set)

    if not final_addresses:
        print(
            "ERROR: discovery produced zero addresses after filters. "
            "Check scan range and discovery mode.",
            file=sys.stderr,
        )
        sys.exit(1)

    write_addresses_output(
        args.out,
        source_rpc=source_rpc,
        target_block_number=target_block_number,
        target_block_hash=target_block_hash,
        target_block_state_root=target_block_state_root,
        method_used=method_used,
        from_block=args.from_block,
        include_zero_balances=args.include_zero_balances,
        raw_discovered=len(discovered_raw),
        non_eoa_filtered=non_eoa_filtered,
        zero_filtered=zero_filtered,
        excluded_count=excluded_count,
        final_addresses=final_addresses,
        preimage_audit=preimage_audit,
    )

    proof_mode = "none"
    proof_addresses: list[str] = []
    if args.proof_all:
        proof_mode = "all"
        proof_addresses = list(final_addresses)
    elif args.proof_sample_size > 0:
        proof_mode = "sample"
        proof_addresses = final_addresses[: min(args.proof_sample_size, len(final_addresses))]

    account_proofs_payload: dict[str, Any] | None = None
    if proof_addresses:
        print(f"Collecting eth_getProof for {len(proof_addresses)} address(es) ({proof_mode} mode)...")
        try:
            proofs = fetch_account_proofs(
                source_rpc,
                proof_addresses,
                target_block_tag,
                args.timeout_seconds,
                args.retries,
                args.backoff_seconds,
                args.progress_every,
            )
        except Exception as exc:
            print(f"ERROR: eth_getProof collection failed: {exc}", file=sys.stderr)
            sys.exit(1)

        account_proofs_payload = {
            "generator": "scripts/discover_addresses.py",
            "timestamp_unix": int(time.time()),
            "block": {
                "number": target_block_number,
                "tag": target_block_tag,
                "hash": target_block_hash,
                "state_root": target_block_state_root,
            },
            "proof_mode": proof_mode,
            "proof_count": len(proofs),
            "proofs": proofs,
        }

    provenance_dir: Path | None = None
    if args.provenance_dir:
        provenance_dir = Path(args.provenance_dir)
    elif args.discovery_mode == "strict" or bool(proof_addresses):
        provenance_dir = Path(f"discovery_provenance_{target_block_number}")

    if provenance_dir is not None:
        manifest: dict[str, Any] = {
            "generator": "scripts/discover_addresses.py",
            "timestamp_unix": int(time.time()),
            "source_rpc": source_rpc,
            "discovery_mode": args.discovery_mode,
            "method_used": method_used,
            "block": {
                "number": target_block_number,
                "tag": target_block_tag,
                "hash": target_block_hash,
                "state_root": target_block_state_root,
                "resolved_from": resolved_from,
            },
            "inputs": {
                "from_block": args.from_block,
                "include_zero_balances": args.include_zero_balances,
                "exclude_addresses_file": args.exclude_addresses_file,
                "out": args.out,
                "page_size": args.page_size,
                "prove_preimages": must_prove_preimages,
                "timeout_seconds": args.timeout_seconds,
                "retries": args.retries,
                "backoff_seconds": args.backoff_seconds,
                "proof_mode": proof_mode,
                "proof_sample_size": args.proof_sample_size,
            },
            "counts": {
                "raw_discovered": len(discovered_raw),
                "non_eoa_filtered": non_eoa_filtered,
                "eoa_after_code_filter": len(discovered_eoa),
                "zero_filtered": zero_filtered,
                "excluded": excluded_count,
                "final": len(final_addresses),
                "proof_addresses": len(proof_addresses),
            },
            "preimage_audit": {
                "enabled": preimage_audit is not None,
                "result": (
                    {
                        "with_incompletes": preimage_audit["with_incompletes"],
                        "without_incompletes": preimage_audit["without_incompletes"],
                        "missing_preimages_by_count": preimage_audit["missing_preimages_by_count"],
                        "missing_preimages_by_parse": preimage_audit["missing_preimages_by_parse"],
                        "passed": preimage_audit["passed"],
                    }
                    if preimage_audit is not None
                    else None
                ),
            },
            "account_proofs": {
                "enabled": bool(proof_addresses),
                "mode": proof_mode,
                "proof_count": len(proof_addresses),
            },
            "cli_argv": sys.argv,
        }

        try:
            write_provenance_bundle(
                provenance_dir,
                manifest=manifest,
                raw_discovered_addresses=discovered_raw,
                eoa_addresses=discovered_eoa,
                nonzero_addresses=discovered_filtered,
                final_addresses=final_addresses,
                account_proofs_payload=account_proofs_payload,
            )
        except OSError as exc:
            print(f"ERROR: failed writing provenance bundle: {exc}", file=sys.stderr)
            sys.exit(1)
        print(f"Provenance bundle written: {provenance_dir}")

    print(f"Zero-balance filtered: {zero_filtered}")
    print(f"Non-EOA filtered: {non_eoa_filtered}")
    print(f"Excluded by file: {excluded_count}")
    print(f"Final addresses written: {len(final_addresses)}")
    print(f"Output file: {args.out}")


if __name__ == "__main__":
    main()
