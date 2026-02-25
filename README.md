# Reth PoS Balance Migration Toolkit

Migration-grade toolkit for moving account balances from an old Reth-based private network to a new Reth network.

## Overview

This repository snapshots balances from an old chain at a fixed block, then replays those balances onto a new chain that can use a different `chainId`. The process preserves account addresses and final balances while intentionally discarding old chain history and transaction lineage.

## Repository Structure

```text
reth_pos_migration/
  README.md                        # usage, workflow, and safety notes
  requirements.txt                 # Python dependencies
  .gitignore                       # local/runtime files to keep out of git

  scripts/
    snapshot_balances.py           # deterministic snapshot from old chain
    migrate_balances.py            # replay balances to new chain from admin wallet
    verify_balances.py             # verify new chain balances match snapshot
    migration_helper.py            # utility: sum/check snapshot totals

  examples/
    addresses_example.txt          # sample addresses input format
    balances_snapshot_example.json # sample snapshot output structure

  genesis/
    README.md                      # notes about storing old/new genesis files
```

## Requirements

- Python 3.10+
- Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment Variables

```bash
export SOURCE_RPC_URL="http://old-node:8545"   # for snapshot_balances.py
export TARGET_RPC_URL="http://new-node:8545"   # for migrate/verify scripts
export ADMIN_PRIVATE_KEY="0x..."               # admin wallet on the NEW chain
export CHAIN_ID="4567"                         # new chainId in decimal
```

## High-Level Workflow

1. Snapshot balances from old chain at a fixed block.

```bash
python scripts/snapshot_balances.py \
  --addresses-file examples/addresses_example.txt \
  --out balances_snapshot.json \
  --block 120000
```

If you omit `--block`, the script resolves a single fixed snapshot block from fallback tags (`finalized,safe,latest`) and still snapshots at that pinned block number.

2. Prepare and start the new Reth network.
- Build `genesis_new.json` with the new `chainId`.
- Fund the admin wallet in genesis `alloc`.
- Start validators/execution clients and confirm RPC is healthy.

3. Replay balances onto new chain (dry-run first, then live run).

```bash
python scripts/migrate_balances.py \
  --snapshot balances_snapshot.json \
  --dry-run

python scripts/migrate_balances.py \
  --snapshot balances_snapshot.json \
  --state-file migration_state.json
```

4. Verify migrated balances.

```bash
python scripts/verify_balances.py \
  --snapshot balances_snapshot.json
```

## Safety and Correctness Notes

- `snapshot_balances.py` resolves and pins one exact block with `eth_getBlockByNumber`, then queries all balances against that fixed block tag.
- RPC fetch failures are recorded in `failed_addresses` and cause a non-zero exit by default unless `--allow-partial` is explicitly used.
- `migrate_balances.py` is resumable via a state file and uses in-flight transaction reconciliation to reduce replay risk.
- The migration scripts never print the admin private key.
