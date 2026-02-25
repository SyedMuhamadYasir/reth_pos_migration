# Genesis Files Notes

The migration scripts in `scripts/` do not directly read genesis files.

This folder is for humans to keep execution-layer genesis artifacts such as:
- `genesis_old.json` for the old network
- `genesis_new.json` for the new network (new `chainId`, admin `alloc`, and related config)

Keep only public chain configuration in genesis files (for example chain ID, fork activation, alloc addresses, bootnodes). Never store private keys or secrets in genesis content.

It is acceptable to keep real genesis files untracked locally and commit only sanitized examples to Git.
