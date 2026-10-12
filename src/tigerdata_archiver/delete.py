"""Delete the local copy of a verified shard: exactly its members, nothing else."""
from __future__ import annotations

import os
import shutil

from .fswalk import is_within, walk


def count_local(base: str, members: list[str]) -> int:
    return sum(1 for _ in walk(base, members))


def delete_shard(base: str, members: list[str], archived_entries: int, source_root: str) -> tuple[bool, str]:
    """Delete members if the local tree has no more entries than were archived.

    Returns (deleted, message). If something was added locally after the upload the
    shard is left alone. Afterwards, directories emptied by the delete are removed up
    to and including `source_root`.
    """
    local = count_local(base, members)
    if local > archived_entries:
        return False, f"local has {local:,} entries but only {archived_entries:,} were archived; not deleting"
    for rel in members:
        p = os.path.join(base, rel)
        if not is_within(p, source_root):
            raise ValueError(f"refusing to delete {p}: outside {source_root}")
        if os.path.islink(p) or (os.path.lexists(p) and not os.path.isdir(p)):
            os.unlink(p)
        elif os.path.isdir(p):
            shutil.rmtree(p)
    for rel in members:
        d = os.path.dirname(os.path.join(base, rel))
        while is_within(d, source_root):
            try:
                os.rmdir(d)  # only succeeds if empty
            except OSError:
                break
            d = os.path.dirname(d)
    return True, f"deleted {len(members):,} members ({local:,} entries)"
