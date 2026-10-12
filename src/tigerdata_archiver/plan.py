"""Split input directories into shards (one tar archive each), packed by size and file count.

Packing is top-down: a directory whose whole subtree fits the budget becomes a single
member; a directory that does not fit is opened and its children packed instead, so a
huge flat directory is packed entry by entry. Items are packed in sorted order, so the
plan is reproducible. Every input directory gets its own shards (named <group>_NNNN).
"""
from __future__ import annotations

import os
import re
import stat
from collections import defaultdict

from .fswalk import scandir_sorted
from .state import Run, write_json


def subtree_totals(root: str) -> dict[str, list[int]]:
    """{dir path: [apparent bytes, entry count]} for root and every directory below it.

    Entry count includes the directory itself, files, symlinks and subdirectories,
    matching how archive/delete count entries. One pass, symlinks not followed.
    """
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    stack = [(root, False)]
    while stack:
        path, done = stack.pop()
        if done:
            parent = os.path.dirname(path)
            if path != root:
                totals[parent][0] += totals[path][0]
                totals[parent][1] += totals[path][1]
            continue
        totals[path][1] += 1  # the directory itself
        stack.append((path, True))
        try:
            entries = scandir_sorted(path)
        except (PermissionError, FileNotFoundError):
            continue
        for e in entries:
            try:
                st = e.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(st.st_mode):
                stack.append((e.path, False))
            else:
                totals[path][0] += st.st_size if stat.S_ISREG(st.st_mode) else 0
                totals[path][1] += 1
    return dict(totals)


def items_for(root: str, totals: dict, max_bytes: int, max_files: int):
    """Yield (abs path, bytes, entries) units that each fit the budget where possible."""
    def expand(path: str):
        b, n = totals.get(path, (0, 1))
        if b <= max_bytes and n <= max_files:
            yield path, b, n
            return
        for e in scandir_sorted(path):
            st = e.stat(follow_symlinks=False)
            if stat.S_ISDIR(st.st_mode):
                yield from expand(e.path)
            else:
                yield e.path, (st.st_size if stat.S_ISREG(st.st_mode) else 0), 1
    yield from expand(root)


def pack(items, max_bytes: int, max_files: int) -> list[list[tuple[str, int, int]]]:
    shards, cur, cb, cn = [], [], 0, 0
    for it in items:
        _, b, n = it
        if cur and (cb + b > max_bytes or cn + n > max_files):
            shards.append(cur)
            cur, cb, cn = [], 0, 0
        cur.append(it)
        cb += b
        cn += n
    if cur:
        shards.append(cur)
    return shards


def group_names(inputs: list[str]) -> list[str]:
    names, seen = [], defaultdict(int)
    for d in inputs:
        base = re.sub(r"[^A-Za-z0-9._-]+", "_", os.path.basename(os.path.abspath(d).rstrip("/"))) or "root"
        seen[base] += 1
        names.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    return names


def make_plan(run_dir: str) -> dict:
    """Walk the request's input dirs and write plan.json + shards/*.files. Runs in Slurm."""
    run = Run(run_dir)
    req = run.request()
    max_bytes, max_files = req["shard_bytes"], req["shard_files"]
    shards, groups = [], {}
    for src, group in zip(req["inputs"], group_names(req["inputs"])):
        src = os.path.abspath(src).rstrip("/")
        base = os.path.dirname(src)
        totals = subtree_totals(src)
        packed = pack(items_for(src, totals, max_bytes, max_files), max_bytes, max_files)
        groups[group] = {"source": src, "bytes": totals[src][0], "entries": totals[src][1], "shards": len(packed)}
        for i, members in enumerate(packed):
            name = f"{group}_{i:04d}"
            run.members_file(name).parent.mkdir(parents=True, exist_ok=True)
            run.members_file(name).write_text(
                "".join(os.path.relpath(p, base) + "\n" for p, _, _ in members))
            shards.append({"name": name, "group": group, "base": base, "source": src,
                           "bytes": sum(b for _, b, _ in members), "entries": sum(n for _, _, n in members),
                           "members": len(members)})
    plan = {"run_id": run.id, "dest": req["dest"], "groups": groups, "shards": shards}
    write_json(run.path("plan.json"), plan)
    return plan
