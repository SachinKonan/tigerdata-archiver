"""Deterministic, symlink-safe directory walking shared by plan/archive/delete/report."""
from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from pathlib import Path


def scandir_sorted(path: str) -> list[os.DirEntry]:
    with os.scandir(path) as it:
        return sorted(it, key=lambda e: e.name)


def walk(base: str, members: list[str]) -> Iterator[tuple[str, str, os.stat_result]]:
    """Yield (abs_path, rel_path, lstat) for every member and everything under it.

    Order is fixed (members as given, children sorted by name, parents before children),
    and symlinks are never followed, so the same tree always yields the same sequence.
    Members that no longer exist are skipped silently; callers compare counts.
    """
    for rel in members:
        top = os.path.join(base, rel)
        try:
            st = os.lstat(top)
        except FileNotFoundError:
            continue
        yield top, rel, st
        if stat.S_ISDIR(st.st_mode):
            yield from _walk_dir(top, rel)


def _walk_dir(abs_dir: str, rel_dir: str):
    try:
        entries = scandir_sorted(abs_dir)
    except (FileNotFoundError, NotADirectoryError):
        return
    for e in entries:
        try:
            st = e.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        rel = f"{rel_dir}/{e.name}"
        yield e.path, rel, st
        if stat.S_ISDIR(st.st_mode):
            yield from _walk_dir(e.path, rel)


def is_within(path: str, root: str) -> bool:
    p, r = Path(os.path.abspath(path)), Path(os.path.abspath(root))
    return p == r or r in p.parents
