import os

from tigerdata_archiver.plan import group_names, items_for, pack, subtree_totals
from tigerdata_archiver.report import storage_report


def write(path, n):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"x" * n)


def test_packing_splits_oversized_dirs_and_flat_dirs(tmp_path):
    root = tmp_path / "in"
    for i in range(3):
        write(f"{root}/small/f{i}", 10)          # small: 30 bytes total -> one member
    for i in range(10):
        write(f"{root}/flat/f{i:02d}", 40)       # flat: 400 bytes -> must be split per file
    totals = subtree_totals(str(root))
    assert totals[str(root)][0] == 430
    items = list(items_for(str(root), totals, max_bytes=100, max_files=1000))
    names = [os.path.relpath(p, root) for p, _, _ in items]
    assert "small" in names and "flat/f00" in names and "flat" not in names
    shards = pack(items, 100, 1000)
    assert all(sum(b for _, b, _ in s) <= 100 for s in shards)
    assert sorted(p for s in shards for p, _, _ in s) == sorted(p for p, _, _ in items)


def test_file_count_budget(tmp_path):
    root = tmp_path / "in"
    for i in range(20):
        write(f"{root}/d/f{i:02d}", 1)
    totals = subtree_totals(str(root))
    shards = pack(items_for(str(root), totals, 10**9, 8), 10**9, 8)
    assert all(sum(n for _, _, n in s) <= 8 for s in shards) and len(shards) >= 3


def test_group_names_unique():
    assert group_names(["/a/data", "/b/data", "/c/x y"]) == ["data", "data_2", "x_y"]


def test_report_counts_shallow_files(tmp_path):
    write(f"{tmp_path}/top.txt", 5)                # depth 0: dropped by the old script
    write(f"{tmp_path}/a/one.txt", 7)              # depth 1
    write(f"{tmp_path}/a/b/c/d/deep.gz", 100)      # deeper than depth: rolls up to a/b/c
    text = storage_report(str(tmp_path), depth=3, by_ext=True, threads=2)
    assert "**Total bytes:** 112" in text
    assert f"`{os.path.realpath(tmp_path)}/a/b/c` |" in text
    assert "`.gz`" in text
