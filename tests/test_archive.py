import filecmp
import glob
import json
import os
import subprocess

import pytest

from tigerdata_archiver.archive import ArchiveOptions, archive_shard
from tigerdata_archiver.delete import delete_shard
from tigerdata_archiver.verify import verify_shard


def make_tree(root):
    os.makedirs(f"{root}/data/a/b", exist_ok=True)
    os.makedirs(f"{root}/data/empty", exist_ok=True)
    for i in range(50):
        with open(f"{root}/data/a/f{i:03d}.txt", "w") as f:
            f.write(f"file {i}\n" * (i + 1))
    with open(f"{root}/data/a/b/big.bin", "wb") as f:
        f.write(os.urandom(300_000))  # bigger than big_file below: streamed, not prefetched
    os.symlink("a/f001.txt", f"{root}/data/link")
    return f"{root}/data"


def run_archive(remote, tmp_path, base, members, part=64 << 10, name="data_0000"):
    opts = ArchiveOptions(part_size=part, read_threads=4, upload_threads=2, big_file=100_000)
    parts = []
    m = archive_shard(remote, "/dest/data", name, base, members, opts, str(tmp_path / "stage"),
                      str(tmp_path / f"{name}.progress.jsonl"), str(tmp_path / f"{name}.index.txt"),
                      lambda rec, skipped: parts.append((rec["part"], skipped)), log=lambda *_: None)
    return m, parts


def test_roundtrip(tmp_path, server, remote):
    src = make_tree(tmp_path / "src")
    m, parts = run_archive(remote, tmp_path, str(tmp_path / "src"), ["data"])
    assert len(m["parts"]) > 3 and m["n_warnings"] == 0
    assert m["entries"] == 1 + 3 + 50 + 1 + 1  # data, a, b, empty, files, big, link
    # restore exactly as documented: cat parts | tar -x
    out = tmp_path / "restored"
    out.mkdir()
    files = sorted(glob.glob(f"{server.root}/dest/data/data_0000.tar.*"))
    subprocess.run(f"cat {' '.join(files)} | tar -xf - -C {out}", shell=True, check=True)
    cmp = filecmp.dircmp(src, out / "data")
    assert not cmp.diff_files and not cmp.left_only and not cmp.right_only
    assert os.readlink(out / "data/link") == "a/f001.txt"
    index = open(tmp_path / "data_0000.index.txt").read().splitlines()
    assert len(index) == 52 and all(len(l.split("\t")) == 4 for l in index)
    assert json.load(open(f"{server.root}/dest/data/data_0000.manifest.json"))["entries"] == m["entries"]


def test_resume_skips_uploaded_parts(tmp_path, server, remote):
    make_tree(tmp_path / "src")
    run_archive(remote, tmp_path, str(tmp_path / "src"), ["data"])
    server.puts.clear()
    _, parts = run_archive(remote, tmp_path, str(tmp_path / "src"), ["data"])
    assert all(skipped for _, skipped in parts)
    assert not [p for p in server.puts if ".tar." in p]  # no part re-uploaded


def test_stuck_remote_file_is_deleted_then_retried(tmp_path, server, remote):
    make_tree(tmp_path / "src")
    server.stuck.add("/dest/data/data_0000.tar.00001")
    m, _ = run_archive(remote, tmp_path, str(tmp_path / "src"), ["data"])
    assert os.path.getsize(f"{server.root}/dest/data/data_0000.tar.00001") == m["parts"][1]["bytes"]


def test_verify_detects_missing_part(tmp_path, server, remote):
    make_tree(tmp_path / "src")
    run_archive(remote, tmp_path, str(tmp_path / "src"), ["data"])
    assert verify_shard(remote, "/dest/data", "data_0000")[0]
    os.unlink(f"{server.root}/dest/data/data_0000.tar.00002")
    ok, problems, _ = verify_shard(remote, "/dest/data", "data_0000")
    assert not ok and "00002" in problems[0]


def test_delete_refuses_when_files_added(tmp_path, server, remote):
    src = make_tree(tmp_path / "src")
    m, _ = run_archive(remote, tmp_path, str(tmp_path / "src"), ["data"])
    open(f"{src}/a/new.txt", "w").write("added after upload")
    deleted, msg = delete_shard(str(tmp_path / "src"), ["data"], m["entries"], src)
    assert not deleted and os.path.exists(src)
    os.unlink(f"{src}/a/new.txt")
    deleted, _ = delete_shard(str(tmp_path / "src"), ["data"], m["entries"], src)
    assert deleted and not os.path.exists(src)


def test_delete_only_members_and_prunes_empty_dirs(tmp_path):
    root = tmp_path / "src" / "data"
    (root / "x").mkdir(parents=True)
    (root / "y").mkdir()
    (root / "x" / "1").write_text("1")
    (root / "y" / "2").write_text("2")
    deleted, _ = delete_shard(str(tmp_path / "src"), ["data/x/1"], 1, str(root))
    assert deleted and not (root / "x").exists() and (root / "y" / "2").exists()
    with pytest.raises(ValueError):
        delete_shard(str(tmp_path / "src"), ["data/../other"], 99, str(root))
