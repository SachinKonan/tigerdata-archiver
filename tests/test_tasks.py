"""run_shard end to end against the fake server: move, idempotent rerun, and the refusal case."""
import os

import pytest

from tigerdata_archiver import tasks
from tigerdata_archiver.config import Config
from tigerdata_archiver.plan import make_plan
from tigerdata_archiver.state import DELETED, FAILED, Run, write_json

from .conftest import FakeToken


@pytest.fixture
def run(tmp_path, server, monkeypatch):
    monkeypatch.setattr(tasks, "Token", lambda c: FakeToken())
    monkeypatch.setattr(tasks.Remote, "mkdirs", lambda self, c, p: None)
    monkeypatch.setattr(tasks, "_be_gentle", lambda: None)
    src = tmp_path / "src" / "data"
    for i in range(30):
        (src / f"d{i % 3}").mkdir(parents=True, exist_ok=True)
        (src / f"d{i % 3}" / f"f{i}").write_bytes(os.urandom(3000))
    r = Run(tmp_path / "runs" / "r1")
    r.dir.mkdir(parents=True)
    r.path("config.json").write_text(Config("coll", server.url, str(tmp_path / "stage"), str(tmp_path)).to_json())
    write_json(r.path("request.json"), {
        "inputs": [str(src)], "dest": "/dest", "part_size": 16 << 10, "shard_bytes": 40_000,
        "shard_files": 1000, "parallel": 2, "read_threads": 2, "upload_threads": 2,
        "cpus": 1, "mem_gb": 1, "hours": 1, "delete": True})
    make_plan(str(r.dir))
    return r


def test_move_rerun_and_refusal(run, server):
    names = [s["name"] for s in run.plan()["shards"]]
    assert len(names) >= 2
    for n in names:
        assert tasks.run_shard(str(run.dir), n).endswith("done")
        assert run.state(n)["state"] == DELETED
    assert not os.path.exists(run.request()["inputs"][0])
    assert os.path.exists(f"{server.root}/dest/_tda/r1/index.txt")

    # rerun after everything finished: no-op, nothing uploaded
    server.puts.clear()
    for n in names:
        tasks.run_shard(str(run.dir), n)
    assert not server.puts

    # an archive missing on the remote with the local data gone: refuse, upload nothing
    n = names[0]
    os.unlink(f"{server.root}/dest/data/{n}.manifest.json")
    write_json(run.path("state", f"{n}.json"), {"state": "planned"})
    assert "FAILED" in tasks.run_shard(str(run.dir), n)
    st = run.state(n)
    assert st["state"] == FAILED and "refusing to upload" in st["error"]
    assert not server.puts
