"""Run directory layout and per-shard state, written atomically.

    runs/<run_id>/
      config.json        settings the run was started with
      request.json       input dirs, destination, options
      plan.json          frozen shards: name, group (input dir), base, bytes, files
      shards/<s>.files   members of shard s, relative to its base (one per line)
      state/<s>.json     state machine + progress for shard s
      progress/<s>.jsonl one line per uploaded part (resume)
      index/<s>.txt      path<TAB>shard<TAB>size<TAB>mtime for every archived file
      jobs.json          shard -> Slurm job id of its latest submission
      logs/              submitit logs
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

PLANNED, UPLOADING, UPLOADED, VERIFIED, DELETED, FAILED = (
    "planned", "uploading", "uploaded", "verified", "deleted", "failed")
DONE_STATES = (DELETED,)


def write_json(path: Path, obj) -> None:
    """Write JSON atomically (tmp file + rename), so readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(json.dumps(obj, indent=1))
    os.replace(tmp, path)


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


class Run:
    def __init__(self, run_dir: str | Path):
        self.dir = Path(run_dir)

    @property
    def id(self) -> str:
        return self.dir.name

    def path(self, *parts: str) -> Path:
        return self.dir.joinpath(*parts)

    def plan(self) -> dict | None:
        return read_json(self.path("plan.json"))

    def request(self) -> dict:
        return read_json(self.path("request.json"), {})

    def members_file(self, shard: str) -> Path:
        return self.path("shards", f"{shard}.files")

    def members(self, shard: str) -> list[str]:
        return [l for l in self.members_file(shard).read_text().splitlines() if l]

    def state(self, shard: str) -> dict:
        return read_json(self.path("state", f"{shard}.json"), {"state": PLANNED})

    def jobs(self) -> dict:
        return read_json(self.path("jobs.json"), {})

    def record_jobs(self, mapping: dict) -> None:
        jobs = self.jobs()
        jobs.update(mapping)
        write_json(self.path("jobs.json"), jobs)


class ShardState:
    """State of one shard, owned by the task processing it."""

    def __init__(self, run: Run, shard: str):
        self.file = run.path("state", f"{shard}.json")
        self._lock = threading.Lock()
        self.data = read_json(self.file, {}) or {}
        self.data.setdefault("state", PLANNED)

    def set(self, state: str | None = None, **fields) -> None:
        with self._lock:
            if state:
                self.data["state"] = state
                self.data.setdefault("history", []).append([state, time.strftime("%Y-%m-%d %H:%M:%S")])
            self.data.update(fields)
            self.data["updated"] = time.time()
            write_json(self.file, self.data)

    def add_progress(self, parts: int, nbytes: int) -> None:
        with self._lock:
            self.data["parts_done"] = self.data.get("parts_done", 0) + parts
            self.data["bytes_done"] = self.data.get("bytes_done", 0) + nbytes
            self.data["updated"] = time.time()
            write_json(self.file, self.data)
