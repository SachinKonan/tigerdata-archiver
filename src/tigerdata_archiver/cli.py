"""`tda` — archive directories to TigerData from a Slurm cluster.

The login node only plans and submits; all reading, uploading and deleting runs in
Slurm jobs (via submitit). Use `tda status RUN --watch` to follow progress.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

from . import config as config_mod
from .config import DEFAULT_PART_SIZE, PART_SIZE_CAP, parse_size
from .state import DELETED, FAILED, Run, write_json


def _runs_dir(cfg) -> Path:
    return Path(cfg.state_dir) / "runs"


def _open_run(cfg, run_id: str) -> Run:
    run = Run(_runs_dir(cfg) / run_id)
    if not run.path("request.json").exists():
        sys.exit(f"no such run: {run_id} (see `tda runs`)")
    return run


# ---------------------------------------------------------------- login
def cmd_login(a):
    from .auth import data_access_scope, https_scope, token_scopes

    cfg = config_mod.load(resolve_https=False)
    c = cfg.collection_id
    info = token_scopes(c)
    steps = f"""
Run these in an interactive terminal (they print a URL to open, then ask for a code):

  1. uv run globus login
  2. uv run globus session consent 'urn:globus:auth:scope:transfer.api.globus.org:all[*{data_access_scope(c)}]'
  3. uv run globus login --gcs {c} --force
     (grants the collection's HTTPS scopes; the token must include both
      {https_scope(c)}
      {data_access_scope(c)} )

Then run `uv run tda login` again."""
    if info is None:
        print(f"No Globus token for collection {c}.{steps}")
        sys.exit(1)
    scope, left = info
    missing = [s for s in (https_scope(c), data_access_scope(c)) if s not in scope]
    if missing:
        print(f"Token for {c} is missing scopes: {missing}{steps}")
        sys.exit(1)
    cfg = config_mod.load()
    life = f"{left / 3600:.1f} h left" if left > 0 else "expired; refreshed on next use"
    print(f"collection   {c}\nhttps server {cfg.https_server}\ntoken        ok ({life}, refreshed automatically)")
    if a.probe:
        from .auth import Token
        from .http import Remote

        r = Remote(cfg.https_server, Token(c), tries=1)
        path = f"{a.probe.rstrip('/')}/.tda_probe_{secrets.token_hex(4)}"
        r.put_bytes(b"probe\n", path)
        print(f"write test   ok (PUT {path}, DELETE http {r.delete(path)})")


# ---------------------------------------------------------------- report
def cmd_report(a):
    from .report import report_to_file, storage_report

    out = os.path.abspath(a.out or f"storage-report-{time.strftime('%Y%m%d-%H%M%S')}.md")
    if a.local:
        Path(out).write_text(storage_report(a.root, a.depth, a.by_ext, a.threads))
        print(f"report written to {out}")
        return
    from .slurm import executor

    cfg = config_mod.load(resolve_https=False)
    run = Run(Path(cfg.state_dir) / "reports")
    ex = executor(run, cfg, "tda-report", cpus=2, mem_gb=4, hours=a.hours)
    job = ex.submit(report_to_file, os.path.realpath(a.root), out, a.depth, a.by_ext, a.threads)
    print(f"submitted report job {job.job_id}; it will write {out}")
    if a.wait:
        print(f"done: {job.result()}")


# ---------------------------------------------------------------- move
def cmd_move(a):
    from .slurm import executor
    from .tasks import plan_and_submit

    cfg = config_mod.load()
    inputs = [os.path.realpath(d) for d in a.dirs]
    for d in inputs:
        if not os.path.isdir(d):
            sys.exit(f"not a directory: {d}")
    for i, x in enumerate(inputs):
        for y in inputs[i + 1:]:
            if x == y or x.startswith(y + "/") or y.startswith(x + "/"):
                sys.exit(f"overlapping inputs: {x} and {y}")
    if not a.dest.startswith("/"):
        sys.exit("--dest must be an absolute path inside the collection, e.g. /myproject/archive")
    part = parse_size(a.part_size)
    if not 0 < part <= PART_SIZE_CAP:
        sys.exit(f"--part-size must be at most {PART_SIZE_CAP} bytes (TigerData rejects larger uploads)")
    read_threads = max(1, min(a.read_threads, a.max_readers // a.parallel))

    run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(2)
    run = Run(_runs_dir(cfg) / run_id)
    run.dir.mkdir(parents=True)
    run.path("config.json").write_text(cfg.to_json())
    write_json(run.path("request.json"), {
        "inputs": inputs, "dest": a.dest.rstrip("/"), "part_size": part,
        "shard_bytes": parse_size(a.shard_size), "shard_files": a.shard_files,
        "parallel": a.parallel, "read_threads": read_threads, "upload_threads": a.upload_threads,
        "cpus": a.cpus, "mem_gb": a.mem_gb, "hours": a.hours, "delete": True, "dry_run": a.dry_run,
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    ex = executor(run, cfg, f"tda-plan-{run_id}", cpus=2, mem_gb=8, hours=a.plan_hours)
    job = ex.submit(plan_and_submit, str(run.dir))
    run.record_jobs({"_plan": job.job_id})
    print(f"run {run_id}: planning job {job.job_id} submitted")
    print(f"  {len(inputs)} input dir(s) -> {cfg.collection_id}:{a.dest}")
    print(f"  {a.parallel} shards at a time, {read_threads} read + {a.upload_threads} upload threads each")
    print("  local data is DELETED after each shard is verified on TigerData" if not a.dry_run
          else "  dry run: plan only, nothing uploaded or deleted")
    print(f"\nfollow progress:  uv run tda status {run_id} --watch")


# ---------------------------------------------------------------- status / runs
def cmd_status(a):
    from .status import show

    cfg = config_mod.load(resolve_https=False)
    run = _open_run(cfg, a.run)
    plan = run.plan()
    if plan:
        print(f"run {run.id} -> {plan['dest']}")
        for g, v in plan["groups"].items():
            print(f"  {g}: {v['bytes'] / 1e9:,.1f} GB, {v['entries']:,} entries, {v['shards']} shard(s)  ({v['source']})")
    show(run, watch=a.watch, interval=a.interval)


def cmd_runs(a):
    cfg = config_mod.load(resolve_https=False)
    d = _runs_dir(cfg)
    for p in sorted(d.iterdir()) if d.exists() else []:
        run = Run(p)
        plan, req = run.plan(), run.request()
        if plan:
            states = [run.state(s["name"]).get("state") for s in plan["shards"]]
            summary = f"{states.count(DELETED)}/{len(states)} shards done, {states.count(FAILED)} failed"
        else:
            summary = "planning"
        print(f"{run.id}  {summary:32s} {', '.join(os.path.basename(i) for i in req.get('inputs', []))}")


# ---------------------------------------------------------------- resume
def cmd_resume(a):
    from .slurm import executor, job_states
    from .tasks import plan_and_submit, submit_shards

    cfg = config_mod.load(resolve_https=False)
    run = _open_run(cfg, a.run)
    jobs = run.jobs()
    live = job_states(list(jobs.values()))
    plan = run.plan()
    if plan is None:
        if str(jobs.get("_plan")) in live:
            print("still planning; nothing to do")
            return
        ex = executor(run, cfg, f"tda-plan-{run.id}", cpus=2, mem_gb=8, hours=a.plan_hours)
        job = ex.submit(plan_and_submit, str(run.dir))
        run.record_jobs({"_plan": job.job_id})
        print(f"planning had not finished; resubmitted planning job {job.job_id}")
        return
    todo, busy, blocked = [], 0, []
    for s in plan["shards"]:
        st = run.state(s["name"])
        if st.get("state") == DELETED:
            continue
        if str(jobs.get(s["name"], "")) in live:
            busy += 1  # never run two jobs on one shard
            continue
        if st.get("state") == FAILED and "refusing to upload" in (st.get("error") or ""):
            blocked.append(s["name"])
            continue
        todo.append(s["name"])
    if blocked:
        print(f"{len(blocked)} shard(s) have no archive on TigerData and no local data; not retrying: "
              f"{', '.join(blocked[:10])}")
    if not todo:
        print(f"nothing to resubmit ({busy} shard(s) still queued or running)")
        return
    submit_shards(run, todo)
    print(f"resubmitted {len(todo)} shard(s); {busy} still queued or running")


# ---------------------------------------------------------------- verify / find / restore
def cmd_verify(a):
    from .auth import Token
    from .http import Remote
    from .tasks import run_config, shard_dest
    from .verify import verify_shard

    cfg = config_mod.load(resolve_https=False)
    run = _open_run(cfg, a.run)
    rcfg, plan = run_config(run), run.plan()
    remote = Remote(rcfg.https_server, Token(rcfg.collection_id), tries=2)
    bad = 0
    for s in plan["shards"]:
        ok, problems, m = verify_shard(remote, shard_dest(plan, s), s["name"])
        bad += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {s['name']}" + ("" if ok else f": {'; '.join(problems)}"))
    print(f"{len(plan['shards']) - bad}/{len(plan['shards'])} shards verified")


def cmd_find(a):
    cfg = config_mod.load(resolve_https=False)
    runs = [_open_run(cfg, a.run)] if a.run else [Run(p) for p in sorted(_runs_dir(cfg).glob("*"))]
    files = []
    for run in runs:
        files += sorted(run.path("index").glob("*.txt"))
    if not files:
        sys.exit("no local indexes found")
    sys.exit(subprocess.run(["grep", "-h", "-E", a.pattern, *map(str, files)]).returncode)


def cmd_restore(a):
    from .auth import Token
    from .http import Remote

    cfg = config_mod.load()
    remote = Remote(cfg.https_server, Token(cfg.collection_id))
    d = a.dest_dir.rstrip("/")
    text = remote.get_text(f"{d}/{a.shard}.manifest.json")
    if text is None:
        sys.exit(f"no manifest at {d}/{a.shard}.manifest.json")
    m = json.loads(text)
    os.makedirs(a.to, exist_ok=True)
    stage = os.path.join(cfg.staging_dir, f"tda_restore_{a.shard}_{os.getpid()}")
    os.makedirs(stage, exist_ok=True)
    tar = subprocess.Popen(["tar", "-xf", "-", "-C", a.to], stdin=subprocess.PIPE)
    try:
        for p in m["parts"]:
            local = os.path.join(stage, p["part"])
            remote.get(f"{d}/{p['part']}", local)
            h = hashlib.sha256()
            with open(local, "rb") as f:
                for chunk in iter(lambda: f.read(8 << 20), b""):
                    h.update(chunk)
                    tar.stdin.write(chunk)
            os.unlink(local)
            if h.hexdigest() != p["sha256"]:
                tar.kill()
                sys.exit(f"checksum mismatch in {p['part']}")
            print(f"{p['part']}: ok", flush=True)
        tar.stdin.close()
        if tar.wait() != 0:
            sys.exit("tar failed")
    finally:
        if os.path.isdir(stage) and not os.listdir(stage):
            os.rmdir(stage)
    print(f"restored {m['entries']:,} entries into {a.to}")


# ---------------------------------------------------------------- main
def main(argv=None):
    p = argparse.ArgumentParser(prog="tda", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("login", help="check Globus login/consents (prints setup steps if missing)")
    s.add_argument("--probe", metavar="DIR", help="also PUT+DELETE a tiny file in this collection dir")
    s.set_defaults(fn=cmd_login)

    s = sub.add_parser("report", help="storage report: files and bytes per directory at a depth")
    s.add_argument("root")
    s.add_argument("--depth", type=int, default=3)
    s.add_argument("--by-ext", action="store_true", help="add per-extension totals")
    s.add_argument("--out", help="output .md (default storage-report-<time>.md in cwd)")
    s.add_argument("--threads", type=int, default=8)
    s.add_argument("--hours", type=float, default=6, help="Slurm time limit")
    s.add_argument("--wait", action="store_true", help="wait for the job (keeps this shell busy)")
    s.add_argument("--local", action="store_true", help="run here instead of Slurm (small dirs only)")
    s.set_defaults(fn=cmd_report)

    s = sub.add_parser("move", help="archive dirs to TigerData, verify, then delete the local copy")
    s.add_argument("dirs", nargs="+")
    s.add_argument("--dest", required=True, help="destination dir inside the collection")
    s.add_argument("--part-size", default=f"{DEFAULT_PART_SIZE >> 20}MiB", help="upload part size (max 1GiB)")
    s.add_argument("--shard-size", default="200GB", help="max bytes per archive")
    s.add_argument("--shard-files", type=int, default=500_000, help="max entries per archive")
    s.add_argument("--parallel", type=int, default=8, help="archives processed at once")
    s.add_argument("--read-threads", type=int, default=8, help="file reads in flight per archive")
    s.add_argument("--upload-threads", type=int, default=2, help="part uploads in flight per archive")
    s.add_argument("--max-readers", type=int, default=32,
                   help="cap on parallel x read-threads (load on the source filesystem)")
    s.add_argument("--cpus", type=int, default=4)
    s.add_argument("--mem-gb", type=int, default=8)
    s.add_argument("--hours", type=float, default=48, help="time limit per shard job")
    s.add_argument("--plan-hours", type=float, default=12, help="time limit for the planning job")
    s.add_argument("--dry-run", action="store_true", help="plan only; see `tda status RUN`")
    s.set_defaults(fn=cmd_move)

    s = sub.add_parser("status", help="progress bars for a run")
    s.add_argument("run")
    s.add_argument("--watch", action="store_true", help="refresh until done (Ctrl-C is safe)")
    s.add_argument("--interval", type=float, default=15)
    s.set_defaults(fn=cmd_status)

    s = sub.add_parser("runs", help="list runs")
    s.set_defaults(fn=cmd_runs)

    s = sub.add_parser("resume", help="reconcile a run and resubmit shards that did not finish")
    s.add_argument("run")
    s.add_argument("--plan-hours", type=float, default=12)
    s.set_defaults(fn=cmd_resume)

    s = sub.add_parser("verify", help="check every shard of a run on TigerData")
    s.add_argument("run")
    s.set_defaults(fn=cmd_verify)

    s = sub.add_parser("find", help="which archive holds a path (grep over the index)")
    s.add_argument("pattern", help="extended regex matched against index lines")
    s.add_argument("--run")
    s.set_defaults(fn=cmd_find)

    s = sub.add_parser("restore", help="download one archive and extract it")
    s.add_argument("shard", help="archive name, e.g. mydata_0003")
    s.add_argument("--dest-dir", required=True, help="collection dir holding the archive")
    s.add_argument("--to", required=True, help="local dir to extract into")
    s.set_defaults(fn=cmd_restore)

    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
