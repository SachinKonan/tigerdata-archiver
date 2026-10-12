"""Functions that run inside Slurm jobs (submitted with submitit)."""
from __future__ import annotations

import os
import socket
import subprocess
import traceback

from .archive import ArchiveOptions, archive_shard
from .auth import Token
from .config import Config
from .delete import delete_shard
from .http import Remote
from .plan import make_plan
from .slurm import clean_slurm_env, executor
from .state import DELETED, FAILED, UPLOADED, UPLOADING, VERIFIED, Run, ShardState
from .verify import verify_shard


def _log(msg: str) -> None:
    print(msg, flush=True)


def _be_gentle() -> None:
    """Lowest I/O priority for this process, to limit load on the shared filesystem."""
    subprocess.run(["ionice", "-c3", "-p", str(os.getpid())], capture_output=True)


def run_config(run: Run) -> Config:
    return Config.from_json(run.path("config.json").read_text())


def shard_dest(plan: dict, shard: dict) -> str:
    return f"{plan['dest'].rstrip('/')}/{shard['group']}"


def run_shard(run_dir: str, name: str) -> str:
    """Archive → verify → delete one shard. Idempotent: safe to rerun after any failure.

    - manifest already on TigerData: skip straight to verify/delete
    - no manifest and the local data is gone: refuse (never upload over a finished archive)
    - otherwise (re)archive; parts uploaded by an earlier attempt are skipped
    """
    _be_gentle()
    run = Run(run_dir)
    cfg, plan, req = run_config(run), run.plan(), run.request()
    shard = next(s for s in plan["shards"] if s["name"] == name)
    st = ShardState(run, name)
    st.set(job_id=os.environ.get("SLURM_ARRAY_JOB_ID", "") and
           f"{os.environ['SLURM_ARRAY_JOB_ID']}_{os.environ.get('SLURM_ARRAY_TASK_ID', '')}"
           or os.environ.get("SLURM_JOB_ID", ""),
           host=socket.gethostname(), error=None, traceback=None)
    if st.data.get("state") == DELETED:
        return f"{name}: already deleted"
    for sub in ("index", "progress"):
        run.path(sub).mkdir(parents=True, exist_ok=True)
    remote = Remote(cfg.https_server, Token(cfg.collection_id))
    dest = shard_dest(plan, shard)
    members = run.members(name)
    try:
        if remote.exists(f"{dest}/{name}.manifest.json"):
            _log(f"{name}: archive already on TigerData")
        else:
            present = [m for m in members if os.path.lexists(os.path.join(shard["base"], m))]
            if not present:
                st.set(FAILED, error="local data is gone but no finished archive exists on TigerData; "
                                     "refusing to upload (check the remote and your local copy)")
                return f"{name}: FAILED (local data gone, no archive)"
            remote.mkdirs(cfg.collection_id, dest)
            opts = ArchiveOptions(part_size=req["part_size"], read_threads=req["read_threads"],
                                  upload_threads=req["upload_threads"])
            stage = os.path.join(cfg.staging_dir, f"tda_{run.id}_{name}_{os.getpid()}")
            st.set(UPLOADING, parts_done=0, bytes_done=0, files_done=0)

            def on_part(rec, skipped):
                st.add_progress(1, rec["bytes"])
                how = "already uploaded" if skipped else f"uploaded in {rec['upload_s']}s"
                _log(f"{rec['part']}: {rec['bytes'] / 1e9:.2f} GB {how}")

            m = archive_shard(remote, dest, name, shard["base"], members, opts, stage,
                              str(run.path("progress", f"{name}.jsonl")), str(run.path("index", f"{name}.txt")),
                              on_part, _log)
            try:
                os.rmdir(stage)
            except OSError:
                pass
            _log(f"{name}: archived {m['total_bytes'] / 1e9:.1f} GB in {len(m['parts'])} parts, "
                 f"{m['entries']:,} entries, {m['elapsed_s']}s")
        st.set(UPLOADED)

        ok, problems, manifest = verify_shard(remote, dest, name)
        if not ok:
            st.set(FAILED, error="verification failed: " + "; ".join(problems))
            return f"{name}: FAILED verification"
        st.set(VERIFIED, archived_bytes=manifest["total_bytes"], bytes_done=manifest["total_bytes"])
        if not run.path("index", f"{name}.txt").exists():
            remote.get(f"{dest}/{name}.filelist.txt", str(run.path("index", f"{name}.txt")))

        if req.get("delete", True):
            deleted, msg = delete_shard(shard["base"], members, manifest["entries"], shard["source"])
            _log(f"{name}: {msg}")
            if not deleted:
                st.set(FAILED, error=msg)
                return f"{name}: FAILED delete check"
            st.set(DELETED, freed_bytes=manifest["total_bytes"])
        finalize_if_done(run, remote)
        return f"{name}: done"
    except Exception as e:
        st.set(FAILED, error=f"{type(e).__name__}: {e}", traceback=traceback.format_exc()[-2000:])
        raise


def finalize_if_done(run: Run, remote: Remote) -> None:
    """When every shard is finished, upload the combined index and plan next to the archives."""
    plan = run.plan()
    final = DELETED if run.request().get("delete", True) else VERIFIED
    if any(run.state(s["name"]).get("state") != final for s in plan["shards"]):
        return
    index = run.path("index.txt")
    with open(index, "w") as out:
        out.write("path\tshard\tsize\tmtime\n")
        for s in plan["shards"]:
            p = run.path("index", f"{s['name']}.txt")
            if p.exists():
                out.write(p.read_text())
    meta = f"{plan['dest'].rstrip('/')}/_tda/{run.id}"
    remote.mkdirs(run_config(run).collection_id, meta)
    remote.put(str(index), f"{meta}/index.txt", _log)
    remote.put(str(run.path("plan.json")), f"{meta}/plan.json", _log)
    _log(f"run complete; index at {meta}/index.txt")


def submit_shards(run: Run, names: list[str]) -> dict:
    """Submit shards as one job array; record shard -> job id."""
    cfg, req = run_config(run), run.request()
    ex = executor(run, cfg, f"tda-{run.id}", cpus=req["cpus"], mem_gb=req["mem_gb"],
                  hours=req["hours"], parallel=req["parallel"])
    with clean_slurm_env(), ex.batch():
        jobs = [ex.submit(run_shard, str(run.dir), n) for n in names]
    mapping = {n: j.job_id for n, j in zip(names, jobs)}
    run.record_jobs(mapping)
    return mapping


def plan_and_submit(run_dir: str) -> str:
    """Planning job: walk the inputs, freeze the plan, then submit the shard array."""
    _be_gentle()
    run = Run(run_dir)
    plan = make_plan(run_dir)
    names = [s["name"] for s in plan["shards"]]
    if run.request().get("dry_run"):
        return f"planned {len(names)} shards (dry run, nothing submitted)"
    submit_shards(run, names)
    return f"planned and submitted {len(names)} shards"
