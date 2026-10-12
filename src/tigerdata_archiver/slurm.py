"""submitit helpers: one executor per run, job arrays, job-state queries."""
from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager

import submitit

from .config import Config
from .state import Run


def executor(run: Run, cfg: Config, name: str, *, cpus: int, mem_gb: int, hours: float,
             parallel: int | None = None) -> submitit.AutoExecutor:
    ex = submitit.AutoExecutor(folder=str(run.path("logs")), cluster="slurm")
    params = dict(name=name, timeout_min=int(hours * 60), cpus_per_task=cpus, mem_gb=mem_gb,
                  nodes=1, tasks_per_node=1)  # this cluster otherwise defaults to 2 tasks
    if parallel:
        params["slurm_array_parallelism"] = parallel
    if cfg.slurm_partition:
        params["slurm_partition"] = cfg.slurm_partition
    if cfg.slurm_account:
        params["slurm_account"] = cfg.slurm_account
    ex.update_parameters(**params)
    return ex


@contextmanager
def clean_slurm_env():
    """Hide the current job's SLURM_*/SUBMITIT_* variables while submitting from inside a job,
    so they do not leak into the new job's settings."""
    saved = {k: v for k, v in os.environ.items() if k.startswith(("SLURM_", "SUBMITIT_", "SBATCH_"))}
    for k in saved:
        del os.environ[k]
    try:
        yield
    finally:
        os.environ.update(saved)


def job_states(job_ids: list[str]) -> dict[str, str]:
    """{job id: Slurm state} for jobs still known to squeue (running/pending/...)."""
    ids = sorted({str(j) for j in job_ids if j})
    if not ids:
        return {}
    # Query by user, not by id: squeue errors out if any listed id has already left the queue.
    r = subprocess.run(["squeue", "-h", "-r", "-u", os.environ.get("USER", ""), "-o", "%i %T"],
                       capture_output=True, text=True, timeout=60)
    wanted, out = set(ids), {}
    for line in r.stdout.splitlines():
        jid, _, st = line.partition(" ")
        if jid in wanted:
            out[jid] = st
    return out
