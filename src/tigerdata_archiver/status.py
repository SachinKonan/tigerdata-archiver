"""Progress of a run: tqdm bars from the run's state files plus one squeue call.

Reads only small files in the run directory, never the data, so it is cheap to run on
a login node; stopping it has no effect on the jobs.
"""
from __future__ import annotations

import time
from collections import Counter

from tqdm import tqdm

from .slurm import job_states
from .state import DELETED, FAILED, PLANNED, UPLOADED, UPLOADING, VERIFIED, Run


def snapshot(run: Run) -> dict:
    plan = run.plan()
    if plan is None:
        return {"planned": False}
    jobs = run.jobs()
    live = job_states(list(jobs.values()))
    groups = {g: {"total": 0, "done": 0} for g in plan["groups"]}
    states, failed, total, done = Counter(), [], 0, 0
    for s in plan["shards"]:
        st = run.state(s["name"])
        state = st.get("state", PLANNED)
        job = live.get(str(jobs.get(s["name"], "")))
        if job and state != DELETED:  # a live job outranks a failure from an earlier attempt
            state = "queued" if job == "PENDING" else (state if state not in (PLANNED, FAILED) else "starting")
        elif state in (PLANNED, UPLOADING, UPLOADED, VERIFIED) and s["name"] in jobs:
            state = "stopped"  # job gone without finishing: needs `tda resume`
        states[state] += 1
        b = s["bytes"] if state == DELETED else min(st.get("bytes_done", 0), s["bytes"])
        groups[s["group"]]["total"] += s["bytes"]
        groups[s["group"]]["done"] += b
        total += s["bytes"]
        done += b
        if state in (FAILED, "stopped"):
            failed.append((s["name"], state, st.get("error") or "", st.get("job_id", "")))
    return {"planned": True, "groups": groups, "states": states, "failed": failed,
            "total": total, "done": done, "n": len(plan["shards"])}


def show(run: Run, watch: bool = False, interval: float = 15) -> None:
    snap = snapshot(run)
    if not snap["planned"]:
        print(f"run {run.id}: still planning (walking the input directories)")
        if not watch:
            return
        while not snap["planned"]:
            time.sleep(interval)
            snap = snapshot(run)
    fmt = "{desc:<28} {percentage:5.1f}%|{bar}| {n_fmt}/{total_fmt}B [{elapsed}<{remaining}, {rate_fmt}]"
    overall = tqdm(total=snap["total"], initial=snap["done"], desc="overall", unit="B", unit_scale=True, unit_divisor=1024,
                   bar_format=fmt, position=0, dynamic_ncols=True)
    bars = {g: tqdm(total=v["total"], initial=v["done"], desc=f"  {g}"[:28], unit="B", unit_scale=True, unit_divisor=1024,
                    bar_format=fmt, position=i + 1, dynamic_ncols=True)
            for i, (g, v) in enumerate(snap["groups"].items())}
    line = tqdm(total=0, position=len(bars) + 1, bar_format="{desc}")
    try:
        while True:
            overall.n = snap["done"]
            overall.refresh()
            for g, b in bars.items():
                b.n = snap["groups"][g]["done"]
                b.refresh()
            st = snap["states"]
            line.set_description_str(
                f"shards: {st[DELETED]}/{snap['n']} done · " +
                " · ".join(f"{k} {v}" for k, v in sorted(st.items()) if k != DELETED and v))
            if not watch or (st[DELETED] == snap["n"]) or (sum(st.values()) == st[DELETED] + st[FAILED] + st["stopped"]):
                break
            time.sleep(interval)
            snap = snapshot(run)
    except KeyboardInterrupt:
        pass
    finally:
        for b in [overall, *bars.values(), line]:
            b.close()
    if snap["failed"]:
        print(f"\n{len(snap['failed'])} shard(s) need attention (fix the cause, then `tda resume {run.id}`):")
        for name, state, err, job in snap["failed"][:20]:
            print(f"  {name} [{state}] job {job}: {err[:200]}")
        print(f"  logs: {run.path('logs')}")
