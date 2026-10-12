"""Check an uploaded shard against its manifest, using only HTTPS HEAD/GET.

(`globus ls` is avoided: it truncates at 1000 entries and times out on big directories.)
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

from .http import Remote


def verify_shard(remote: Remote, dest_dir: str, shard: str) -> tuple[bool, list[str], dict | None]:
    """(ok, problems, manifest). ok means every part is present at exactly its recorded size,
    the file list exists, nothing extra exists, and nothing was skipped while archiving."""
    text = remote.get_text(f"{dest_dir}/{shard}.manifest.json")
    if text is None:
        return False, ["manifest missing"], None
    m = json.loads(text)
    problems = []
    with ThreadPoolExecutor(8) as ex:
        sizes = list(ex.map(lambda p: remote.head(f"{dest_dir}/{p['part']}"), m["parts"]))
    for p, got in zip(m["parts"], sizes):
        if got != p["bytes"]:
            problems.append(f"{p['part']}: remote size {got}, manifest {p['bytes']}")
    if remote.head(f"{dest_dir}/{shard}.filelist.txt") is None:
        problems.append("filelist missing")
    if remote.head(f"{dest_dir}/{shard}.tar.{len(m['parts']):05d}") is not None:
        problems.append("extra part beyond the manifest")
    if m.get("n_warnings"):
        problems.append(f"{m['n_warnings']} files were skipped or changed while archiving "
                        f"(first: {m['warnings'][0]}); not deleting")
    return not problems, problems[:10], m
