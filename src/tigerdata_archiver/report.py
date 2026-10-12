"""Storage report: file count and apparent size per directory at a chosen depth.

Port of the original `script.sbatch` (find | gawk), with two changes:
- files shallower than the aggregation depth are counted under their own directory
  (the original dropped them, under-reporting totals);
- optional per-extension totals.
Top-level children of the root are scanned concurrently by a thread pool.
"""
from __future__ import annotations

import os
import stat
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor


def _ext(name: str) -> str:
    for multi in (".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst"):
        if name.endswith(multi):
            return multi
    _, e = os.path.splitext(name)
    return e.lower() if e else "(none)"


def _scan(root: str, top: str, depth: int, by_ext: bool):
    """Aggregate everything under root/top. Returns (sizes, counts, ext_sizes, ext_counts)."""
    sizes, counts = Counter(), Counter()
    ext_sizes, ext_counts = defaultdict(Counter), defaultdict(Counter)
    stack = [os.path.join(root, top)] if top else [root]
    while stack:
        d = stack.pop()
        rel = os.path.relpath(d, root)
        parts = [] if rel == "." else rel.split(os.sep)
        key = "/".join(parts[:depth]) or "."
        try:
            with os.scandir(d) as it:
                entries = list(it)
        except (PermissionError, FileNotFoundError, NotADirectoryError):
            continue
        for e in entries:
            try:
                st = e.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(st.st_mode):
                if top or parts:  # the root's own children are scanned by their own task
                    stack.append(e.path)
            elif stat.S_ISREG(st.st_mode):
                sizes[key] += st.st_size
                counts[key] += 1
                if by_ext:
                    x = _ext(e.name)
                    ext_sizes[key][x] += st.st_size
                    ext_counts[key][x] += 1
    return sizes, counts, ext_sizes, ext_counts


def human(n: float) -> str:
    for u in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or u == "PiB":
            return f"{n:.1f}{u}" if u != "B" else f"{int(n)}B"
        n /= 1024
    return str(n)


def storage_report(root: str, depth: int = 3, by_ext: bool = False, threads: int = 8,
                   top_ext: int = 5) -> str:
    """Scan root and return the report as Markdown."""
    root = os.path.realpath(root)
    t0 = time.time()
    with os.scandir(root) as it:
        tops = sorted(e.name for e in it if e.is_dir(follow_symlinks=False))
    sizes, counts = Counter(), Counter()
    ext_sizes, ext_counts = defaultdict(Counter), defaultdict(Counter)
    with ThreadPoolExecutor(threads) as ex:
        results = ex.map(lambda t: _scan(root, t, depth, by_ext), [""] + tops)
        for s, c, es, ec in results:
            sizes.update(s)
            counts.update(c)
            for k in es:
                ext_sizes[k].update(es[k])
                ext_counts[k].update(ec[k])
    total_b, total_n = sum(sizes.values()), sum(counts.values())
    lines = [
        "# Storage Report", "",
        f"- **Root:** `{root}`",
        f"- **Aggregation depth:** {depth}",
        f"- **Folders with files:** {len(sizes)}",
        f"- **Total files:** {total_n}",
        f"- **Total storage:** {human(total_b)}",
        f"- **Total bytes:** {total_b}",
        f"- **Generated:** {time.strftime('%a %b %d %H:%M:%S %Z %Y')} ({time.time() - t0:.0f}s)",
        f"- **SLURM job:** {os.environ.get('SLURM_JOB_ID', 'N/A')}",
        "", "## Per-directory usage", "",
    ]
    if by_ext:
        lines += ["| Files | Storage | Bytes | Directory | Top extensions |",
                  "|------:|--------:|------:|-----------|----------------|"]
    else:
        lines += ["| Files | Storage | Bytes | Directory |", "|------:|--------:|------:|-----------|"]
    for key, b in sorted(sizes.items(), key=lambda kv: -kv[1]):
        path = root if key == "." else f"{root}/{key}"
        row = f"| {counts[key]} | {human(b)} | {b} | `{path}` |"
        if by_ext:
            tops_e = ", ".join(f"{x} {human(v)} ({ext_counts[key][x]})"
                               for x, v in ext_sizes[key].most_common(top_ext))
            row += f" {tops_e} |"
        lines.append(row)
    if by_ext:
        all_s, all_c = Counter(), Counter()
        for k in ext_sizes:
            all_s.update(ext_sizes[k])
            all_c.update(ext_counts[k])
        lines += ["", "## By extension", "", "| Extension | Files | Storage | Bytes |", "|---|--:|--:|--:|"]
        lines += [f"| `{x}` | {all_c[x]} | {human(v)} | {v} |" for x, v in all_s.most_common(50)]
    lines += ["", "## Aggregate", "", f"- **Total files:** {total_n}",
              f"- **Total storage:** {human(total_b)}", f"- **Total bytes:** {total_b}", ""]
    return "\n".join(lines)


def report_to_file(root: str, out: str, depth: int, by_ext: bool, threads: int) -> str:
    text = storage_report(root, depth, by_ext, threads)
    with open(out, "w") as f:
        f.write(text)
    return out
