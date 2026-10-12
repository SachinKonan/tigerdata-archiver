"""Write one shard as a tar stream, cut into parts, and upload them in parallel.

Reading: a thread pool prefetches upcoming small files (reads on a network filesystem
are latency-bound, so several requests in flight help); one writer appends members to
the tar in a fixed order, so the archive is deterministic and restorable with plain
`cat <name>.tar.* | tar -xf -`. Files larger than --big-file are streamed directly.

Uploading: the tar stream is cut into fixed-size parts staged on local scratch; a pool
of upload threads PUTs them while the writer keeps going. Parts already uploaded by an
earlier attempt (same index, size and sha256 in the progress log) are skipped.
"""
from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import queue
import stat
import tarfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .fswalk import walk
from .http import Remote


@dataclass
class ArchiveOptions:
    part_size: int
    read_threads: int = 8
    upload_threads: int = 2
    prefetch_bytes: int = 1 << 30
    big_file: int = 64 << 20


class PartSink:
    """File-like object: bytes written are cut into parts and handed to upload threads."""

    def __init__(self, name, stage, part_size, already, upload_one, threads, on_part):
        self.name, self.stage, self.part_size = name, stage, part_size
        self.already, self.upload_one, self.on_part = already, upload_one, on_part
        self.parts, self.idx, self.error = [], 0, None
        self.q: queue.Queue = queue.Queue(maxsize=threads)
        self.workers = [threading.Thread(target=self._work, daemon=True) for _ in range(threads)]
        for w in self.workers:
            w.start()
        self._open()

    def _open(self):
        self.pname = f"{self.name}.tar.{self.idx:05d}"
        self.ppath = os.path.join(self.stage, self.pname)
        self.f = open(self.ppath, "wb")
        self.h, self.n = hashlib.sha256(), 0

    def write(self, b) -> int:
        if self.error:
            raise RuntimeError(f"upload failed: {self.error}")
        mv, total = memoryview(b), len(b)
        while mv:
            take = mv[: self.part_size - self.n]
            self.f.write(take)
            self.h.update(take)
            self.n += len(take)
            mv = mv[len(take):]
            if self.n == self.part_size:
                self._finish()
                self._open()
        return total

    def _finish(self):
        self.f.close()
        rec = {"part": self.pname, "bytes": self.n, "sha256": self.h.hexdigest()}
        self.parts.append(rec)
        self.idx += 1
        prev = self.already.get(rec["part"])
        if prev and prev["bytes"] == rec["bytes"] and prev["sha256"] == rec["sha256"]:
            os.unlink(self.ppath)
            self.on_part(rec, skipped=True)
        else:
            self.q.put((self.ppath, rec))

    def _work(self):
        while True:
            item = self.q.get()
            if item is None:
                return
            path, rec = item
            try:
                if not self.error:
                    t0 = time.time()
                    self.upload_one(path, rec)
                    rec["upload_s"] = round(time.time() - t0, 1)
                    self.on_part(rec, skipped=False)
            except BaseException as e:
                self.error = e
            finally:
                os.unlink(path)

    def close(self):
        """Flush the last part and wait for all uploads."""
        if self.n:
            self._finish()
        else:
            self.f.close()
            os.unlink(self.ppath)
        for _ in self.workers:
            self.q.put(None)
        for w in self.workers:
            w.join()
        if self.error:
            raise RuntimeError(f"upload failed: {self.error}")


_names: dict = {}


def _owner(uid: int, gid: int) -> tuple[str, str]:
    if (uid, gid) not in _names:
        try:
            u = pwd.getpwuid(uid).pw_name
        except KeyError:
            u = ""
        try:
            g = grp.getgrgid(gid).gr_name
        except KeyError:
            g = ""
        _names[(uid, gid)] = (u, g)
    return _names[(uid, gid)]


def _tarinfo(abs_path: str, rel: str, st: os.stat_result) -> tarfile.TarInfo | None:
    ti = tarfile.TarInfo(rel)
    ti.mode = stat.S_IMODE(st.st_mode)
    ti.uid, ti.gid = st.st_uid, st.st_gid
    ti.uname, ti.gname = _owner(st.st_uid, st.st_gid)
    ti.mtime = int(st.st_mtime)
    if stat.S_ISDIR(st.st_mode):
        ti.type = tarfile.DIRTYPE
    elif stat.S_ISLNK(st.st_mode):
        ti.type, ti.linkname = tarfile.SYMTYPE, os.readlink(abs_path)
    elif stat.S_ISREG(st.st_mode):
        ti.type, ti.size = tarfile.REGTYPE, st.st_size
    else:
        return None  # sockets, fifos, devices
    return ti


def _read(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def archive_shard(remote: Remote, dest_dir: str, shard: str, base: str, members: list[str],
                  opts: ArchiveOptions, stage: str, progress_path: str, index_path: str,
                  on_part, log=print) -> dict:
    """Archive `members` (relative to `base`) to <dest_dir>/<shard>.tar.NNNNN; return the manifest."""
    os.makedirs(stage, exist_ok=True)
    already = {}
    if os.path.exists(progress_path):
        for line in open(progress_path):
            p = json.loads(line)
            already[p["part"]] = p
    plock = threading.Lock()

    def upload_one(path, rec):
        remote.put(path, f"{dest_dir}/{rec['part']}", log)
        with plock, open(progress_path, "a") as pf:
            pf.write(json.dumps(rec) + "\n")

    sink = PartSink(shard, stage, opts.part_size, already, upload_one, opts.upload_threads, on_part)
    warnings, entries, files, t0 = [], 0, 0, time.time()
    window: deque = deque()
    inflight = 0
    it = walk(base, members)
    pending = None

    with open(index_path, "w") as index, ThreadPoolExecutor(opts.read_threads) as pool:
        tf = tarfile.open(fileobj=sink, mode="w|", format=tarfile.PAX_FORMAT, bufsize=1 << 20)

        def fill():
            nonlocal pending, inflight
            while len(window) < opts.read_threads * 8:
                if pending is None:
                    pending = next(it, None)
                    if pending is None:
                        return
                abs_path, rel, st = pending
                small = stat.S_ISREG(st.st_mode) and st.st_size <= opts.big_file
                if small and window and inflight + st.st_size > opts.prefetch_bytes:
                    return
                fut = pool.submit(_read, abs_path) if small else None
                inflight += st.st_size if small else 0
                window.append((abs_path, rel, st, fut))
                pending = None

        while True:
            fill()
            if not window:
                break
            abs_path, rel, st, fut = window.popleft()
            ti = _tarinfo(abs_path, rel, st)
            if ti is None:
                warnings.append(f"skipped special file: {rel}")
                continue
            try:
                if fut is not None:
                    inflight -= st.st_size
                    data = fut.result()
                    if len(data) != st.st_size:
                        warnings.append(f"changed while reading: {rel}")
                    ti.size = len(data)
                    tf.addfile(ti, _BytesReader(data))
                elif ti.type == tarfile.REGTYPE:
                    with open(abs_path, "rb") as f:
                        tf.addfile(ti, f)  # raises if the file shrank; the shard then fails
                else:
                    tf.addfile(ti)
            except (PermissionError, FileNotFoundError) as e:
                warnings.append(f"unreadable: {rel}: {e.__class__.__name__}")
                continue
            entries += 1
            if ti.type in (tarfile.REGTYPE, tarfile.SYMTYPE):
                files += 1
                safe = rel.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                index.write(f"{safe}\t{shard}\t{ti.size}\t{ti.mtime}\n")
        tf.close()
    sink.close()

    manifest = {
        "format": "tigerdata-archiver/1", "shard": shard, "base": base, "dest": dest_dir,
        "parts": sink.parts, "total_bytes": sum(p["bytes"] for p in sink.parts),
        "entries": entries, "files": files, "warnings": warnings[:1000], "n_warnings": len(warnings),
        "elapsed_s": round(time.time() - t0), "finished": time.strftime("%Y-%m-%d %H:%M:%S"),
        "restore": f"cat {shard}.tar.* | tar -xf -   (recreates paths relative to {base})",
    }
    remote.put(index_path, f"{dest_dir}/{shard}.filelist.txt", log)
    # The manifest goes last: its presence on TigerData means the archive is complete.
    remote.put_bytes(json.dumps(manifest, indent=1).encode(), f"{dest_dir}/{shard}.manifest.json", log)
    return manifest


class _BytesReader:
    """Minimal file object over bytes for tarfile.addfile (avoids a BytesIO copy)."""

    def __init__(self, data: bytes):
        self.mv, self.pos = memoryview(data), 0

    def read(self, n: int = -1) -> bytes:
        end = len(self.mv) if n < 0 else min(len(self.mv), self.pos + n)
        out = self.mv[self.pos:end]
        self.pos = end
        return out
