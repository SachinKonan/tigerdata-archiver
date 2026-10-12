"""Minimal HTTPS client for a Globus collection, using curl.

curl is used rather than a Python HTTP library because it uploads a 1 GB file with a
known Content-Length (the server rejects chunked uploads with 411) without buffering
it in memory. The bearer token is passed through a 0600 header file, never argv.
"""
from __future__ import annotations

import os
import subprocess
import tempfile
import time
import urllib.parse

from .auth import Token


class Remote:
    def __init__(self, https_server: str, token: Token, tries: int = 14):
        self.base = https_server.rstrip("/")
        self.token = token
        self.tries = tries
        self._hdr_dir = tempfile.mkdtemp(prefix="tda_hdr_")  # mkdtemp is 0700

    def url(self, path: str) -> str:
        return self.base + urllib.parse.quote("/" + path.strip("/"))

    def _curl(self, *args: str) -> subprocess.CompletedProcess:
        fd, hdr = tempfile.mkstemp(dir=self._hdr_dir)
        try:
            os.write(fd, self.token.header().encode())
            os.close(fd)
            return subprocess.run(["curl", "-sS", "-H", f"@{hdr}", *args], capture_output=True, text=True)
        finally:
            os.unlink(hdr)

    def mkdirs(self, collection_id: str, path: str) -> None:
        """Create path and its parents in the collection (HTTPS PUT does not create them).

        Uses the Globus Transfer API; existing directories are fine.
        """
        import globus_sdk
        from globus_cli.login_manager import LoginManager

        tc = LoginManager().get_transfer_client()
        parts = [p for p in path.strip("/").split("/") if p]
        for i in range(1, len(parts) + 1):
            d = "/" + "/".join(parts[:i])
            for attempt in range(5):
                try:
                    tc.operation_mkdir(collection_id, path=d)
                    break
                except globus_sdk.TransferAPIError as e:
                    if e.code == "ExternalError.MkdirFailed.Exists" or "Exists" in str(e.code):
                        break
                    if attempt == 4:
                        raise
                    time.sleep(10 * (attempt + 1))

    def head(self, path: str) -> int | None:
        """Size in bytes of a remote file, or None if it does not exist."""
        r = self._curl("-I", self.url(path))
        lines = r.stdout.splitlines()
        if not lines or " 200" not in lines[0]:
            return None
        for line in lines:
            k, _, v = line.partition(":")
            if k.strip().lower() == "content-length":
                return int(v.strip())
        return None

    def exists(self, path: str) -> bool:
        return self.head(path) is not None

    def delete(self, path: str) -> str:
        r = self._curl("-o", "/dev/null", "-w", "%{http_code}", "-X", "DELETE", self.url(path))
        return r.stdout.strip()

    def put(self, local: str, path: str, log=print) -> None:
        """Upload a file; on failure delete the remote copy and retry with backoff.

        A failed write can leave the remote file in a state where every later PUT to it
        fails (the server answers 404 with "globus_xio: System error in close:
        Input/output error"); deleting it first clears that.
        """
        url = self.url(path)
        for i in range(self.tries):
            r = self._curl("--fail-with-body", "-o", "/dev/null", "-w", "%{http_code}", "-T", local, url)
            if r.returncode == 0 and r.stdout.strip() in ("200", "201", "204"):
                return
            log(f"  upload attempt {i + 1} of {path} failed: http={r.stdout.strip()} {r.stderr.strip()[:200]}")
            log(f"  deleted remote file before retry: http={self.delete(path)}")
            time.sleep(min(300, 15 * 2 ** i))
        raise RuntimeError(f"giving up on {path} after {self.tries} attempts")

    def put_bytes(self, data: bytes, path: str, log=print) -> None:
        fd, tmp = tempfile.mkstemp(dir=self._hdr_dir)
        try:
            os.write(fd, data)
            os.close(fd)
            self.put(tmp, path, log)
        finally:
            os.unlink(tmp)

    def get(self, path: str, local: str) -> None:
        for i in range(self.tries):
            r = self._curl("--fail", "-o", local, self.url(path))
            if r.returncode == 0:
                return
            time.sleep(min(300, 15 * 2 ** i))
        raise RuntimeError(f"could not download {path}: {r.stderr.strip()[:200]}")

    def get_text(self, path: str) -> str | None:
        r = self._curl("--fail", self.url(path))
        return r.stdout if r.returncode == 0 else None
