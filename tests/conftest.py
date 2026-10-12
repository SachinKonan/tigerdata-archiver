"""A local stand-in for a Globus collection's HTTPS server.

Supports PUT (requires Content-Length, like TigerData), HEAD, GET and DELETE on files
under a temp directory, and can simulate a "stuck" file that rejects writes until it
is deleted (the TigerData `globus_xio ... Input/output error` behaviour).
"""
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import pytest


class FakeToken:
    def header(self):
        return "Authorization: Bearer test"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _path(self):
        return os.path.join(self.server.root, unquote(self.path).lstrip("/"))

    def do_PUT(self):
        self.server.puts.append(unquote(self.path))
        if "Content-Length" not in self.headers:
            self.send_response(411); self.end_headers(); return
        n = int(self.headers["Content-Length"])
        data = self.rfile.read(n)
        if unquote(self.path) in self.server.stuck:
            self.send_response(404); self.end_headers()
            self.wfile.write(b"globus_xio: System error in close: Input/output error")
            return
        p = self._path()
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(data)
        self.send_response(201); self.send_header("Content-Length", "0"); self.end_headers()

    def do_HEAD(self):
        p = self._path()
        if not os.path.isfile(p):
            self.send_response(404); self.end_headers(); return
        self.send_response(200); self.send_header("Content-Length", str(os.path.getsize(p))); self.end_headers()

    def do_GET(self):
        p = self._path()
        if not os.path.isfile(p):
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        data = open(p, "rb").read()
        self.send_response(200); self.send_header("Content-Length", str(len(data))); self.end_headers()
        self.wfile.write(data)

    def do_DELETE(self):
        self.server.stuck.discard(unquote(self.path))
        p = self._path()
        if os.path.isfile(p):
            os.unlink(p)
            self.send_response(204)
        else:
            self.send_response(404)
        self.send_header("Content-Length", "0"); self.end_headers()


@pytest.fixture
def server(tmp_path):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.root = str(tmp_path / "remote")
    srv.puts, srv.stuck = [], set()
    os.makedirs(srv.root)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    srv.url = f"http://127.0.0.1:{srv.server_address[1]}"
    yield srv
    srv.shutdown()


@pytest.fixture
def remote(server, monkeypatch):
    import tigerdata_archiver.http as h

    monkeypatch.setattr(h.time, "sleep", lambda s: None)
    return h.Remote(server.url, FakeToken(), tries=3)
