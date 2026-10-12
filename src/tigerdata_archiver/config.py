"""Settings from the environment / .env, with defaults."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from dotenv import load_dotenv

PART_SIZE_CAP = 1 << 30  # TigerData's HTTPS endpoint rejects single uploads above ~1 GiB (HTTP 413)
DEFAULT_PART_SIZE = 1000 << 20


@dataclass
class Config:
    collection_id: str
    https_server: str
    staging_dir: str
    state_dir: str
    slurm_partition: str = ""
    slurm_account: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=1)

    @classmethod
    def from_json(cls, text: str) -> "Config":
        return cls(**json.loads(text))


def _default_staging() -> str:
    scratch = Path("/scratch") / os.environ.get("USER", "")
    if scratch.is_dir() and os.access(scratch, os.W_OK):
        return str(scratch)
    return os.environ.get("TMPDIR", "/tmp")


def load(env_file: str | None = None, resolve_https: bool = True) -> Config:
    """Read settings from .env (cwd or the repo root) and the environment."""
    load_dotenv(env_file or Path.cwd() / ".env", override=False)
    load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
    coll = os.environ.get("GLOBUS_COLLECTION_ID", "").strip()
    if not coll:
        raise SystemExit("GLOBUS_COLLECTION_ID is not set (see .env.example)")
    state = os.environ.get("TDA_STATE_DIR", "").strip() or str(
        Path.home() / ".local" / "state" / "tigerdata-archiver")
    https = os.environ.get("GLOBUS_HTTPS_SERVER", "").strip().rstrip("/")
    if not https and resolve_https:
        https = resolve_https_server(coll, state)
    return Config(
        collection_id=coll,
        https_server=https,
        staging_dir=os.environ.get("TDA_STAGING_DIR", "").strip() or _default_staging(),
        state_dir=state,
        slurm_partition=os.environ.get("SLURM_PARTITION", "").strip(),
        slurm_account=os.environ.get("SLURM_ACCOUNT", "").strip(),
    )


def resolve_https_server(collection_id: str, state_dir: str) -> str:
    """Look up the collection's HTTPS server via the Transfer API (cached per collection)."""
    cache = Path(state_dir) / "https_server.json"
    known = json.loads(cache.read_text()) if cache.exists() else {}
    if collection_id in known:
        return known[collection_id]
    from globus_cli.login_manager import LoginManager

    ep = LoginManager().get_transfer_client().get_endpoint(collection_id)
    server = (ep.get("https_server") or "").rstrip("/")
    if not server:
        raise SystemExit(f"collection {collection_id} has no HTTPS server; set GLOBUS_HTTPS_SERVER")
    cache.parent.mkdir(parents=True, exist_ok=True)
    known[collection_id] = server
    cache.write_text(json.dumps(known, indent=1))
    return server


def parse_size(text: str) -> int:
    """'1000MiB', '1GiB', '500MB', '2G' -> bytes."""
    t = text.strip().upper().replace(" ", "")
    units = {"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40,
             "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12,
             "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40, "B": 1}
    for u in sorted(units, key=len, reverse=True):
        if t.endswith(u):
            return int(float(t[: -len(u)]) * units[u])
    return int(t)
