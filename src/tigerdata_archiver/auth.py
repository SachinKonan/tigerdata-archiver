"""Access tokens for a collection's HTTPS server, via the Globus CLI's own login.

The Globus CLI stores tokens in ~/.globus/cli/storage.db and refreshes them with its
registered client. We reuse its authorizer instead of handling client credentials
ourselves; refreshed tokens are written back to that store.
"""
from __future__ import annotations

import sys
import threading
import time


def https_scope(collection_id: str) -> str:
    return f"https://auth.globus.org/scopes/{collection_id}/https"


def data_access_scope(collection_id: str) -> str:
    return f"https://auth.globus.org/scopes/{collection_id}/data_access"


class Token:
    """Thread-safe `Authorization:` header for a collection, refreshed as needed."""

    def __init__(self, collection_id: str):
        from globus_cli.login_manager import LoginManager

        lm = LoginManager()
        if not lm.has_login(collection_id):
            sys.exit(f"no Globus login for collection {collection_id}; run `tda login` for setup steps")
        self._auth = lm._get_client_authorizer(collection_id)
        self._lock = threading.Lock()

    def header(self) -> str:
        with self._lock:
            for i in range(5):
                try:
                    return f"Authorization: {self._auth.get_authorization_header()}"
                except Exception as e:  # e.g. sqlite 'database is locked' when many jobs refresh at once
                    print(f"  token refresh attempt {i + 1} failed: {type(e).__name__}: {str(e)[:150]}",
                          flush=True)
                    time.sleep(30 * (i + 1))
        sys.exit("could not obtain a Globus token")


def token_scopes(collection_id: str) -> tuple[str, float] | None:
    """(scope string, seconds until expiry) of the stored token, or None if absent."""
    from globus_cli.login_manager import LoginManager

    data = LoginManager().storage.adapter.get_token_data(collection_id)
    if not data:
        return None
    return data.get("scope", ""), data["expires_at_seconds"] - time.time()
