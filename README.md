# tigerdata-archiver (`tda`)

Move directories from a Slurm cluster's shared storage to a Princeton **TigerData** project
over Globus HTTPS — chunked, uploaded in parallel, verified, and then deleted locally —
plus a storage report to find what to move.

Built from the tooling used to move 8.6 TB off the CS `neuronic` cluster when the
cluster cannot run a Globus endpoint of its own: everything goes over the TigerData
collection's HTTPS interface from ordinary Slurm jobs.

```
uv run tda report /n/fs/mylab/me --depth 3 --by-ext       # where is the space?
uv run tda move /n/fs/mylab/me/big_dataset --dest /me/archive
uv run tda status <run> --watch                            # progress bars
```

## How it works

- **Login node: plan and submit only.** `tda move` submits a planning job and exits. The
  planning job (Slurm) walks the inputs once, splits them into **shards**, and submits the
  shards as a job array (via [submitit](https://github.com/facebookincubator/submitit)).
- **Shard = one tar archive = one Slurm task.** Planning packs directories top-down: a
  directory whose whole subtree fits the budget (`--shard-size`, default 200 GB, and
  `--shard-files`, default 500k entries) becomes one member; larger ones are opened and their
  children packed instead, so huge flat directories are split file by file.
- **Parts.** Each shard's tar stream is cut into **parts** (`--part-size`, default 1000 MiB).
  TigerData's HTTPS endpoint rejects single uploads above ~1 GiB, so that is the hard cap.
  Restore is `cat <shard>.tar.* | tar -xf -`.
- **Inside each task:** a thread pool reads files concurrently (network filesystems are
  latency-bound on small files), one writer builds the tar in a fixed order, and a second
  pool uploads parts while the next ones are being written.
- **Then verify, then delete.** After a shard's manifest is uploaded, every part is checked
  on TigerData (exact size), and only then is the local copy deleted — exactly the shard's
  members, and only if nothing was added locally since it was archived. Shards that had
  files skipped or changed while archiving are not deleted.

### What lands on TigerData

```
<dest>/<input dir name>/<shard>.tar.00000 …   the archive, in parts
<dest>/<input dir name>/<shard>.filelist.txt  path<TAB>shard<TAB>size<TAB>mtime per file
<dest>/<input dir name>/<shard>.manifest.json parts with sha256, counts, warnings
<dest>/_tda/<run id>/index.txt                every file of the run (written when the run completes)
<dest>/_tda/<run id>/plan.json
```

The manifest is uploaded last; its presence means the archive is complete. Destination
directories are created automatically (through the Globus Transfer API, since HTTPS
uploads cannot create directories).

## Setup

### 1. Install

```bash
git clone <this repo> && cd tigerdata-archiver
uv sync
cp .env.example .env    # then fill in GLOBUS_COLLECTION_ID
```

The repo must live on storage that compute nodes can see (jobs run its `.venv`).

### 2. Globus login and consents (once per account)

These commands print a URL; open it, log in with your Princeton account, and paste the code
back. Run them in a normal interactive terminal.

1. **Log in to the Globus CLI**
   ```bash
   uv run globus login
   ```
2. **Find your TigerData collection** and put its UUID in `.env` as `GLOBUS_COLLECTION_ID`:
   ```bash
   uv run globus endpoint search "TigerData"
   ```
   It is named like `Princeton TigerData - <pi>/<project>`.
3. **Consent to data access on the collection** (needed to list/manage it):
   ```bash
   uv run globus session consent \
     'urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/<COLLECTION_ID>/data_access]'
   ```
4. **Consent to HTTPS uploads.** The uploader needs a token for the collection that carries
   both `https://auth.globus.org/scopes/<COLLECTION_ID>/https` and
   `.../<COLLECTION_ID>/data_access`:
   ```bash
   uv run globus login --gcs <COLLECTION_ID> --force
   ```
5. **Check everything** (optionally with a write test into a directory you own):
   ```bash
   uv run tda login --probe /<your dir on the collection>
   ```
   `tda login` prints the exact commands above, filled in, if anything is missing.

Tokens are stored by the Globus CLI in `~/.globus/cli/storage.db` and refreshed
automatically (jobs refresh them through the CLI's own login code; no secrets are copied
into this repo). `.env` holds only identifiers, never tokens, and is git-ignored.

### `.env`

| Variable | Meaning |
|---|---|
| `GLOBUS_COLLECTION_ID` | your TigerData collection UUID (required) |
| `GLOBUS_HTTPS_SERVER` | its HTTPS server; looked up automatically if blank |
| `TDA_STAGING_DIR` | where parts are staged (default `/scratch/$USER`, else `$TMPDIR`) |
| `TDA_STATE_DIR` | runs, plans, indexes, logs (default `~/.local/state/tigerdata-archiver`) |
| `SLURM_PARTITION`, `SLURM_ACCOUNT` | optional, for submitted jobs |

## Usage

### Storage report

```bash
uv run tda report /n/fs/mylab/me --depth 3 --by-ext --out ~/report.md
```

Runs as a Slurm job (submits and exits; `--wait` to block, `--local` for small trees).
Lists files and bytes per directory at `--depth`, largest first; `--by-ext` adds the top
extensions per directory and an overall by-extension table.

### Move

```bash
uv run tda move DIR [DIR ...] --dest /me/archive [--parallel 8] [--dry-run]
```

| Option | Default | |
|---|---|---|
| `--part-size` | `1000MiB` | upload unit, max 1 GiB |
| `--shard-size` / `--shard-files` | `200GB` / `500000` | max bytes / entries per archive |
| `--parallel` | 8 | archives in flight (Slurm array throttle) |
| `--read-threads` | 8 | concurrent file reads per archive |
| `--upload-threads` | 2 | concurrent part uploads per archive |
| `--max-readers` | 32 | cap on parallel × read-threads (load on shared storage) |
| `--dry-run` | | plan only; inspect with `tda status RUN` |

**Local data is deleted after each shard verifies.** Use `--dry-run` first to check the plan.

### Progress

```bash
uv run tda status RUN --watch
```

One overall bar (bytes uploaded / planned, rate, ETA), one bar per input directory, and a
line of shard counts by state. It only reads small state files, so it is cheap; Ctrl-C does
not affect the jobs. `uv run tda runs` lists runs.

### Failures and resume

Each shard moves through `planned → uploading → uploaded → verified → deleted`, recorded in
the run directory. If jobs die (time limit, node failure, `scancel`, network outage):

```bash
uv run tda resume RUN
```

It skips shards that are done or still queued/running and resubmits the rest. A resubmitted
shard reconciles against TigerData first:

| On TigerData | Local copy | What happens |
|---|---|---|
| manifest present | present | verify, then delete |
| manifest present | gone | verify, mark done |
| no manifest | present | archive again; parts already uploaded are skipped |
| no manifest | gone | **refuses** — never uploads over a finished archive |

Failed uploads are retried with backoff; a failed part is deleted on the server before the
retry (TigerData can otherwise keep rejecting writes to that file).

### Find and restore

```bash
uv run tda find 'images/.*\.png' [--run RUN]   # grep the index: path, shard, size, mtime
uv run tda restore SHARD --dest-dir /me/archive/<input> --to /scratch/$USER/restore
```

`restore` downloads parts one at a time, checks each sha256 and extracts as it goes; run it
inside a Slurm job for large archives. Without the tool: download `<shard>.tar.*` and run
`cat <shard>.tar.* | tar -xf -`.

## Etiquette on shared clusters

- Never run `move` workers or reports on the login node; the tool submits them to Slurm.
- `--max-readers` bounds concurrent reads on the source filesystem; keep it modest.
- Workers run at idle I/O priority (`ionice -c3`).
