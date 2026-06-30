# jobmon

Live web dashboard for SLURM jobs running on Lawrencium (LRC) or any SSH-accessible cluster.

Runs entirely on your local machine, SSHes into the cluster on a configurable interval, and serves an auto-refreshing page showing every active job's state, resource usage, elapsed/walltime progress bars, parsed log tail, and recently finished jobs.

![jobmon dashboard screenshot](screenshot.png)

## Features

- Active jobs from `squeue`: state badge, partition, node, GRES, CPUs, start time, work dir
- Walltime progress bar and per-job fold progress bar (parsed from log output)
- Log tail (last 80 lines) with live scroll-to-bottom behavior
- Error chip summary: ESMFold failures, OOM events, CUDA errors, tracebacks, numpy issues
- Recently finished jobs (last 12 h via `sacct`)
- Pause / manual-refresh controls
- Zero dependencies: stdlib only, no pip installs required
- Binds to `127.0.0.1` only (never exposed to the network)

## Requirements

- Python 3.8+ locally
- SSH access to the cluster configured as a named host in `~/.ssh/config` (default alias: `lrc`)
- `BatchMode yes` and a working auth method (GSSAPI / SSH key) so the connection is non-interactive
- Python 3.6+ on the login node (the remote snippet avoids 3.7+ kwargs for compatibility)

## Setup

Configure your cluster alias in `~/.ssh/config` if you have not already:

```
Host lrc
    HostName lrc.lbl.gov
    User your_username
    GSSAPIAuthentication yes
    GSSAPIDelegateCredentials yes
```

Then run:

```bash
python3 jobmon.py
```

Open `http://127.0.0.1:8765` in your browser. The page polls itself every 3 seconds; the server polls the cluster every 8 seconds.

## Options

```
python3 jobmon.py --target lrc --port 8765 --interval 8
```

| Flag | Default | Description |
|------|---------|-------------|
| `--target` | `lrc` | SSH host alias from `~/.ssh/config` |
| `--port` | `8765` | Local port to serve the dashboard on |
| `--interval` | `8` | Seconds between SSH polls to the cluster |

Stop with `Ctrl-C`.

## Log parsing

The dashboard parses job stdout logs for the following patterns (adjust in `REMOTE_PY` if your jobs emit different output):

| Chip | Pattern matched |
|------|----------------|
| Fold progress bar | `Folded N/M` |
| Groups started | `[group] ... seqs -> N ORFs` |
| ESMFold fail | `ESMFold failed` |
| NumPy fail | `Numpy is not available` |
| Tracebacks | `Traceback (most recent` |
| OOM | `out of memory` (case-insensitive) |
| CUDA error | `CUDA error` |
| Done | `Wrote ...groups)` |
