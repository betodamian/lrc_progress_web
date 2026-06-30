#!/usr/bin/env python3
"""
jobmon — live web dashboard for your SLURM jobs on Lawrencium (lrc).

Runs locally (no GPU needed), SSHes to the cluster on a timer, and serves an
auto-refreshing page at http://127.0.0.1:8765 showing, for every job in your
squeue: state, node/partition/GRES, elapsed-vs-walltime, parsed progress
(folds done, failure counts, "done" marker), and a live tail of the job's log.
Recently finished jobs (last 12h, via sacct) are listed too.

Usage:
    python3 tools/jobmon.py                 # defaults: target=lrc port=8765 interval=8s
    python3 tools/jobmon.py --port 9000 --target lrc --interval 10

Stop with Ctrl-C. Stdlib only; no pip installs. Binds to 127.0.0.1 only.
"""
import argparse
import http.server
import json
import socketserver
import subprocess
import threading
import time

# ---- remote collector: runs on the login node via `ssh <target> python3 -` ----
REMOTE_PY = r'''
import json, subprocess, os, re

def run(cmd):
    # NOTE: the login node ships Python 3.6 -> avoid 3.7+ kwargs (capture_output, text).
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                           universal_newlines=True, timeout=20)
        return p.stdout or ""
    except Exception:
        return ""

user = (run(["whoami"]).strip() or os.environ.get("USER", ""))
fmt = "%i|%j|%T|%M|%l|%R|%P|%S|%D"
out = run(["squeue", "-u", user, "-h", "-o", fmt])
jobs = []
for line in out.splitlines():
    p = line.split("|")
    if len(p) < 9:
        continue
    jid, name, state, elapsed, limit, reason, part, start, nnodes = p[:9]
    sc = run(["scontrol", "show", "job", jid])
    def grab(key, s=sc):
        m = re.search(r"(?:^|\s)" + key + r"=(\S+)", s)
        return m.group(1) if m else ""
    stdout = grab("StdOut")
    info = {
        "id": jid, "name": name, "state": state, "elapsed": elapsed,
        "limit": limit, "reason": reason, "partition": part, "start": start,
        "nnodes": nnodes, "nodelist": grab("NodeList"), "workdir": grab("WorkDir"),
        "command": grab("Command"), "numcpus": grab("NumCPUs"),
        "runtime": grab("RunTime"), "gres": grab("TresPerNode") or grab("Gres"),
        "stdout": stdout,
    }
    tail, nlines, parsed = "", 0, {}
    if stdout and os.path.exists(stdout):
        try:
            sz = os.path.getsize(stdout)
            with open(stdout, errors="replace") as f:
                if sz > 1_000_000:
                    f.seek(sz - 1_000_000); f.readline()
                data = f.read()
            lines = data.splitlines()
            nlines = len(lines)
            tail = "\n".join(lines[-80:])
            folded = re.findall(r"Folded (\d+)/(\d+)", data)
            groups = re.findall(r"\[([\w:.+\-]+)\][^\n]*seqs -> (\d+) ORFs", data)
            parsed = {
                "folded": [int(folded[-1][0]), int(folded[-1][1])] if folded else None,
                "groups_started": len(groups),
                "esmfold_failed": len(re.findall(r"ESMFold failed", data)),
                "numpy_fail": len(re.findall(r"Numpy is not available", data)),
                "tracebacks": len(re.findall(r"Traceback \(most recent", data)),
                "oom": len(re.findall(r"out of memory", data, re.I)),
                "cuda_err": len(re.findall(r"CUDA error", data)),
                "done": bool(re.search(r"Wrote .*groups\)", data)),
            }
        except Exception as e:
            tail = "(could not read log: %s)" % e
    info["logtail"], info["loglines"], info["parsed"] = tail, nlines, parsed
    jobs.append(info)

rec = run(["sacct", "-u", user, "--starttime", "now-12hours", "-X", "-n", "-p",
           "--format=JobID,JobName,State,Elapsed,End"])
recent = []
for line in rec.splitlines():
    c = line.split("|")
    if len(c) >= 5 and c[0]:
        recent.append({"id": c[0], "name": c[1], "state": c[2], "elapsed": c[3], "end": c[4]})
print(json.dumps({"user": user, "jobs": jobs, "recent": recent}))
'''

# ---- shared state, refreshed by a background poller ----
_lock = threading.Lock()
_last_good = {"user": "", "jobs": [], "recent": []}
_state = {"ok": False, "error": "starting…", "ts": 0.0, "data": _last_good}

# Keep these minimal: rely on the user's ~/.ssh/config for `lrc` (GSSAPI auth +
# its own connection multiplexing). Overriding ControlPath breaks GSSAPI here.
SSH_FLAGS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]


def poll_once(target):
    try:
        p = subprocess.run(
            ["ssh", *SSH_FLAGS, target, "python3 -"],
            input=REMOTE_PY, capture_output=True, text=True, timeout=45,
        )
        if p.returncode != 0:
            return {"ok": False, "error": (p.stderr.strip() or f"ssh exit {p.returncode}")}
        return {"ok": True, "data": json.loads(p.stdout)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def refresh(target):
    global _state, _last_good
    r = poll_once(target)
    with _lock:
        if r["ok"]:
            _last_good = r["data"]
        _state = {"ok": r["ok"], "error": r.get("error", ""), "ts": time.time(), "data": _last_good}


def poller(target, interval):
    while True:
        refresh(target)
        time.sleep(interval)


HTML_PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>LRC Job Monitor</title>
<style>
:root{
  --bg:#0d1117; --panel:#161b22; --panel2:#0b0f14; --border:#30363d; --border2:#21262d;
  --fg:#e6edf3; --muted:#8b949e; --accent:#58a6ff;
  --green:#2ea043; --amber:#bb8009; --blue:#1f6feb; --red:#da3633; --gray:#6e7681;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
  font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
header{position:sticky;top:0;z-index:5;display:flex;align-items:center;gap:14px;flex-wrap:wrap;
  padding:12px 18px;background:var(--panel);border-bottom:1px solid var(--border)}
header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.2px}
.dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:6px;vertical-align:middle}
.muted{color:var(--muted)}
.spacer{flex:1}
button{background:var(--panel2);color:var(--fg);border:1px solid var(--border);border-radius:6px;
  padding:5px 10px;cursor:pointer;font-size:13px}
button:hover{border-color:var(--accent)}
.wrap{max-width:1100px;margin:18px auto;padding:0 18px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:10px;margin-bottom:16px;overflow:hidden}
.card .top{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px 14px;border-bottom:1px solid var(--border2)}
.badge{font-size:11px;font-weight:700;letter-spacing:.4px;padding:3px 8px;border-radius:20px;color:#fff;white-space:nowrap}
.jname{font-weight:650;font-size:15px}
.jid{color:var(--muted);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px 16px;padding:12px 14px}
.kv{display:flex;flex-direction:column;gap:1px;min-width:0}
.kv .k{font-size:10.5px;text-transform:uppercase;letter-spacing:.5px;color:var(--muted)}
.kv .v{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bars{padding:0 14px 6px}
.bar{height:8px;border-radius:5px;background:var(--panel2);overflow:hidden;border:1px solid var(--border2)}
.bar > i{display:block;height:100%;background:var(--blue)}
.bar.time > i{background:linear-gradient(90deg,#2ea043,#bb8009)}
.barlabel{display:flex;justify-content:space-between;font-size:11.5px;color:var(--muted);margin:8px 0 3px}
.chips{display:flex;gap:6px;flex-wrap:wrap;padding:4px 14px 12px}
.chip{font-size:11.5px;padding:3px 9px;border-radius:20px;border:1px solid var(--border);background:var(--panel2)}
.chip.ok{color:#3fb950;border-color:#1f6f2f}
.chip.bad{color:#ff7b72;border-color:#7a2620;background:#2a1413}
.chip.done{color:#fff;background:var(--green);border-color:var(--green)}
.logwrap{border-top:1px solid var(--border2)}
.logbar{display:flex;align-items:center;gap:10px;padding:7px 14px;color:var(--muted);font-size:12px}
pre.log{margin:0;max-height:300px;overflow:auto;background:var(--panel2);
  padding:10px 14px;font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;
  white-space:pre;color:#c9d1d9;border-top:1px solid var(--border2)}
.empty{text-align:center;color:var(--muted);padding:50px 0}
.recent{font-size:13px}
.recent table{width:100%;border-collapse:collapse}
.recent td{padding:6px 8px;border-bottom:1px solid var(--border2);font-family:ui-monospace,Menlo,monospace;font-size:12px}
.sec{font-size:12px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);margin:6px 0 10px}
.err{background:#2a1413;border:1px solid #7a2620;color:#ff7b72;padding:8px 12px;border-radius:8px;margin-bottom:14px;font-size:13px}
</style></head>
<body>
<header>
  <h1>LRC Job Monitor</h1>
  <span class="muted" id="user"></span>
  <span class="spacer"></span>
  <span class="muted"><span class="dot" id="dot" style="background:var(--gray)"></span><span id="conn">connecting…</span></span>
  <span class="muted">updated <span id="updated">—</span></span>
  <button id="pause">⏸ Pause</button>
  <button id="now">↻ Refresh</button>
</header>
<div class="wrap">
  <div id="errbox"></div>
  <div id="jobs"></div>
  <div id="recentbox"></div>
</div>
<script>
const PAGE_POLL = 3000;
let paused = false;

function esc(s){return (s==null?"":String(s)).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));}
function agoStr(ms){if(!ms)return "—";const d=(Date.now()-ms)/1000;
  if(d<2)return"just now";if(d<60)return Math.floor(d)+"s ago";
  if(d<3600)return Math.floor(d/60)+"m ago";return Math.floor(d/3600)+"h ago";}
function parseDur(s){if(!s||/UNLIMITED|N\/A|INVALID|NONE/i.test(s))return null;
  let days=0,rest=s;if(s.includes("-")){const[d,r]=s.split("-");days=+d;rest=r;}
  const ps=rest.split(":").map(Number);if(ps.some(isNaN))return null;
  let sec=0;for(const p of ps)sec=sec*60+p;return days*86400+sec;}
function stateInfo(st){const s=(st||"").toUpperCase();
  if(s.startsWith("RUN"))return["RUNNING","var(--green)"];
  if(s.startsWith("PEND"))return["PENDING","var(--amber)"];
  if(s.startsWith("COMPLETI")||s==="CG")return["COMPLETING","var(--blue)"];
  if(s.startsWith("COMPLETED")||s==="CD")return["COMPLETED","var(--gray)"];
  if(s.startsWith("FAIL")||s.startsWith("CANCEL")||s.startsWith("TIMEOUT")||s.startsWith("OUT_OF"))return[s,"var(--red)"];
  return[s||"?","var(--gray)"];}

function kv(k,v){return v?`<div class="kv"><span class="k">${esc(k)}</span><span class="v" title="${esc(v)}">${esc(v)}</span></div>`:"";}

function jobCard(j){
  const[lbl,col]=stateInfo(j.state);
  const p=j.parsed||{};
  // time bar
  let timeBar="";
  const el=parseDur(j.elapsed), lim=parseDur(j.limit);
  if(el!=null&&lim&&lim>0){const pct=Math.min(100,100*el/lim);
    timeBar=`<div class="barlabel"><span>walltime</span><span>${esc(j.elapsed)} / ${esc(j.limit)} (${pct.toFixed(0)}%)</span></div>
      <div class="bar time"><i style="width:${pct}%"></i></div>`;}
  // fold progress bar
  let foldBar="";
  if(p.folded){const[a,b]=p.folded;const pct=b>0?100*a/b:0;
    foldBar=`<div class="barlabel"><span>current-group folds</span><span>${a} / ${b} (${pct.toFixed(0)}%)</span></div>
      <div class="bar"><i style="width:${pct}%"></i></div>`;}
  // chips
  let chips=[];
  if(p.groups_started)chips.push(`<span class="chip">groups: ${p.groups_started}</span>`);
  const fails=[["ESMFold fail",p.esmfold_failed],["numpy fail",p.numpy_fail],
    ["traceback",p.tracebacks],["OOM",p.oom],["CUDA err",p.cuda_err]];
  for(const[n,c] of fails){if(c==null)continue;
    chips.push(`<span class="chip ${c>0?"bad":"ok"}">${n}: ${c}</span>`);}
  if(p.done)chips.push(`<span class="chip done">✓ wrote output</span>`);
  const bars=(timeBar||foldBar)?`<div class="bars">${timeBar}${foldBar}</div>`:"";
  const chipsHtml=chips.length?`<div class="chips">${chips.join("")}</div>`:"";
  const log=j.logtail?`<div class="logwrap">
      <div class="logbar">log tail · ${j.loglines||0} lines · <span title="${esc(j.stdout)}">${esc((j.stdout||"").split("/").pop())}</span></div>
      <pre class="log" data-jobid="${esc(j.id)}">${esc(j.logtail)}</pre></div>`:"";
  return `<div class="card">
    <div class="top">
      <span class="badge" style="background:${col}">${esc(lbl)}</span>
      <span class="jname">${esc(j.name)}</span>
      <span class="jid">#${esc(j.id)}</span>
      <span class="spacer"></span>
      <span class="jid">${esc(j.nodelist||j.reason||"")}</span>
    </div>
    <div class="grid">
      ${kv("Partition",j.partition)}${kv("Node",j.nodelist)}${kv("GRES",j.gres)}
      ${kv("CPUs",j.numcpus)}${kv("Runtime",j.runtime||j.elapsed)}${kv("Time limit",j.limit)}
      ${kv("Started",j.start)}${kv("Work dir",j.workdir)}
    </div>
    ${bars}${chipsHtml}${log}
  </div>`;
}

function recentTable(rec){
  if(!rec||!rec.length)return "";
  const rows=rec.slice(0,12).map(r=>{const[lbl,col]=stateInfo(r.state);
    return `<tr><td>#${esc(r.id)}</td><td>${esc(r.name)}</td>
      <td><span class="badge" style="background:${col};font-size:10px">${esc(lbl)}</span></td>
      <td>${esc(r.elapsed)}</td><td>${esc(r.end)}</td></tr>`;}).join("");
  return `<div class="card recent"><div class="top"><span class="sec" style="margin:0">Recently finished (12h)</span></div>
    <table>${rows}</table></div>`;}

async function tick(){
  if(paused)return;
  try{
    const r=await fetch("/api/jobs",{cache:"no-store"});
    const s=await r.json();
    render(s);
  }catch(e){
    document.getElementById("dot").style.background="var(--red)";
    document.getElementById("conn").textContent="server unreachable";
  }
}

function render(s){
  document.getElementById("user").textContent=s.data.user?("· "+s.data.user+" @ lrc"):"";
  document.getElementById("dot").style.background=s.ok?"var(--green)":"var(--red)";
  document.getElementById("conn").textContent=s.ok?"connected":"ssh error";
  document.getElementById("updated").textContent=agoStr(s.ts*1000);
  document.getElementById("errbox").innerHTML=
    (!s.ok&&s.error)?`<div class="err">SSH poll failed: ${esc(s.error)} <span class="muted">(showing last known data)</span></div>`:"";

  const jobs=s.data.jobs||[];
  // preserve log scroll positions
  const sm={};
  document.querySelectorAll("pre.log").forEach(pre=>{
    sm[pre.dataset.jobid]={top:pre.scrollTop,
      atBottom:(pre.scrollHeight-pre.clientHeight-pre.scrollTop)<24};});
  document.getElementById("jobs").innerHTML = jobs.length
    ? jobs.map(jobCard).join("")
    : `<div class="empty">No active jobs in your squeue.</div>`;
  document.querySelectorAll("pre.log").forEach(pre=>{
    const st=sm[pre.dataset.jobid];
    pre.scrollTop = st ? (st.atBottom?pre.scrollHeight:st.top) : pre.scrollHeight;});
  document.getElementById("recentbox").innerHTML = recentTable(s.data.recent);
}

document.getElementById("pause").onclick=function(){
  paused=!paused;this.textContent=paused?"▶ Resume":"⏸ Pause";};
document.getElementById("now").onclick=tick;
tick();
setInterval(tick,PAGE_POLL);
setInterval(()=>{const u=document.getElementById("updated");},1000);
</script>
</body></html>"""


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/jobs"):
            with _lock:
                payload = json.dumps(_state).encode()
            self._send(200, payload, "application/json")
        elif self.path == "/" or self.path.startswith("/index"):
            self._send(200, HTML_PAGE.encode(), "text/html; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain")


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="lrc", help="ssh host alias (default: lrc)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--interval", type=int, default=8, help="seconds between SSH polls")
    args = ap.parse_args()

    print(f"[jobmon] priming first poll to {args.target} …", flush=True)
    refresh(args.target)  # synchronous first poll so the page has data immediately
    threading.Thread(target=poller, args=(args.target, args.interval), daemon=True).start()

    srv = Server(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}"
    print(f"[jobmon] serving {url}  (target={args.target}, every {args.interval}s)  — Ctrl-C to stop", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[jobmon] bye")


if __name__ == "__main__":
    main()
