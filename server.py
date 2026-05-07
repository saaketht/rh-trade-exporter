"""FastAPI dashboard server for rh-trade-exporter."""

import base64
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException, Request, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="RH Trade Dashboard")

BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs"
STATIC_DIR = BASE_DIR / "static"
TOKEN_FILE = BASE_DIR / ".server_token"
RH_TOKEN_FILE = BASE_DIR / ".rh_token"
JOBS_DIR = OUTPUTS_DIR / ".admin_jobs"
NOTES_FILE = OUTPUTS_DIR / "journal_notes.json"

# Admin job whitelist — never accept arbitrary commands.
ADMIN_SCRIPTS = {
    "hood":      [sys.executable, str(BASE_DIR / "hood.py")],
    "cash_flow": [sys.executable, str(BASE_DIR / "cash_flow.py")],
    "backfill":  [sys.executable, str(BASE_DIR / "cash_flow.py"), "--backfill"],
}

# In-memory job table. Single-worker uvicorn assumed.
_jobs_lock = threading.Lock()
_jobs: dict = {}  # job_id -> {state, script, started_at, ended_at, exit_code, log_path, proc}

# --- Auth ---

def _load_token() -> str:
    try:
        return TOKEN_FILE.read_text().strip()
    except FileNotFoundError:
        return ""

def verify_token(request: Request, token: Optional[str] = Query(None)):
    expected = _load_token()
    if not expected:
        return  # no token file = auth disabled (local dev)
    # Check query param first, then Authorization header
    if token and token == expected:
        return
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer ") and auth[7:].strip() == expected:
        return
    raise HTTPException(status_code=401, detail="Unauthorized")

# --- CSV helpers ---

# Map CSV column names to clean JSON keys
COLUMN_MAP = {
    "Trade #": "trade_num",
    "Date": "date",
    "Day": "day",
    "Account": "account",
    "Symbol": "symbol",
    "Expiry Date": "expiry_date",
    "Type": "type",
    "Strike": "strike",
    "Qty": "qty",
    "Asset Open": "open",
    "Asset High": "high",
    "Asset Low": "low",
    "Asset Close": "close",
    "VWAP": "vwap",
    "8 EMA": "ema8",
    "Entry Time": "entry_time",
    "Exit Time": "exit_time",
    "Hold Time (min)": "hold_time_min",
    "Entry Hour": "entry_hour",
    "Entry Cost": "entry_cost",
    "Risk ($)": "risk",
    "Exit Credit": "exit_credit",
    "P/L ($)": "pl",
    "Cumulative P/L ($)": "cumulative_pl",
    "P/L (%)": "pl_pct",
    "Win/Loss": "wl",
    "Is Win": "is_win",
    "VIX": "vix",
    "Delta": "delta",
    "Group ID": "group_id",
    "DTE": "dte",
}

INT_FIELDS = {"trade_num", "qty", "entry_hour", "is_win", "dte"}
FLOAT_FIELDS = {
    "strike", "open", "high", "low", "close", "vwap", "ema8",
    "hold_time_min", "entry_cost", "risk", "exit_credit",
    "pl", "cumulative_pl", "pl_pct", "vix", "delta",
}

def _normalize_date(d: str) -> str:
    """Convert M/D/YYYY to YYYY-MM-DD."""
    if not d or "-" in d:
        return d
    try:
        parts = d.split("/")
        return f"{parts[2]}-{int(parts[0]):02d}-{int(parts[1]):02d}"
    except (IndexError, ValueError):
        return d

def _convert(key: str, val: str):
    if val == "":
        return None
    if key in INT_FIELDS:
        try:
            return int(float(val))
        except ValueError:
            return val
    if key in FLOAT_FIELDS:
        try:
            return round(float(val), 2)
        except ValueError:
            return val
    return val

def _read_csv(filename: str) -> list[dict]:
    path = OUTPUTS_DIR / filename
    if not path.exists():
        return []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            out = {}
            for csv_col, val in row.items():
                key = COLUMN_MAP.get(csv_col, csv_col)
                out[key] = _convert(key, val)
            # Normalize dates
            if "date" in out:
                out["date"] = _normalize_date(out["date"])
            if "expiry_date" in out:
                out["expiry_date"] = _normalize_date(out["expiry_date"])
            rows.append(out)
        return rows

def _read_jsonl(filename: str) -> list[dict]:
    path = OUTPUTS_DIR / filename
    if not path.exists():
        return []
    items = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items

def _read_notes() -> dict:
    if not NOTES_FILE.exists():
        return {}
    return json.loads(NOTES_FILE.read_text())

def _write_notes(notes: dict):
    NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)
    NOTES_FILE.write_text(json.dumps(notes, indent=2))

# --- API endpoints ---

@app.get("/api/trades")
def get_trades(symbol: Optional[str] = None, _=Depends(verify_token)):
    spy = _read_csv("spy_trades.csv")
    other = _read_csv("other_trades.csv")
    all_trades = spy + other
    if symbol:
        sym = symbol.upper()
        all_trades = [t for t in all_trades if (t.get("symbol") or "").upper() == sym]
    return all_trades

@app.get("/api/trades/daily")
def get_daily(_=Depends(verify_token)):
    trades = _read_csv("spy_trades.csv")
    daily: dict[str, dict] = {}
    for t in trades:
        d = t["date"]
        if d not in daily:
            daily[d] = {"date": d, "pl": 0, "num_trades": 0, "wins": 0, "cumulative_pl": 0, "vix": t.get("vix")}
        daily[d]["pl"] += t["pl"] or 0
        daily[d]["num_trades"] += 1
        if t.get("is_win") == 1:
            daily[d]["wins"] += 1
        daily[d]["cumulative_pl"] = t.get("cumulative_pl") or daily[d]["cumulative_pl"]
    return sorted(daily.values(), key=lambda x: x["date"])

@app.get("/api/trades/open")
def get_open(_=Depends(verify_token)):
    return _read_csv("unmatched_opens.csv")

@app.get("/api/cash-flow")
def get_cash_flow(_=Depends(verify_token)):
    """Merge historical (synthetic, from --backfill) + live snapshots.
    Dedup by UTC date; live snapshot wins on collision (most accurate).
    """
    historical = _read_jsonl("cash_flow_historical.jsonl")
    live = _read_jsonl("cash_flow.jsonl")
    by_date: dict[str, dict] = {}
    # Insert historical first so live overrides on the same date
    for s in historical:
        ts = s.get("timestamp") or ""
        if ts:
            by_date[ts[:10]] = s
    for s in live:
        ts = s.get("timestamp") or ""
        if ts:
            by_date[ts[:10]] = s
    return sorted(by_date.values(), key=lambda x: x.get("timestamp", ""))

@app.get("/api/summary")
def get_summary(_=Depends(verify_token)):
    spy = _read_csv("spy_trades.csv")
    other = _read_csv("other_trades.csv")
    all_trades = spy + other
    if not all_trades:
        return {"total_trades": 0}

    wins = [t for t in all_trades if t.get("is_win") == 1]
    losses = [t for t in all_trades if t.get("wl") == "LOSS"]
    total_pl = sum(t.get("pl") or 0 for t in all_trades)
    win_pls = [t["pl"] for t in wins if t.get("pl")]
    loss_pls = [t["pl"] for t in losses if t.get("pl")]

    return {
        "total_trades": len(all_trades),
        "spy_trades": len(spy),
        "other_trades": len(other),
        "total_pl": round(total_pl, 2),
        "win_rate": round(len(wins) / max(len(wins) + len(losses), 1) * 100, 1),
        "avg_win": round(sum(win_pls) / max(len(win_pls), 1), 2),
        "avg_loss": round(sum(loss_pls) / max(len(loss_pls), 1), 2),
        "best_trade": max((t.get("pl") or 0 for t in all_trades), default=0),
        "worst_trade": min((t.get("pl") or 0 for t in all_trades), default=0),
        "last_updated": max((t.get("date") or "" for t in all_trades), default=""),
    }

@app.get("/api/notes")
def get_notes(_=Depends(verify_token)):
    return _read_notes()

@app.post("/api/notes")
async def save_note(request: Request, _=Depends(verify_token)):
    body = await request.json()
    group_id = body.get("group_id")
    note = body.get("note", "")
    if not group_id:
        raise HTTPException(status_code=400, detail="group_id required")
    notes = _read_notes()
    if note:
        notes[group_id] = note
    else:
        notes.pop(group_id, None)
    _write_notes(notes)
    return {"ok": True}

# --- Admin: token status, token update, manual fetch runs ---

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _decode_jwt_unverified(token: str) -> dict:
    """Decode the JWT payload WITHOUT verifying the signature. We don't need
    cryptographic verification — we're just reading our own copy of an RH-issued
    token to surface the expiry. The authority on validity is RH's API itself,
    via /user/."""
    raw = token.strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    parts = raw.split(".")
    if len(parts) < 2:
        return {}
    try:
        return json.loads(_b64url_decode(parts[1]))
    except Exception:
        return {}


def _read_rh_token() -> Optional[str]:
    if not RH_TOKEN_FILE.exists():
        return None
    raw = RH_TOKEN_FILE.read_text().strip()
    if not raw:
        return None
    return raw if raw.lower().startswith("bearer ") else f"Bearer {raw}"


def _mask_token(bearer: str) -> str:
    """Return 'eyJhbGc…JKvLi' style masked preview."""
    body = bearer.split(" ", 1)[1] if bearer.lower().startswith("bearer ") else bearer
    if len(body) < 16:
        return "***"
    return f"{body[:8]}…{body[-6:]}"


def _token_status_payload(probe: bool = False) -> dict:
    bearer = _read_rh_token()
    if not bearer:
        return {"valid": False, "exp": None, "expires_in_seconds": None,
                "masked": None, "probed": False}
    payload = _decode_jwt_unverified(bearer)
    exp = payload.get("exp")
    now = int(time.time())
    expires_in = (exp - now) if isinstance(exp, (int, float)) else None
    valid_by_exp = expires_in is not None and expires_in > 0
    out = {
        "valid": valid_by_exp,
        "exp": (datetime.fromtimestamp(exp, tz=timezone.utc).isoformat() if exp else None),
        "expires_in_seconds": expires_in,
        "masked": _mask_token(bearer),
        "probed": False,
    }
    if probe:
        try:
            r = requests.get("https://api.robinhood.com/user/",
                             headers={"Authorization": bearer, "Accept": "application/json",
                                      "User-Agent": "Mozilla/5.0"},
                             timeout=10)
            out["probed"] = True
            out["probe_ok"] = (r.status_code == 200)
            out["probe_status"] = r.status_code
            if r.status_code != 200:
                out["valid"] = False
        except requests.RequestException as e:
            out["probed"] = True
            out["probe_ok"] = False
            out["probe_error"] = str(e)[:200]
    return out


@app.get("/api/admin/token-status")
def admin_token_status(probe: bool = Query(False), _=Depends(verify_token)):
    return _token_status_payload(probe=probe)


_BEARER_RE = re.compile(r"Bearer\s+([A-Za-z0-9._\-]+)", re.IGNORECASE)


def _extract_token(blob: str) -> Optional[str]:
    """Pull a Bearer token out of: a raw JWT, 'Bearer <jwt>', or a curl-paste
    that contains a -H 'Authorization: Bearer <jwt>' line."""
    if not blob:
        return None
    blob = blob.strip()
    m = _BEARER_RE.search(blob)
    if m:
        return f"Bearer {m.group(1)}"
    # Maybe just a raw JWT
    if re.fullmatch(r"[A-Za-z0-9._\-]+", blob) and blob.count(".") >= 2:
        return f"Bearer {blob}"
    return None


@app.post("/api/admin/token")
async def admin_set_token(request: Request, _=Depends(verify_token)):
    body = await request.json()
    raw = body.get("token", "") or body.get("blob", "")
    bearer = _extract_token(raw)
    if not bearer:
        raise HTTPException(status_code=400,
                            detail="Could not find a Bearer token in input")
    # Validate against RH before persisting
    try:
        r = requests.get("https://api.robinhood.com/user/",
                         headers={"Authorization": bearer, "Accept": "application/json",
                                  "User-Agent": "Mozilla/5.0"},
                         timeout=10)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Could not reach RH: {e}")
    if r.status_code == 401:
        raise HTTPException(status_code=400, detail="RH rejected the token (401)")
    if r.status_code != 200:
        raise HTTPException(status_code=400,
                            detail=f"RH returned HTTP {r.status_code}; not saving")
    # Atomic write with chmod 600
    RH_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".rh_token.", dir=str(RH_TOKEN_FILE.parent))
    try:
        with os.fdopen(fd, "w") as f:
            f.write(bearer + "\n")
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, RH_TOKEN_FILE)
    except Exception:
        try: os.unlink(tmp_path)
        except FileNotFoundError: pass
        raise
    return _token_status_payload(probe=False)


def _any_running_job() -> Optional[str]:
    with _jobs_lock:
        for jid, job in _jobs.items():
            if job.get("state") == "running":
                return jid
    return None


def _spawn_job(script: str) -> str:
    cmd = ADMIN_SCRIPTS[script]
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    job_id = uuid.uuid4().hex[:12]
    log_path = JOBS_DIR / f"{job_id}.log"
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        cmd, cwd=str(BASE_DIR),
        stdout=log_file, stderr=subprocess.STDOUT,
        bufsize=1, text=True,
    )
    job = {
        "id": job_id,
        "script": script,
        "state": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ended_at": None,
        "exit_code": None,
        "log_path": str(log_path),
        "proc": proc,
        "log_file_handle": log_file,
    }
    with _jobs_lock:
        _jobs[job_id] = job

    # Watcher thread: when proc finishes, flip state and close handle
    def _watch():
        rc = proc.wait()
        with _jobs_lock:
            job["state"] = "done" if rc == 0 else "failed"
            job["exit_code"] = rc
            job["ended_at"] = datetime.now(timezone.utc).isoformat()
        try:
            log_file.flush()
            log_file.close()
        except Exception:
            pass

    threading.Thread(target=_watch, daemon=True).start()
    return job_id


def _job_payload(job: dict, log_tail_bytes: int = 4096) -> dict:
    log_path = Path(job["log_path"])
    log_tail = ""
    if log_path.exists():
        try:
            size = log_path.stat().st_size
            with open(log_path, "rb") as f:
                if size > log_tail_bytes:
                    f.seek(size - log_tail_bytes)
                log_tail = f.read().decode("utf-8", errors="replace")
        except OSError:
            pass
    return {
        "id": job["id"],
        "script": job["script"],
        "state": job["state"],
        "started_at": job["started_at"],
        "ended_at": job["ended_at"],
        "exit_code": job["exit_code"],
        "log_tail": log_tail,
    }


@app.post("/api/admin/run")
async def admin_run(request: Request, _=Depends(verify_token)):
    body = await request.json()
    script = body.get("script", "")
    if script not in ADMIN_SCRIPTS:
        raise HTTPException(status_code=400,
                            detail=f"Unknown script. Allowed: {sorted(ADMIN_SCRIPTS)}")
    busy = _any_running_job()
    if busy:
        raise HTTPException(status_code=409,
                            detail=f"Another job is running ({busy}); wait for it to finish")
    jid = _spawn_job(script)
    with _jobs_lock:
        return _job_payload(_jobs[jid])


@app.get("/api/admin/run/{job_id}")
def admin_run_status(job_id: str, _=Depends(verify_token)):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="No such job")
    return _job_payload(job)


@app.get("/api/admin/runs")
def admin_recent_runs(_=Depends(verify_token)):
    """List recent jobs (newest first), small payload — for showing history."""
    with _jobs_lock:
        jobs = list(_jobs.values())
    jobs.sort(key=lambda j: j.get("started_at") or "", reverse=True)
    return [{
        "id": j["id"], "script": j["script"], "state": j["state"],
        "started_at": j["started_at"], "ended_at": j["ended_at"],
        "exit_code": j["exit_code"],
    } for j in jobs[:20]]


# --- Dashboard route ---

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/dashboard", include_in_schema=False)
def dashboard(request: Request, token: Optional[str] = Query(None)):
    # Verify auth
    verify_token(request, token)
    html = (STATIC_DIR / "index.html").read_text()
    return HTMLResponse(html)

# --- Static files (no auth — the dashboard shell gates access) ---

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
