"""FastAPI dashboard server for rh-trade-exporter."""

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="RH Trade Dashboard")

BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs"
STATIC_DIR = BASE_DIR / "static"
TOKEN_FILE = BASE_DIR / ".server_token"
NOTES_FILE = OUTPUTS_DIR / "journal_notes.json"

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
    return _read_jsonl("cash_flow.jsonl")

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
