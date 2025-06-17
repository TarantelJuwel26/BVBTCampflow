#!/usr/bin/env python


from __future__ import annotations
import hashlib, json, os, sys, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import csv
from pathlib import Path

# ───────── configuration ─────────
BASE_URL  = "https://api.campflow.de/"
EVENT_ID  = "lst_tZmFgcC33pXQes4OvtIa"

TEAM_NAME_COL = "col_9RodWlHTUW1bHtBe1VvN"
VILLAGE_COL   = "col_ZUBDynEEutHqO8PX7GDL"
LABEL_PAID    = "Bezahlt"

WORKSHEET     = "Internet"
RESERVED      = 72                    # fixed places before wait-list
POLL_SECONDS  = 0.5
TIMEOUT       = 10

COLOR_GREEN   = {"red": 0.0, "green": 1.0, "blue": 0.0}
COLOR_RED     = {"red": 1.0, "green": 0.0, "blue": 0.0}
COLOR_WHITE   = {"red": 1.0, "green": 1.0, "blue": 1.0}

csv_path = "campflow.csv"
counter = 0
# ──────────────────────────────────


# ═════════ Google Sheets helpers ═════════
def sheets_service():
    key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS missing")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = service_account.Credentials.from_service_account_file(key, scopes=scopes)
    svc    = build("sheets", "v4", credentials=creds)
    return svc, svc.spreadsheets()


def ensure_worksheet(ss, sid: str, title: str) -> int:
    """
    Ensures sheet exists, header row, two blanks & Warteliste in place.
    Returns sheetId.
    """
    meta = ss.get(spreadsheetId=sid).execute()
    sheet_id: Optional[int] = None
    for s in meta["sheets"]:
        if s["properties"]["title"] == title:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        add = {"addSheet": {"properties": {"title": title,
                                           "gridProperties": {"rowCount": 500,
                                                              "columnCount": 2}}}}
        sheet_id = ss.batchUpdate(spreadsheetId=sid,
                                  body={"requests": [add]}).execute()["replies"][0]["addSheet"]["properties"]["sheetId"]

    # header
    header = [["Startplatz", "Mannschaft"]]
    ss.values().update(spreadsheetId=sid, range=f"{title}!A1",
                       valueInputOption="USER_ENTERED", body={"values": header}).execute()

    # two blank rows + Warteliste label
    ss.values().update(spreadsheetId=sid,
                       range=f"{title}!A{RESERVED+2}:B{RESERVED+4}",
                       valueInputOption="USER_ENTERED",
                       body={"values": [["", ""], ["", ""], ["", "Warteliste"]]}).execute()

    # bold Warteliste
    bold_req = {"repeatCell": {
        "range": {"sheetId": sheet_id,
                  "startRowIndex": RESERVED+1+2,   # 0-based row 75 (display 76)
                  "endRowIndex":   RESERVED+1+3,
                  "startColumnIndex": 1,
                  "endColumnIndex":   2},
        "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
        "fields": "userEnteredFormat.textFormat.bold"}}
    ss.batchUpdate(spreadsheetId=sid, body={"requests": [bold_req]}).execute()

    return sheet_id


def sheet_row_for_pos(pos: int) -> int:
    """Convert position → 1-based row number in sheet."""
    return pos + 1 if pos <= RESERVED else pos + 4


def read_sheet(ss, sid: str) -> Dict[str, dict]:
    """
    team-name → {row, pos, paid, text}
    Ignores header, blank rows, 'Warteliste'.
    """
    res = ss.values().get(spreadsheetId=sid,
                          range=f"{WORKSHEET}!A2:B").execute()
    rows = res.get("values", [])
    out: Dict[str, dict] = {}

    def is_paid(cell: str) -> bool:
        return cell.endswith("– bestätigt") or cell.endswith("- bestätigt")

    for idx, row in enumerate(rows, start=2):
        if idx in (RESERVED+2, RESERVED+3) or (idx == RESERVED+4):  # blanks + label
            continue
        if not row:
            continue
        cell = row[1] if len(row) > 1 else ""
        if cell.strip() == "Warteliste":
            continue
        team = cell.split(" aus ")[0] if " aus " in cell else cell
        paid = is_paid(cell)
        try:
            pos = int(row[0])
        except (IndexError, ValueError):
            pos = 0
        out[team] = {"row": idx, "pos": pos, "paid": paid, "text": cell}
    return out


def batch_write(ss, sid: str, updates: List[Tuple[int, List[str]]]):
    if not updates:
        return
    body = [{"range": f"{WORKSHEET}!A{r}:B{r}", "values": [vals]} for r, vals in updates]
    ss.values().batchUpdate(spreadsheetId=sid,
                            body={"valueInputOption": "USER_ENTERED", "data": body}).execute()


def colour_rows(ss, sid: str, sheet_id: int, fmt: List[Tuple[int, Optional[bool]]]):
    if not fmt:
        return
    reqs = []
    for row, paid in fmt:
        color = COLOR_WHITE if paid is None else (COLOR_GREEN if paid else COLOR_RED)
        reqs.append({"repeatCell": {
            "range": {"sheetId": sheet_id,
                      "startRowIndex": row-1,
                      "endRowIndex": row,
                      "startColumnIndex": 1,
                      "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"backgroundColor": color}},
            "fields":  "userEnteredFormat.backgroundColor"}})
    ss.batchUpdate(spreadsheetId=sid, body={"requests": reqs}).execute()


# ═════════ Campflow helpers ═════════
def fetch(path: str, **q) -> Dict[str, Any]:
    load_dotenv()
    tok = os.getenv("API_TOKEN")
    if not tok:
        raise RuntimeError("API_TOKEN missing")
    r = requests.get(urljoin(BASE_URL, path.lstrip("/")),
                     headers={"Authorization": f"Bearer {tok.strip()}"},
                     params=q or None, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def persons() -> Dict[str, Any]:
    data = fetch(f"lists/{EVENT_ID}/persons")
    data["data"] = [d for d in data["data"] if d["cancellation_date"] == ""]
    
    global counter
    # save every 10 seconds the data to a CSV file based on the counter and the POLL_SECONDS
    if counter * POLL_SECONDS >= 10:
        save_persons_to_csv(data["data"], csv_path)
        # reset the counter
        counter = 0
    counter += 1



    return data


def make_rows(payload: Dict[str, Any]) -> List[Tuple[int, str, bool]]:
    items = payload.get("data", [])
    def to_dt(s: str) -> datetime:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        elif "+" not in s[-6:] and "-" not in s[-6:]:
            s = s + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    ordered = sorted(items, key=lambda i: to_dt(i["creation_date"]))
    rows: List[Tuple[int, str, bool]] = []
    for pos, it in enumerate(ordered, 1):
        team    = it.get(TEAM_NAME_COL, "").strip()
        village = it.get(VILLAGE_COL, "").strip()
        paid    = LABEL_PAID in it.get("label_names", [])
        text = f"{team} aus {village} – {'bestätigt' if paid else 'unbestätigt'}"
        rows.append((pos, text, paid))
    return rows


def fingerprint(rows) -> str:
    return hashlib.sha256(json.dumps(rows, separators=(",", ":"), sort_keys=True)
                          .encode()).hexdigest()


# ═════════ data helpers ═════════
def _flatten(obj: Dict[str, Any], parent: str = "", sep: str = ".") -> Dict[str, Any]:
    """
    Recursively flattens a nested dict.

    name = {"first_name": "Tom"}       →  {"name.first_name": "Tom"}
    phone_numbers = [{"number": "+49"} →  {"phone_numbers": "+49"}  (joined with ;)
    """
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        new_key = f"{parent}{sep}{key}" if parent else key
        if isinstance(value, dict):
            out.update(_flatten(value, new_key, sep))
        elif isinstance(value, list):
            # join scalars / extract dict items if they’re simple
            if all(isinstance(v, (str, int, float, bool)) or v is None for v in value):
                out[new_key] = ";".join("" if v is None else str(v) for v in value)
            else:
                # complex list of dicts → keep JSON string for safety
                out[new_key] = ";".join(json.dumps(v, ensure_ascii=False) for v in value)
        else:
            out[new_key] = value
    return out


def save_persons_to_csv(persons: List[Dict[str, Any]], path: str | Path) -> None:
    """
    Write every person dict from Campflow to one CSV row.

    • Column names = flattened keys (dot-notation for nested objects).
    • Missing values are left blank.
    """
    if not persons:
        raise ValueError("Person list is empty")

    # 1️⃣  flatten every person so nested fields become simple columns
    flat_people = [_flatten(p) for p in persons]

    # 2️⃣  gather the full union of columns (order = sorted for determinism)
    fieldnames = sorted({k for person in flat_people for k in person})

    # 3️⃣  write the CSV
    path = Path(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for person in flat_people:
            writer.writerow(person)

    print(f"✅  Saved {len(flat_people)} people to {path.resolve()}")

# ═════════ sync loop ═════════
def main() -> None:
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("SPREADSHEET_ID missing")
    svc, ss = sheets_service()
    sheet_id = ensure_worksheet(ss, sid, WORKSHEET)

    last_fp: Optional[str] = None
    print(f"Polling every {POLL_SECONDS}s … Ctrl-C to exit")

    while True:
        try:
            rows = make_rows(persons())         # [(pos, text, paid)]
            fp   = fingerprint(rows)
            if fp == last_fp:
                time.sleep(POLL_SECONDS); continue

            current = read_sheet(ss, sid)

            updates: List[Tuple[int, List[str]]] = []
            fmt:     List[Tuple[int, Optional[bool]]] = []
            seen: set[str] = set()

            # upsert current list
            for pos, text, paid in rows:
                r = sheet_row_for_pos(pos)
                team = text.split(" aus ")[0]
                seen.add(team)
                if team in current:
                    if current[team]["text"] != text:
                        updates.append((r, [pos, text]))
                else:
                    updates.append((r, [pos, text]))
                fmt.append((r, paid))

            # clear rows for teams that disappeared
            for team, meta in current.items():
                if team not in seen:
                    updates.append((meta["row"], ["", ""]))
                    fmt.append((meta["row"], None))

            batch_write(ss, sid, updates)
            colour_rows(ss, sid, sheet_id, fmt)

            last_fp = fp
            #print(f"[{datetime.utcnow():%H:%M:%S}Z] synced "
            #      f"(rows={len(rows)}, updates={len(updates)})")
            #utc.now is deprecated, use timezone aware datetime for german time
            print(f"[{datetime.now():%H:%M:%S}] synced "
                  f"(rows={len(rows)}, updates={len(updates)})")

        except (requests.RequestException, HttpError, Exception) as e:
            print("‼", e)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:   main()
    except KeyboardInterrupt:
        print("\nStopped."); sys.exit(0)
