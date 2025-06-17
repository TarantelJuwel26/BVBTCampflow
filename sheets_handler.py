#!/usr/bin/env python
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# colour triplets (G‑Sheets expects 0‑1 floats)
COLOR_GREEN = {"red": 0.0, "green": 1.0, "blue": 0.0}
COLOR_RED   = {"red": 1.0, "green": 0.0, "blue": 0.0}
COLOR_WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}

class GoogleSheetHandler:
    def __init__(
        self,
        spreadsheet_id: str,
        *,
        worksheet: str = "Internet",
        reserved: int = 72,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet
        self.reserved = reserved

        self._svc, self._ss = self._sheets_service()
        self.sheet_id = self._ensure_worksheet()

    # ───── public helpers ─────
    def row_for_position(self, pos: int) -> int:
        return pos + 1 if pos <= self.reserved else pos + 4

    def get_current(self) -> Dict[str, dict]:
        return self._read_sheet()

    def apply_changes(
        self,
        updates: List[Tuple[int, List[str]]],
        formats: List[Tuple[int, Optional[bool]]],
    ) -> None:
        self._batch_write(updates)
        self._colour_rows(formats)

    # ───── private bits ─────
    def _sheets_service(self):
        key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not key:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS missing")
        creds = service_account.Credentials.from_service_account_file(
            key, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        svc = build("sheets", "v4", credentials=creds)
        return svc, svc.spreadsheets()

    def _ensure_worksheet(self):
        ss = self._ss; sid = self.spreadsheet_id
        meta = ss.get(spreadsheetId=sid).execute()
        sheet_id = next(
            (s["properties"]["sheetId"] for s in meta["sheets"] if s["properties"]["title"] == self.worksheet),
            None,
        )
        if sheet_id is None:
            add = {"addSheet": {"properties": {"title": self.worksheet,"gridProperties": {"rowCount": 500,"columnCount": 2}}}}
            sheet_id = ss.batchUpdate(spreadsheetId=sid, body={"requests": [add]}).execute()["replies"][0]["addSheet"]["properties"]["sheetId"]
        # header row
        ss.values().update(spreadsheetId=sid, range=f"{self.worksheet}!A1", valueInputOption="USER_ENTERED", body={"values": [["Startplatz", "Mannschaft"]]}).execute()
        # two blanks + "Warteliste"
        ss.values().update(spreadsheetId=sid, range=f"{self.worksheet}!A{self.reserved+2}:B{self.reserved+4}", valueInputOption="USER_ENTERED", body={"values": [["", ""],["", ""],["", "Warteliste"]]}).execute()
        # bold formatting for the label
        bold = {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": self.reserved+3, "endRowIndex": self.reserved+4, "startColumnIndex": 1, "endColumnIndex": 2}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}}
        ss.batchUpdate(spreadsheetId=sid, body={"requests": [bold]}).execute()
        return sheet_id

    def _read_sheet(self):
        ss = self._ss; sid = self.spreadsheet_id
        res = ss.values().get(spreadsheetId=sid, range=f"{self.worksheet}!A2:B").execute()
        rows = res.get("values", [])
        out: Dict[str, dict] = {}
        def is_paid(c: str):
            return c.endswith("– bestätigt") or c.endswith("- bestätigt")
        for idx, row in enumerate(rows, start=2):
            if idx in (self.reserved+2, self.reserved+3, self.reserved+4):
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

    def _batch_write(self, updates):
        if not updates:
            return
        body = [{"range": f"{self.worksheet}!A{r}:B{r}", "values": [vals]} for r, vals in updates]
        self._ss.values().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"valueInputOption": "USER_ENTERED", "data": body}).execute()

    def _colour_rows(self, formats):
        if not formats:
            return
        reqs = []
        for row, paid in formats:
            color = COLOR_WHITE if paid is None else (COLOR_GREEN if paid else COLOR_RED)
            reqs.append({"repeatCell": {"range": {"sheetId": self.sheet_id, "startRowIndex": row-1, "endRowIndex": row, "startColumnIndex": 1, "endColumnIndex": 2}, "cell": {"userEnteredFormat": {"backgroundColor": color}}, "fields": "userEnteredFormat.backgroundColor"}})
        self._ss.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": reqs}).execute()