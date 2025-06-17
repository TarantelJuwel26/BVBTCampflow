#!/usr/bin/env python

import os
import sys
import time
from datetime import datetime
from typing import List, Optional, Tuple

from campflow_api import CampflowAPI
from sheets_handler import GoogleSheetHandler

# ───── runtime config (environment variables override defaults) ─────
EVENT_ID = os.getenv("EVENT_ID", "lst_tZmFgcC33pXQes4OvtIa")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "0.5"))


def main() -> None:
    # Spreadsheet ID is required – abort early if missing
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("SPREADSHEET_ID missing in env")

    # Instantiate helper classes
    api = CampflowAPI(event_id=EVENT_ID, poll_seconds=POLL_SECONDS)
    sheet = GoogleSheetHandler(spreadsheet_id=sid)

    last_fp: Optional[str] = None
    print(f"Polling every {POLL_SECONDS}s … Ctrl‑C to exit")

    while True:
        try:
            # 1️⃣  fetch & transform Campflow data
            rows = api.make_rows(api.fetch_persons())  # [(position, text, paid)]
            fp = CampflowAPI.fingerprint(rows)
            if fp == last_fp:             # nothing changed since last sync
                time.sleep(POLL_SECONDS)
                continue

            # 2️⃣  read current state of the sheet
            current = sheet.get_current()

            updates: List[Tuple[int, List[str]]] = []        # rows to write
            formats: List[Tuple[int, Optional[bool]]] = []    # rows to colour
            seen: set[str] = set()                           # track present teams

            # 3️⃣  upsert rows that exist or are new
            for pos, text, paid in rows:
                row_idx = sheet.row_for_position(pos)
                team = text.split(" aus ")[0]
                seen.add(team)

                if team in current:
                    if current[team]["text"] != text:        # update text only if changed
                        updates.append((row_idx, [pos, text]))
                else:                                         # new entry – write pos+text
                    updates.append((row_idx, [pos, text]))

                formats.append((row_idx, paid))               # colour row green/red

            # 4️⃣  clear rows whose teams disappeared
            for team, meta in current.items():
                if team not in seen:
                    updates.append((meta["row"], ["", ""]))
                    formats.append((meta["row"], None))      # reset colour

            # 5️⃣  push changes in one go
            sheet.apply_changes(updates, formats)
            last_fp = fp

            print(
                f"[{datetime.now():%H:%M:%S}] synced "
                f"(rows={len(rows)}, updates={len(updates)})"
            )

        except Exception as exc:
            # network issues, API errors, Google API errors … log & keep running
            print("‼", exc)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped.")
        sys.exit(0)