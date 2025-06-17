#!/usr/bin/env python

from __future__ import annotations

import csv
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

# ───────── public constants (imported by main.py) ─────────
BASE_URL: str = "https://api.campflow.de/"
TEAM_NAME_COL: str = "col_9RodWlHTUW1bHtBe1VvN"
VILLAGE_COL: str   = "col_ZUBDynEEutHqO8PX7GDL"
LABEL_PAID: str    = "Bezahlt"
POLL_SECONDS: float = 0.5
TIMEOUT: int = 10
CSV_PATH: str | Path = "campflow.csv"


class CampflowAPI:
    """All interaction with Campflow (+optional CSV snapshots)."""

    def __init__(
        self,
        event_id: str,
        *,
        base_url: str = BASE_URL,
        timeout: int = TIMEOUT,
        poll_seconds: float = POLL_SECONDS,
        csv_path: str | Path = CSV_PATH,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.event_id = event_id
        self.timeout = timeout
        self.poll_seconds = poll_seconds
        self.csv_path = Path(csv_path)
        self._counter: int = 0  # seconds accumulator

    # ───── high‑level entry points ─────
    def fetch_persons(self) -> Dict[str, Any]:
        data = self._fetch(f"lists/{self.event_id}/persons")
        # strip cancellations
        data["data"] = [d for d in data["data"] if not d.get("cancellation_date")]
        self._maybe_snapshot(data["data"])
        return data

    def make_rows(self, payload: Dict[str, Any]) -> List[Tuple[int, str, bool]]:
        items = payload.get("data", [])

        def to_dt(s: str):
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            elif "+" not in s[-6:] and "-" not in s[-6:]:
                s += "+00:00"
            return datetime.fromisoformat(s).astimezone(timezone.utc)

        ordered = sorted(items, key=lambda it: to_dt(it["creation_date"]))
        rows: List[Tuple[int, str, bool]] = []
        for pos, it in enumerate(ordered, 1):
            team = it.get(TEAM_NAME_COL, "").strip()
            village = it.get(VILLAGE_COL, "").strip()
            paid = LABEL_PAID in it.get("label_names", [])
            txt = f"{team} aus {village} – {'bestätigt' if paid else 'unbestätigt'}"
            rows.append((pos, txt, paid))
        return rows

    @staticmethod
    def fingerprint(rows: List[Tuple[int, str, bool]]) -> str:
        return hashlib.sha256(
            json.dumps(rows, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()

    # ───── helpers ─────
    def _fetch(self, path: str, **params) -> Dict[str, Any]:
        load_dotenv()
        token = os.getenv("API_TOKEN")
        if not token:
            raise RuntimeError("API_TOKEN missing in env")
        url = urljoin(self.base_url, path.lstrip("/"))
        r = requests.get(url, headers={"Authorization": f"Bearer {token.strip()}"}, params=params or None, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _maybe_snapshot(self, persons):
        self._counter += 1
        if self._counter * self.poll_seconds < 10:
            return
        self._counter = 0
        _save_persons_to_csv(persons, self.csv_path)


# ──────────────────── internal utils ────────────────────

def _flatten(obj: Dict[str, Any], parent: str = "", sep: str = ".") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, val in obj.items():
        new_key = f"{parent}{sep}{key}" if parent else key
        if isinstance(val, dict):
            out.update(_flatten(val, new_key, sep))
        elif isinstance(val, list):
            if all(isinstance(v, (str, int, float, bool)) or v is None for v in val):
                out[new_key] = ";".join("" if v is None else str(v) for v in val)
            else:
                out[new_key] = ";".join(json.dumps(v, ensure_ascii=False) for v in val)
        else:
            out[new_key] = val
    return out


def _save_persons_to_csv(persons, path: Path):
    flat = [_flatten(p) for p in persons]
    fieldnames = sorted({k for f in flat for k in f})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader(); w.writerows(flat)
    print(f"✅  snapshot → {path.resolve()}")