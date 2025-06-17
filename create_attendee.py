#!/usr/bin/env python
"""
mass_create_attendees.py  –  create N attendees in a Campflow list

Example
-------
python mass_create_attendees.py lst_tZmFgcC33pXQes4OvtIa 90
"""

from __future__ import annotations
import argparse, json, os, random, sys
from datetime import date, timedelta
from typing import Any, Dict

import requests
from dotenv import load_dotenv

# ─────────────────── configuration ───────────────────
BASE_URL   = os.getenv("BASE_URL", "https://api.campflow.de/")
API_TOKEN  = os.getenv("API_TOKEN")           # must be set in .env

PRICE      = "Teilnehmendenbeitrag"
EMAIL      = "bvbt.messingen@gmail.com"
PHONE      = "+49 000 00000000"               # dummy – change if you want
COUNTRY    = {"country_code": "de", "country_name": "Deutschland"}

# Sample pools – extend or replace freely
FIRST_NAMES = ["Tom", "Luca", "Mia", "Anna", "Jonas", "Lea", "Felix", "Paula"]
LAST_NAMES  = ["Brüning", "Schmidt", "Meyer", "Fischer", "Schneider", "Wagner"]
VILLAGES    = ["Spelle", "Andervenne", "Messingen", "Beesten", "Halverde",
               "Schapen", "Venhaus", "Freren"]

TEAM_NAMES  = [
    "Strandsäufer", "Feuerfüchse", "Waldkobolde", "Seeadler", "Bergsteiger",
    "Flussratten",  "Wiesenhopser", "Sternschnuppen",
]
# custom column IDs (change if your list uses other IDs)
COL_DUP_MAIL = "col_nbCINop8bmZKiH8phuEb"
COL_TEAMNAME = "col_9RodWlHTUW1bHtBe1VvN"
COL_VILLAGE  = "col_ZUBDynEEutHqO8PX7GDL"
# ──────────────────────────────────────────────────────


def random_birthdate(min_age: int = 10, max_age: int = 25) -> str:
    """Return YYYY-MM-DD for a random date between min_age and max_age (inclusive)."""
    today = date.today()
    start = today - timedelta(days=max_age * 365)
    end   = today - timedelta(days=min_age * 365)
    days  = random.randint(0, (end - start).days)
    return (start + timedelta(days=days)).isoformat()


def build_payload(first: str, last: str, team: str, village: str) -> Dict[str, Any]:
    return {
        # core
        "name": {"first_name": first, "last_name": last},
        "primary_email": EMAIL,
        "phone_numbers": [{"label": None, "number": PHONE}],
        "address": {
            "street":     "",
            "postcode":   "",
            "city":       village,
            "postal_info": None,
            **COUNTRY,
        },
        "birthdate": random_birthdate(),
        "label_names": [],
        # custom columns
        COL_DUP_MAIL: EMAIL,
        COL_TEAMNAME: team,
        COL_VILLAGE:  village,
    }


def post_person(list_id: str, payload: Dict[str, Any]) -> None:
    url = f"{BASE_URL.rstrip('/')}/lists/{list_id}/persons"
    headers = {
        "Authorization": f"Bearer {API_TOKEN.strip()}",
        "Content-Type":  "application/json",
    }
    print("\n➡  POST", url)
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    r = requests.post(url, headers=headers, json=payload, timeout=15)
    print("⬅ ", r.status_code, r.reason)
    if r.ok:
        try:
            print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        except ValueError:
            print(r.text)
    else:
        raise SystemExit(f"Request failed: {r.status_code} {r.text}")


def main() -> None:
    if not API_TOKEN:
        sys.exit("ERROR: API_TOKEN missing in environment (.env)")

    parser = argparse.ArgumentParser(description="Bulk-create Campflow persons")
    parser.add_argument("list_id", help="List ID starting with lst_…")
    parser.add_argument("count", type=int, help="How many attendees to create")
    args = parser.parse_args()

    for i in range(1, args.count + 1):
        first   = random.choice(FIRST_NAMES)
        last    = random.choice(LAST_NAMES)
        team    = random.choice(TEAM_NAMES) + f" {i}"            # make names unique
        village = random.choice(VILLAGES)

        payload = build_payload(first, last, team, village)
        post_person(args.list_id, payload)

    print(f"\n✅ Successfully sent {args.count} requests.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.")
