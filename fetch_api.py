#!/usr/bin/env python
#$ python test_api.py                    # → /lists/<EVENT_ID>/persons
#$ python test_api.py persons/defTd12312 # → /lists/<EVENT_ID>/persons/defTd12312
#$ python test_api.py ""                 # → /lists/<EVENT_ID>/ (root list object)

import argparse
import json
import os
import sys
from typing import Any, Dict

from campflow_api import CampflowAPI

EVENT_ID = os.getenv("EVENT_ID", "lst_tZmFgcC33pXQes4OvtIa")


def _pretty(obj: Dict[str, Any]):
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def _list_keys(obj: Any):
    """Print top‑level keys and, if present, HAL‑style `_links`."""
    if isinstance(obj, dict):
        keys = [k for k in obj.keys() if k != "_links"]
        if keys:
            print("keys :", ", ".join(keys))
        if "_links" in obj and isinstance(obj["_links"], dict):
            print("links:", ", ".join(obj["_links"].keys()))
    else:
        print(f"(response is a {type(obj).__name__}, no further structure to list)")


def main() -> None:
    p = argparse.ArgumentParser(description="Query any Campflow sub‑resource")
    p.add_argument("subpath", nargs="?", default="persons", help="Sub‑resource path relative to /lists/<EVENT_ID>/ (default: persons)")
    p.add_argument("--list", "-l", action="store_true", help="List available keys/_links instead of dumping full JSON")
    args = p.parse_args()

    api = CampflowAPI(event_id=EVENT_ID)

    # Build full API path; empty subpath → root list object
    api_path = f"lists/{EVENT_ID}/{args.subpath}".rstrip("/")

    try:
        data = api._fetch(api_path)
    except Exception as exc:
        print("❌", exc)
        sys.exit(1)

    if args.list:
        _list_keys(data)
    else:
        _pretty(data)


if __name__ == "__main__":
    main()