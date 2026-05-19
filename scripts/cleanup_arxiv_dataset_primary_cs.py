#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import re
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = Path(
    os.environ.get("ARXIV_SOURCE_DIR", REPO_ROOT / "datasets" / "arxiv-cs-lg-2015-now")
)
TARGET_DIR = Path(
    os.environ.get(
        "ARXIV_TARGET_DIR",
        REPO_ROOT / "datasets" / "arxiv-cs-lg-2015-now-primary-cs-only",
    )
)
SOURCE_YEARS_DIR = SOURCE_DIR / "years"
TARGET_YEARS_DIR = TARGET_DIR / "years"
MERGED_DATASET_PATH = TARGET_DIR / "upload.jsonl"
PRIMARY_CS_RE = re.compile(r"^cs\.[A-Z]{2}$")


def open_writer(year: int, handles: dict[int, object]) -> object:
    if year not in handles:
        TARGET_YEARS_DIR.mkdir(parents=True, exist_ok=True)
        path = TARGET_YEARS_DIR / f"arxiv_cs_primary_only_{year}.jsonl"
        handles[year] = path.open("w", encoding="utf-8")
    return handles[year]


def main() -> int:
    manifest_path = SOURCE_DIR / "manifest.json"
    source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if TARGET_DIR.exists():
        shutil.rmtree(TARGET_DIR)
    TARGET_YEARS_DIR.mkdir(parents=True, exist_ok=True)

    stats = defaultdict(lambda: {"records": 0, "bytes": 0})
    handles: dict[int, object] = {}
    merged_handle = MERGED_DATASET_PATH.open("w", encoding="utf-8")
    total_records = 0
    total_bytes = 0

    try:
        for src_path in sorted(SOURCE_YEARS_DIR.glob("*.jsonl")):
            with src_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    primary_category = record.get("primary_category")
                    if not isinstance(primary_category, str) or not PRIMARY_CS_RE.match(primary_category):
                        continue

                    year = int(record["year"])
                    encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                    encoded_bytes = encoded.encode("utf-8")

                    handle = open_writer(year, handles)
                    handle.write(encoded)
                    merged_handle.write(encoded)

                    stats[year]["records"] += 1
                    stats[year]["bytes"] += len(encoded_bytes)
                    total_records += 1
                    total_bytes += len(encoded_bytes)
    finally:
        merged_handle.close()
        for handle in handles.values():
            handle.close()

    years = [
        {
            "year": year,
            "records": bucket["records"],
            "bytes": bucket["bytes"],
        }
        for year, bucket in sorted(stats.items())
    ]

    manifest = {
        "source": source_manifest.get("source", {}),
        "derived_from": str(SOURCE_DIR.name),
        "filter": {
            "input_dataset": SOURCE_DIR.name,
            "primary_category_pattern": r"^cs\.[A-Z]{2}$",
            "start_year": source_manifest.get("filter", {}).get("start_year"),
            "end_year": source_manifest.get("filter", {}).get("end_year"),
        },
        "fields": source_manifest.get("fields", []),
        "years": years,
        "totals": {
            "records": total_records,
            "bytes": total_bytes,
            "shards": 1,
        },
        "created_at": datetime.now().astimezone().isoformat(),
    }

    (TARGET_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    readme = f"""# arXiv cs.LG corpus, 2015 to now, primary cs.* only

Source:
- Derived from local dataset: `{SOURCE_DIR.name}`

Filter:
- keep only records where `primary_category` matches `cs.XX`
- original `cs.LG` inclusion filter is preserved from the source dataset

Layout:
- dataset root contains backend-ready merged JSONL: `upload.jsonl`
- `years/` contains one JSONL file per year
- `manifest.json` lists counts and fields

This dataset is a cleaned derivative and does not modify the original dataset.
"""
    (TARGET_DIR / "README.md").write_text(readme, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
