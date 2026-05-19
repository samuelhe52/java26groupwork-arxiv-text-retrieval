#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import re
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path

os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = Path(
    os.environ.get("ARXIV_DATASET_DIR", REPO_ROOT / "datasets" / "arxiv-cs-lg-2015-now")
)
WORK_DIR = Path(
    os.environ.get("ARXIV_CACHE_DIR", REPO_ROOT / ".cache" / "arxiv-cs-lg-snapshot")
)
RAW_DIR = WORK_DIR / "raw"
OUTPUT_DIR = BASE_DIR / "years"
MERGED_DATASET_PATH = BASE_DIR / "upload.jsonl"
SOURCE_REPO = "jackkuo/arXiv-metadata-oai-snapshot"
SOURCE_FILE = "arxiv-metadata-oai-snapshot.json"
START_YEAR = 2015
CATEGORY = "cs.LG"
CURRENT_YEAR = datetime.now().year
CURRENT_MONTH = datetime.now().month
ID_RE = re.compile(r"^(?P<yy>\d{2})(?P<mm>\d{2})\.\d+$")


def log(message: str) -> None:
    print(message, flush=True)


def download_snapshot() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = RAW_DIR / SOURCE_FILE

    download_url = (
        f"https://huggingface.co/datasets/{SOURCE_REPO}/resolve/main/{SOURCE_FILE}?download=true"
    )

    remote_size = None
    head = subprocess.run(
        ["curl", "-sI", "-L", download_url],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in head.stdout.splitlines():
        if line.lower().startswith("content-length:"):
            try:
                remote_size = int(line.split(":", 1)[1].strip())
            except ValueError:
                remote_size = None

    local_size = snapshot_path.stat().st_size if snapshot_path.exists() else 0
    if remote_size is not None and local_size == remote_size:
        return snapshot_path

    if local_size > 0:
        log(
            f"resuming snapshot download: {SOURCE_REPO}/{SOURCE_FILE} "
            f"({local_size} / {remote_size or 'unknown'} bytes)"
        )
    else:
        log(f"downloading snapshot: {SOURCE_REPO}/{SOURCE_FILE}")

    subprocess.run(
        [
            "curl",
            "-L",
            "--fail",
            "--retry",
            "5",
            "--retry-all-errors",
            "--retry-delay",
            "5",
            "-C",
            "-",
            "--output",
            str(snapshot_path),
            download_url,
        ],
        check=True,
    )
    if not snapshot_path.exists():
        raise FileNotFoundError(f"snapshot file not found after download: {snapshot_path}")
    return snapshot_path


def parse_submission_date(arxiv_id: str) -> tuple[int | None, int | None]:
    match = ID_RE.match(arxiv_id)
    if not match:
        return None, None
    year = 2000 + int(match.group("yy"))
    month = int(match.group("mm"))
    return year, month


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def open_writer(year: int, handles: dict[int, object]) -> object:
    if year not in handles:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / f"arxiv_{CATEGORY.replace('.', '_')}_{year}.jsonl"
        handles[year] = path.open("w", encoding="utf-8")
    return handles[year]


def assemble(snapshot_path: Path) -> dict:
    if BASE_DIR.exists():
        shutil.rmtree(BASE_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    stats = defaultdict(lambda: {"records": 0, "bytes": 0})
    year_handles: dict[int, object] = {}
    merged_handle = MERGED_DATASET_PATH.open("w", encoding="utf-8")
    total_records = 0
    total_bytes = 0
    max_year_seen = 0

    try:
        with snapshot_path.open("r", encoding="utf-8") as fh:
            for line_no, raw_line in enumerate(fh, start=1):
                if not raw_line.strip():
                    continue

                data = json.loads(raw_line)
                categories_raw = data.get("categories", "")
                categories = categories_raw.split()
                if CATEGORY not in categories:
                    continue

                arxiv_id = data.get("id", "")
                year, month = parse_submission_date(arxiv_id)
                if year is None or year < START_YEAR or year > CURRENT_YEAR:
                    continue

                title = normalize_text(data.get("title"))
                abstract = normalize_text(data.get("abstract"))
                record = {
                    "id": arxiv_id,
                    "year": year,
                    "month": month,
                    "submitter": data.get("submitter"),
                    "authors": data.get("authors"),
                    "title": title,
                    "abstract": abstract,
                    "text": f"{title}\n\n{abstract}",
                    "comments": data.get("comments"),
                    "journal_ref": data.get("journal-ref"),
                    "doi": data.get("doi"),
                    "report_no": data.get("report-no"),
                    "categories": categories_raw,
                    "categories_list": categories,
                    "primary_category": categories[0] if categories else None,
                    "license": data.get("license"),
                    "update_date": data.get("update_date"),
                    "source": SOURCE_REPO,
                }

                encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                encoded_bytes = encoded.encode("utf-8")

                handle = open_writer(year, year_handles)
                handle.write(encoded)
                merged_handle.write(encoded)

                stats[year]["records"] += 1
                stats[year]["bytes"] += len(encoded_bytes)
                total_records += 1
                total_bytes += len(encoded_bytes)
                max_year_seen = max(max_year_seen, year)

                if total_records % 50000 == 0:
                    log(f"filtered {total_records} records so far; latest year={year}")
    finally:
        merged_handle.close()
        for handle in year_handles.values():
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
        "source": {
            "repo": SOURCE_REPO,
            "file": SOURCE_FILE,
            "description": "ArXiv metadata snapshot mirrored on Hugging Face",
        },
        "filter": {
            "category": CATEGORY,
            "start_year": START_YEAR,
            "end_year": CURRENT_YEAR,
            "submission_date_source": "arXiv id prefix YYMM",
        },
        "fields": [
            "id",
            "year",
            "month",
            "submitter",
            "authors",
            "title",
            "abstract",
            "text",
            "comments",
            "journal_ref",
            "doi",
            "report_no",
            "categories",
            "categories_list",
            "primary_category",
            "license",
            "update_date",
            "source",
        ],
        "years": years,
        "totals": {
            "records": total_records,
            "bytes": total_bytes,
            "shards": 1,
        },
        "created_at": datetime.now().astimezone().isoformat(),
    }

    (BASE_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    readme = f"""# arXiv {CATEGORY} corpus, 2015 to now

Source:
- Hugging Face mirror: `{SOURCE_REPO}`
- Snapshot file: `{SOURCE_FILE}`

Filter:
- category: `{CATEGORY}`
- submission year: `>= {START_YEAR}`
- year is derived from the arXiv ID prefix (`YYMM`)

Layout:
- dataset root contains backend-ready merged JSONL: `upload.jsonl`
- `years/` contains one JSONL file per year
- `manifest.json` lists counts and fields

Records include title, abstract, category metadata, and a combined `text` field for search/indexing.
"""
    (BASE_DIR / "README.md").write_text(readme, encoding="utf-8")

    return manifest


def main() -> int:
    snapshot_path = download_snapshot()
    manifest = assemble(snapshot_path)
    log(f"done: {manifest['totals']['records']} records, {manifest['totals']['bytes']} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
