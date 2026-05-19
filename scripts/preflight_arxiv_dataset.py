#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

try:
    import resource
except ImportError:  # pragma: no cover - unavailable on Windows
    resource = None

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_DIR = REPO_ROOT / "datasets" / "arxiv-cs-lg-2015-now-primary-cs-only"
DEFAULT_QUERIES = [
    "graph neural network",
    "reinforcement learning",
    "federated learning",
    "contrastive learning",
    "causal inference",
    "diffusion model",
    "large language model",
]

WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9'\-]+")
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "based",
    "be",
    "been",
    "but",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "dataset",
    "datasets",
    "different",
    "first",
    "for",
    "from",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "method",
    "methods",
    "model",
    "models",
    "may",
    "me",
    "might",
    "must",
    "my",
    "no",
    "not",
    "of",
    "on",
    "one",
    "or",
    "our",
    "out",
    "over",
    "paper",
    "problem",
    "problems",
    "propose",
    "proposed",
    "present",
    "results",
    "s",
    "second",
    "she",
    "should",
    "show",
    "studies",
    "study",
    "so",
    "such",
    "t",
    "than",
    "that",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "two",
    "to",
    "too",
    "under",
    "up",
    "very",
    "was",
    "we",
    "way",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "will",
    "will",
    "with",
    "work",
    "using",
    "would",
    "new",
    "newly",
    "also",
    "however",
    "you",
    "your",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Python-only preflight checks on the arXiv JSONL corpus."
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=DEFAULT_DATASET_DIR,
        help="Dataset directory containing manifest.json plus a merged .jsonl file or a years/ shard directory.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the Markdown report.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Override or add a TF-IDF smoke-test query. Repeatable.",
    )
    parser.add_argument(
        "--year-keyword-since",
        type=int,
        default=2019,
        help="Only report yearly keywords from this year onward.",
    )
    parser.add_argument(
        "--year-keyword-count",
        type=int,
        default=10,
        help="Number of keywords to show for each year.",
    )
    parser.add_argument(
        "--year-keyword-min-count",
        type=int,
        default=40,
        help="Minimum within-year term count required to qualify as a yearly keyword.",
    )
    return parser.parse_args()


def normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def tokenize(text: str) -> list[str]:
    return [token for token in WORD_RE.findall(text.lower()) if token not in STOPWORDS]


def tokenize_raw(text: str) -> list[str]:
    return WORD_RE.findall(text.lower())


def load_manifest(dataset_dir: Path) -> dict:
    manifest_path = dataset_dir / "manifest.json"
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def iter_records(dataset_dir: Path):
    shard_paths = list_dataset_shards(dataset_dir)
    for shard_path in shard_paths:
        with shard_path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                yield shard_path, line_no, json.loads(line)


def list_dataset_shards(dataset_dir: Path) -> list[Path]:
    root_shards = sorted(path for path in dataset_dir.glob("*.jsonl") if path.is_file())
    if root_shards:
        return root_shards

    years_dir = dataset_dir / "years"
    return sorted(path for path in years_dir.glob("*.jsonl") if path.is_file())


def percentile(values: list[int], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * (pct / 100.0)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[int(position)])
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def format_count(value: int) -> str:
    return f"{value:,}"


def format_pct(value: float) -> str:
    return f"{value:.2f}%"


def format_seconds(value: float) -> str:
    return f"{value:.2f} s"


def format_mib(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f} MiB"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def render_row(row: list[str]) -> str:
        return "| " + " | ".join(
            cell.ljust(widths[idx]) for idx, cell in enumerate(row)
        ) + " |"

    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    lines = [render_row(headers), separator]
    lines.extend(render_row(row) for row in rows)
    return "\n".join(lines)


def score_year_keywords(
    year_term_frequency: dict[int, Counter[str]],
    year_token_totals: Counter[int],
    global_term_frequency: Counter[str],
    global_token_total: int,
    since_year: int,
    keyword_count: int,
    min_count: int,
) -> dict[int, list[tuple[str, int, float]]]:
    if global_token_total <= 0:
        return {}

    scored: dict[int, list[tuple[str, int, float]]] = {}
    for year in sorted(year_term_frequency):
        if year < since_year:
            continue
        total = year_token_totals.get(year, 0)
        if total <= 0:
            continue

        year_scores: list[tuple[str, int, float]] = []
        for term, count in year_term_frequency[year].items():
            if count < min_count:
                continue
            global_count = global_term_frequency.get(term, 0)
            if global_count <= 0:
                continue
            year_rate = (count + 1.0) / total
            global_rate = (global_count + 1.0) / global_token_total
            score = math.log(year_rate / global_rate)
            year_scores.append((term, count, score))

        year_scores.sort(key=lambda item: (-item[2], -item[1], item[0]))
        scored[year] = year_scores[:keyword_count]

    return scored


def max_rss_mib(ru_maxrss: int | None) -> float | None:
    if ru_maxrss is None:
        return None
    if sys.platform == "darwin":
        return ru_maxrss / (1024 * 1024)
    return ru_maxrss / 1024.0


def main() -> int:
    args = parse_args()
    start_wall = time.perf_counter()
    start_usage = resource.getrusage(resource.RUSAGE_SELF) if resource else None
    dataset_dir = args.dataset_dir.resolve()
    manifest = load_manifest(dataset_dir)
    expected_years = {
        entry["year"]: entry["records"]
        for entry in manifest.get("years", [])
        if isinstance(entry, dict) and "year" in entry and "records" in entry
    }
    expected_total_records = manifest.get("totals", {}).get("records")
    expected_fields = set(manifest.get("fields", []))

    doc_meta: list[dict[str, object]] = []
    yearly_counts: Counter[int] = Counter()
    year_term_frequency: dict[int, Counter[str]] = defaultdict(Counter)
    year_token_totals: Counter[int] = Counter()
    primary_category_counts: Counter[str] = Counter()
    term_frequency: Counter[str] = Counter()
    document_frequency: Counter[str] = Counter()
    query_postings: dict[str, list[tuple[int, int]]] = defaultdict(list)
    duplicate_hashes: Counter[str] = Counter()
    invalid_json = 0
    schema_mismatches = 0
    missing_title = 0
    missing_abstract = 0
    missing_text = 0
    short_abstracts = 0
    title_lengths: list[int] = []
    abstract_lengths: list[int] = []
    text_lengths: list[int] = []
    sample_schema_mismatch: list[str] = []
    sample_errors: list[str] = []

    queries = args.queries or DEFAULT_QUERIES
    query_terms = sorted(
        {
            token
            for query in queries
            for token in tokenize(query)
        }
    )

    for shard_path in list_dataset_shards(dataset_dir):
        with shard_path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    invalid_json += 1
                    if len(sample_errors) < 5:
                        sample_errors.append(
                            f"{shard_path.name}:{line_no} invalid JSON: {exc.msg}"
                        )
                    continue

                doc_index = len(doc_meta)
                doc_id = str(record.get("id", ""))
                year = record.get("year")
                title = normalize_text(record.get("title"))
                abstract = normalize_text(record.get("abstract"))
                text = normalize_text(record.get("text")) or f"{title}\n\n{abstract}"

                if expected_fields and set(record.keys()) != expected_fields:
                    schema_mismatches += 1
                    if len(sample_schema_mismatch) < 5:
                        sample_schema_mismatch.append(
                            f"{shard_path.name}:{line_no} keys={sorted(record.keys())}"
                        )

                if not title:
                    missing_title += 1
                if not abstract:
                    missing_abstract += 1
                if not text:
                    missing_text += 1

                if isinstance(year, int):
                    yearly_counts[year] += 1
                primary_category = record.get("primary_category")
                if isinstance(primary_category, str) and primary_category:
                    primary_category_counts[primary_category] += 1

                title_tokens_raw = tokenize_raw(title)
                abstract_tokens_raw = tokenize_raw(abstract)
                text_tokens_raw = tokenize_raw(text)
                title_lengths.append(len(title_tokens_raw))
                abstract_lengths.append(len(abstract_tokens_raw))
                text_lengths.append(len(text_tokens_raw))
                if len(abstract_tokens_raw) < 40:
                    short_abstracts += 1

                tokens = tokenize(text)
                token_counts = Counter(tokens)
                term_frequency.update(tokens)
                for token in set(tokens):
                    document_frequency[token] += 1
                if isinstance(year, int):
                    year_term_frequency[year].update(tokens)
                    year_token_totals[year] += len(tokens)
                for term in query_terms:
                    tf = token_counts.get(term)
                    if tf:
                        query_postings[term].append((doc_index, tf))

                digest = hashlib.sha1(f"{title}\0{abstract}".encode("utf-8")).hexdigest()
                duplicate_hashes[digest] += 1
                doc_meta.append(
                    {
                        "id": doc_id,
                        "title": title,
                        "year": year,
                        "primary_category": primary_category,
                    }
                )

    doc_count = len(doc_meta)
    duplicate_title_abstract_groups = sum(1 for count in duplicate_hashes.values() if count > 1)
    duplicate_title_abstract_records = sum(
        count - 1 for count in duplicate_hashes.values() if count > 1
    )
    vocab_size = len(term_frequency)
    global_token_total = sum(term_frequency.values())
    abstract_median = statistics.median(abstract_lengths) if abstract_lengths else 0.0
    abstract_p10 = percentile(abstract_lengths, 10)
    abstract_p90 = percentile(abstract_lengths, 90)
    abstract_max = max(abstract_lengths) if abstract_lengths else 0
    title_median = statistics.median(title_lengths) if title_lengths else 0.0
    text_median = statistics.median(text_lengths) if text_lengths else 0.0
    manifest_total_match = (
        expected_total_records is None or expected_total_records == doc_count
    )
    all_years = sorted(set(yearly_counts) | set(expected_years))
    year_mismatches = {
        year: {"manifest": expected_years.get(year), "actual": yearly_counts.get(year)}
        for year in all_years
        if expected_years.get(year) != yearly_counts.get(year)
    }
    missing_rate = (
        (missing_title + missing_abstract + missing_text) / (doc_count * 3)
        if doc_count
        else 0.0
    )
    shard_count = len(list_dataset_shards(dataset_dir))
    year_keywords = score_year_keywords(
        year_term_frequency=year_term_frequency,
        year_token_totals=year_token_totals,
        global_term_frequency=term_frequency,
        global_token_total=global_token_total,
        since_year=args.year_keyword_since,
        keyword_count=args.year_keyword_count,
        min_count=args.year_keyword_min_count,
    )
    end_wall = time.perf_counter()
    end_usage = resource.getrusage(resource.RUSAGE_SELF) if resource else None
    wall_seconds = end_wall - start_wall
    cpu_seconds = (
        (end_usage.ru_utime + end_usage.ru_stime) - (start_usage.ru_utime + start_usage.ru_stime)
        if start_usage and end_usage
        else None
    )
    peak_rss = max_rss_mib(end_usage.ru_maxrss if end_usage else None) if end_usage else None

    report_lines: list[str] = [
        "# arXiv Dataset Preflight Report",
        "",
        f"- Dataset: `{dataset_dir}`",
        f"- Records: `{format_count(doc_count)}`",
    ]
    if expected_total_records is not None:
        report_lines.append(
            f"- Manifest records: `{format_count(expected_total_records)}`"
            f" ({'match' if manifest_total_match else 'mismatch'})"
        )
    report_lines.extend(
        [
            f"- Years covered: `{min(yearly_counts)}` to `{max(yearly_counts)}`"
            if yearly_counts
            else "- Years covered: `n/a`",
            f"- JSONL shards: `{shard_count}`",
            "",
            "## Structural Checks",
        ]
    )

    structural_rows = [
        ["Invalid JSON lines", format_count(invalid_json)],
        ["Missing title", format_count(missing_title)],
        ["Missing abstract", format_count(missing_abstract)],
        ["Missing text", format_count(missing_text)],
        ["Schema mismatches", format_count(schema_mismatches)],
        [
            "Exact duplicate title+abstract pairs",
            format_count(duplicate_title_abstract_records),
        ],
        [
            "Duplicate title+abstract groups",
            format_count(duplicate_title_abstract_groups),
        ],
        [
            "Abstracts shorter than 40 tokens",
            f"{format_count(short_abstracts)} ({format_pct(short_abstracts / doc_count * 100 if doc_count else 0.0)})",
        ],
        [
            "Missing-field rate",
            format_pct(missing_rate * 100),
        ],
    ]
    report_lines.append(markdown_table(["Check", "Result"], structural_rows))

    report_lines.extend(
        [
            "",
            "## Corpus Shape",
        ]
    )
    shape_rows = [
        ["Title tokens, median", f"{title_median:.0f}"],
        ["Abstract tokens, median", f"{abstract_median:.0f}"],
        ["Abstract tokens, p10 / p90", f"{abstract_p10:.0f} / {abstract_p90:.0f}"],
        ["Abstract tokens, max", f"{abstract_max:.0f}"],
        ["Text tokens, median", f"{text_median:.0f}"],
        ["Vocabulary size", format_count(vocab_size)],
        ["Unique primary categories", format_count(len(primary_category_counts))],
    ]
    report_lines.append(markdown_table(["Metric", "Value"], shape_rows))

    if yearly_counts:
        report_lines.extend(
            [
                "",
                "## Yearly Counts",
            ]
        )
        year_rows = [
            [
                str(year),
                format_count(actual),
                format_count(expected_years.get(year))
                if expected_years.get(year) is not None
                else "n/a",
            ]
            for year, actual in sorted(yearly_counts.items())
        ]
        report_lines.append(markdown_table(["Year", "Actual", "Manifest"], year_rows))

    if primary_category_counts:
        report_lines.extend(
            [
                "",
                "## Primary Categories",
            ]
        )
        category_rows = [
            [category, format_count(count)]
            for category, count in primary_category_counts.most_common(10)
        ]
        report_lines.append(markdown_table(["Category", "Docs"], category_rows))

    if year_keywords:
        report_lines.extend(
            [
                "",
                f"## Yearly Keywords Since {args.year_keyword_since}",
                "",
                "Keywords are ranked by how much a term is overrepresented in a year versus the full corpus.",
            ]
        )
        keyword_rows = [
            [
                str(year),
                ", ".join(f"{term} ({count})" for term, count, _score in terms),
            ]
            for year, terms in sorted(year_keywords.items())
        ]
        report_lines.append(markdown_table(["Year", "Keywords"], keyword_rows))

    report_lines.extend(
        [
            "",
            "## Top Terms",
        ]
    )
    top_term_rows = [
        [term, format_count(count)]
        for term, count in term_frequency.most_common(20)
    ]
    report_lines.append(markdown_table(["Term", "Count"], top_term_rows))

    report_lines.extend(
        [
            "",
            "## TF-IDF Smoke Test",
            "",
            "The scores below use the document title plus abstract as the searchable text.",
        ]
    )
    for query in queries:
        query_tokens = [token for token in tokenize(query) if token in document_frequency]
        if not query_tokens:
            report_lines.extend(
                [
                    "",
                    f"### `{query}`",
                    "",
                    "_No indexed query terms were found in the corpus._",
                ]
            )
            continue

        scores: Counter[int] = Counter()
        for term in query_tokens:
            idf = math.log((doc_count + 1) / (document_frequency[term] + 1)) + 1.0
            for doc_index, tf in query_postings.get(term, []):
                scores[doc_index] += (1.0 + math.log(tf)) * idf

        report_lines.extend(["", f"### `{query}`"])
        top_hits = scores.most_common(3)
        if not top_hits:
            report_lines.append("_No matching documents found._")
            continue

        hit_rows = []
        for doc_index, score in top_hits:
            meta = doc_meta[doc_index]
            hit_rows.append(
                [
                    f"{score:.3f}",
                    str(meta.get("year", "")),
                    str(meta.get("id", "")),
                    str(meta.get("title", "")),
                ]
            )
        report_lines.append(markdown_table(["Score", "Year", "ID", "Title"], hit_rows))

    report_lines.extend(
        [
            "",
            "## Quick Read",
            "",
            f"- Manifest match: {'yes' if manifest_total_match and not year_mismatches else 'no'}",
            f"- Structural anomalies: {'none' if not (invalid_json or schema_mismatches or missing_title or missing_abstract) else 'present'}",
            f"- Retrieval signal: {'present' if all(query_postings.get(term) for term in query_terms if term in document_frequency) else 'partial'}",
        ]
    )

    if sample_errors:
        report_lines.extend(["", "## Parse Errors", *[f"- {item}" for item in sample_errors]])
    if sample_schema_mismatch:
        report_lines.extend(
            ["", "## Schema Mismatch Samples", *[f"- {item}" for item in sample_schema_mismatch]]
        )
    if year_mismatches:
        report_lines.extend(["", "## Year Mismatches"])
        for year, info in year_mismatches.items():
            report_lines.append(
                f"- {year}: actual {format_count(info['actual'])}, manifest {format_count(info['manifest'])}"
            )

    report_lines.extend(
        [
            "",
            "## Performance",
        ]
    )
    performance_rows = [
        ["Wall time", format_seconds(wall_seconds)],
        [
            "CPU time",
            format_seconds(cpu_seconds) if cpu_seconds is not None else "n/a",
        ],
        [
            "CPU utilization",
            format_pct((cpu_seconds / wall_seconds) * 100)
            if cpu_seconds is not None and wall_seconds > 0
            else "n/a",
        ],
        ["Peak RSS", format_mib(peak_rss)],
    ]
    if resource and end_usage:
        performance_rows.extend(
            [
                ["Minor faults", format_count(end_usage.ru_minflt)],
                ["Major faults", format_count(end_usage.ru_majflt)],
                ["Voluntary context switches", format_count(end_usage.ru_nvcsw)],
                ["Involuntary context switches", format_count(end_usage.ru_nivcsw)],
                ["Input blocks", format_count(end_usage.ru_inblock)],
                ["Output blocks", format_count(end_usage.ru_oublock)],
            ]
        )
    report_lines.append(markdown_table(["Metric", "Value"], performance_rows))

    report = "\n".join(report_lines) + "\n"
    print(report, end="")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
