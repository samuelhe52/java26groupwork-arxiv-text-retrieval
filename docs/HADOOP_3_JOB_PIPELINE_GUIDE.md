# Hadoop 3-Job Pipeline Guide

This note is a code-reading guide for teammates. It explains how the current Hadoop pipeline is organized, why it is now 3 jobs instead of 5, and which files to read first.

## 1. High-level flow

Current cluster-mode pipeline:

```text
staged input JSONL
    -> Job 1: TermStatisticsJob
    -> Job 2: ScoredTermsJob
    -> Job 3: DocumentKeywordsJob
    -> CorpusIndexService rebuilds the in-memory search snapshot
```

More concretely:

```text
input
  -> term-statistics/{tf, df}
  -> scored-terms/{tfidf, index}
  -> keywords
  -> backend search snapshot
```

The orchestration entry point is:

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/HadoopProcessingService.java`

The 3-job flow is wired in `processDataset(...)`.

## 2. Why it is now 3 jobs

The older version had 5 separate jobs:

1. `TermFrequencyJob`
2. `DocumentFrequencyJob`
3. `TfIdfJob`
4. `InvertedIndexJob`
5. `DocumentKeywordsJob`

Now the first two are merged, and the middle two are merged:

1. `TermStatisticsJob`
   Produces both `tf` and `df` in one MapReduce pass.
2. `ScoredTermsJob`
   Reads `tf` and `df`, then produces both `tfidf` and `index` in one reduce pass.
3. `DocumentKeywordsJob`
   Still reads `tfidf` and extracts top keywords for each document.

So the visible artifacts used by the backend are still the same logical outputs:

- `tf`
- `df`
- `tfidf`
- `index`
- `keywords`

What changed is only how those artifacts are produced internally.

## 3. Job 1: `TermStatisticsJob`

File:

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/jobs/TermStatisticsJob.java`

Purpose:

- Read staged JSONL documents.
- Tokenize each document.
- Count term frequency inside each document.
- Count document frequency across the corpus.

Output directories:

- `term-statistics/tf`
- `term-statistics/df`

How it works:

- Mapper parses one document and builds a local `termCounts` map.
- For each term, mapper emits:
  - `TF\t<docId>\t<term>` -> count
  - `DF\t<term>` -> `1`
- Reducer sums values and writes to different directories using `MultipleOutputs`.

Important detail:

- This job writes the same line shapes that the rest of the backend already expects:
  - `tf`: `<docId>\t<term>\t<count>`
  - `df`: `<term>\t<count>`

## 4. Job 2: `ScoredTermsJob`

File:

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/jobs/ScoredTermsJob.java`

Purpose:

- Join `tf` and `df`.
- Compute TF-IDF score for each `(term, document)` pair.
- Build the inverted index entries at the same time.

Input:

- `term-statistics/tf`
- `term-statistics/df`

Output:

- `scored-terms/tfidf`
- `scored-terms/index`

How it works:

- It uses `MultipleInputs`.
- One mapper reads TF rows and rewrites them into values keyed by `term`.
- Another mapper reads DF rows and also keys them by `term`.
- Reducer receives all TF rows and the DF row for one term.
- Reducer computes IDF once, then computes TF-IDF for each document containing that term.
- Reducer writes:
  - all TF-IDF rows to `tfidf`
  - only allowed postings to `index`

Important detail:

- `index` is filtered by `app.corpus.max-document-frequency-ratio`.
- Very common words can still appear in `tfidf`, but they may be excluded from the inverted index.

Output line shapes:

- `tfidf`: `<term>\t<docId>\t<score>`
- `index`: `<term>\t<docId>\t<score>`

## 5. Job 3: `DocumentKeywordsJob`

File:

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/jobs/DocumentKeywordsJob.java`

Purpose:

- Read all TF-IDF rows.
- Group them by document.
- Keep only top K keywords for each document.

Why this job is still separate:

- `ScoredTermsJob` is term-centered.
- `DocumentKeywordsJob` is document-centered.
- Merging it again would make the reducer logic more awkward and much harder to read.

Output line shape:

- `keywords`: `<docId>\tterm1|score1,term2|score2,...`

## 6. How the backend uses these outputs

After Hadoop jobs finish, the backend reads those artifact directories and rebuilds the in-memory search snapshot.

Main file:

- `backend/src/main/java/com/java26groupwork/finalassignment/corpus/CorpusIndexService.java`

Useful reading point:

- `buildSnapshot(...)`

What it reads:

- staged input documents
- `df`
- `keywords`
- `tf`
- `index`

Why `tf` is still needed after `tfidf` and `index` already exist:

- The backend still uses `tf` to rebuild aggregate statistics such as:
  - top corpus terms
  - yearly term summaries
  - yearly token totals

So the pipeline is not only building a search index. It is also building the metadata used by the API and frontend summary views.

## 7. Recommended reading order

If you want to understand the code quickly, read in this order:

1. `HadoopProcessingService`
   Understand the full orchestration and artifact paths first.
2. `TermStatisticsJob`
   Understand how `tf` and `df` are produced.
3. `ScoredTermsJob`
   Understand how `tfidf` and `index` are produced.
4. `DocumentKeywordsJob`
   Understand how document keywords are selected.
5. `CorpusIndexService.buildSnapshot(...)`
   Understand how Hadoop outputs become the runtime search snapshot.

## 8. One-sentence mental model

- Job 1 counts terms, Job 2 scores terms and builds postings, Job 3 converts scores into per-document keyword summaries.
