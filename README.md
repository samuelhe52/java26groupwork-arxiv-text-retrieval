# Hadoop Text Retrieval System

[中文](README.zh-CN.md)

This project is a Hadoop/YARN-based text retrieval application for the Java final assignment. Its scope is to process a domain-specific text corpus, compute TF-IDF scores, extract representative keywords, build an inverted index, and expose search and result views through a web application.

The corpus is based on arXiv abstract data, with local scripts in this repo used to prepare category-filtered JSONL datasets before they are uploaded to HDFS. The application is intended to show how distributed storage, distributed computation, and a Java web backend can work together in one retrieval workflow.

## Locked Stack

- Backend: Spring Boot 3.4.1 on Java 17
- Backend build: Maven
- Backend entrypoint: `backend/mvnw`
- Hadoop client: 3.4.1
- Frontend: Vue 3 + Vite
- Frontend language: JavaScript
- Frontend runtime: Node 20 LTS
- Package namespace: `com.java26groupwork.finalassignment`
- Dataset scripts: repo-relative by default, with environment variable overrides
- Collaboration decisions: [docs/PROJECT_DECISIONS.md](docs/PROJECT_DECISIONS.md)

## What TF-IDF Means

TF-IDF stands for `Term Frequency - Inverse Document Frequency`.

- `TF` measures how often a word appears in one document.
- `IDF` measures how rare or distinctive that word is across the whole corpus.
- A high TF-IDF score usually means the word is important to that document, but not so common that it appears everywhere.

In this project, TF-IDF is used to identify document keywords and to support ranking in search results.

## Project Scope

- store the text corpus in HDFS
- run MapReduce jobs on YARN for TF, DF, and TF-IDF computation
- extract top keywords for each document
- build an inverted index for keyword-based retrieval
- provide a web interface for search, corpus overview, and result display

## Repository Structure

- `backend/`: Spring Boot backend for APIs, job orchestration, and retrieval logic
- `frontend/`: Vue 3 + Vite frontend for search and visualization pages
- `datasets/`: local dataset snapshots and processed corpus files
- `scripts/`: data preparation helpers for assembling and cleaning arXiv-based datasets
- `Makefile`: top-level shortcuts for backend, frontend, and dataset archive tasks

## Backend Environment

The backend supports two Hadoop modes:

- `local`: no cluster required; uses the local filesystem for development
- `cluster`: loads real Hadoop client XML config and connects to HDFS/YARN

The backend resolves the Hadoop config directory in this order:

1. `HADOOP_CONF_DIR`
2. `HADOOP_HOME/etc/hadoop`
3. `${user.home}/hadoop/etc/hadoop`

This keeps the repo portable across macOS, Linux, and WSL. Each team member can point the backend at their own Hadoop installation without editing tracked files.

Example environment setup:

```bash
# macOS Orb
export HADOOP_CONF_DIR=/Users/<your-name>/OrbStack/ubuntu/opt/hadoop-3.4.1/etc/hadoop

# Linux or WSL
export HADOOP_CONF_DIR=/opt/hadoop-3.4.1/etc/hadoop
```

## Local Development

For normal backend development, use the default `local` mode. This does not require HDFS, YARN, or a running cluster.

```bash
make backend-run
```

In this mode, the backend uses `file:///` and keeps the Hadoop integration path simple while the web/API code is still being built.

## Cluster Testing

When you want to test against a real Hadoop cluster, set `HADOOP_CONF_DIR` first and run the backend with the `cluster` Spring profile:

```bash
cd backend
HADOOP_CONF_DIR=/path/to/etc/hadoop ./mvnw -q spring-boot:run -Dspring-boot.run.profiles=cluster
```

In `cluster` mode, the backend loads:

- `core-site.xml`
- `hdfs-site.xml`
- `yarn-site.xml` if present

That means active NameNode discovery, HDFS HA behavior, and YARN HA behavior come from the cluster's own Hadoop client config instead of being hardcoded in the application.

## Current Data Preparation Scripts

- `scripts/assemble_arxiv_snapshot_dataset.py`: builds a local arXiv `cs.LG` dataset organized by year
- `scripts/cleanup_arxiv_dataset_primary_cs.py`: derives a stricter primary-`cs.*` subset from the assembled dataset
- `scripts/preflight_arxiv_dataset.py`: runs Python-only corpus checks, yearly keywords since 2019, and TF-IDF smoke tests before any Java or Hadoop work

## Common Commands

```bash
make backend-build
make backend-run
make backend-test
make dataset-preflight
make frontend-install
make frontend-dev
make frontend-build
make archive-primary
make archive-primary-only
```
