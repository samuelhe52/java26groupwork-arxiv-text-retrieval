# Hadoop Text Retrieval System

[中文](README.zh-CN.md)

This project is a Hadoop/YARN-based text retrieval application for arXiv abstract data. Its scope is to process a domain-specific text corpus, compute TF-IDF scores, extract representative keywords, build an inverted index, and expose search and result views through a web application.

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

## Local Development

For normal backend development, use the default `local` mode. This does not require HDFS, YARN, or a running cluster.

```bash
make backend-run
```

In this mode, the backend reads the dataset directly and builds the TF-IDF search snapshot in-process, without writing Hadoop job artifacts to a local temp tree.

## Cluster Testing

When you want to test against a real Hadoop cluster, set `HADOOP_CONF_DIR` first and run:

```bash
export HADOOP_CONF_DIR=/path/to/etc/hadoop
make backend-run-cluster
```

## Current Data Preparation Scripts

- `scripts/assemble_arxiv_snapshot_dataset.py`: builds a local arXiv `cs.LG` dataset with a backend-ready merged `upload.jsonl` plus optional yearly shards
- `scripts/cleanup_arxiv_dataset_primary_cs.py`: derives a stricter primary-`cs.*` subset and writes the backend-ready merged `upload.jsonl`
- `scripts/preflight_arxiv_dataset.py`: runs Python-only corpus checks, yearly keywords since 2019, and TF-IDF smoke tests before any Java or Hadoop work
