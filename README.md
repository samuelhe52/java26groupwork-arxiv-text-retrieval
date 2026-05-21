# Hadoop arXiv Text Retrieval

[中文](README.zh-CN.md)

A Hadoop/YARN text retrieval system for arXiv abstracts with TF-IDF indexing, keyword extraction, and a Spring Boot + Vue web UI.

## Stack

- Backend: Spring Boot 3.4.1 on Java 17
- Hadoop client: 3.4.1
- Frontend: Vue 3 + Vite
- Build tools: Maven and npm

## Repository Structure

- `backend/`: Spring Boot API, retrieval logic, and Hadoop integration
- `frontend/`: Vue web UI for search and corpus views
- `scripts/`: dataset assembly, cleanup, and preflight helpers
- `docs/`: project notes and Hadoop pipeline guides

## Hadoop Modes

- `local`: builds the search snapshot directly from the dataset for normal development
- `cluster`: connects to real HDFS/YARN using Hadoop client XML config

The backend resolves Hadoop config from:

1. `HADOOP_CONF_DIR`
2. `HADOOP_HOME/etc/hadoop`
3. `${user.home}/hadoop/etc/hadoop`

## Development

```bash
make backend-run
make backend-test
make frontend-install
make frontend-dev
make frontend-build
```

## Cluster Run

```bash
export HADOOP_CONF_DIR=/path/to/etc/hadoop
make backend-run-cluster
```

## Dataset Scripts

- `scripts/assemble_arxiv_snapshot_dataset.py`: assemble an arXiv snapshot dataset
- `scripts/cleanup_arxiv_dataset_primary_cs.py`: derive a stricter primary-`cs.*` subset
- `scripts/preflight_arxiv_dataset.py`: run dataset checks and TF-IDF smoke tests
