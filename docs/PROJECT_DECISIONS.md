# Project Decisions

These choices are locked for collaboration unless the team explicitly changes them later.

## Stack

- Backend: Spring Boot 3.4.1
- Backend language: Java 17
- Backend build tool: Maven
- Backend wrapper: `backend/mvnw`
- Hadoop client: 3.4.1
- Frontend: Vue 3 + Vite
- Frontend language: JavaScript
- Frontend package manager: npm
- Frontend runtime: Node 20 LTS

## Repo Layout

- `backend/`: Spring Boot API and Hadoop integration shell
- `frontend/`: Vue 3 web UI
- `datasets/`: local corpus snapshots and derivatives, ignored by git
- `scripts/`: dataset assembly and cleanup helpers

## Collaboration Conventions

- Java package namespace: `com.java26groupwork.finalassignment`
- Build entrypoints: root `Makefile`, plus per-module `Makefile` files
- Local dataset scripts default to repo-relative paths
- Path overrides are available through environment variables
- No Docker or TypeScript in the initial scaffold
- Use LF line endings for text files
