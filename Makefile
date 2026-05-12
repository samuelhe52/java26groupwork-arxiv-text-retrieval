.PHONY: backend-build backend-run backend-test frontend-install frontend-dev frontend-build archive-primary archive-primary-only dataset-preflight clean

BACKEND_DIR := backend
FRONTEND_DIR := frontend
DATASETS_DIR := datasets
DATASET_DIR ?= datasets/arxiv-cs-lg-2015-now-primary-cs-only
DESKTOP_DIR := $(HOME)/Desktop

backend-build:
	$(MAKE) -C $(BACKEND_DIR) build

backend-run:
	$(MAKE) -C $(BACKEND_DIR) run

backend-test:
	$(MAKE) -C $(BACKEND_DIR) test

frontend-install:
	$(MAKE) -C $(FRONTEND_DIR) install

frontend-dev:
	$(MAKE) -C $(FRONTEND_DIR) dev

frontend-build:
	$(MAKE) -C $(FRONTEND_DIR) build

archive-primary:
	7z a -t7z -mx=5 -aoa $(DESKTOP_DIR)/arxiv-cs-lg-2015-now.7z $(DATASETS_DIR)/arxiv-cs-lg-2015-now

archive-primary-only:
	7z a -t7z -mx=5 -aoa $(DESKTOP_DIR)/arxiv-cs-lg-2015-now-primary-cs-only.7z $(DATASETS_DIR)/arxiv-cs-lg-2015-now-primary-cs-only

dataset-preflight:
	python3 scripts/preflight_arxiv_dataset.py --dataset-dir $(DATASET_DIR)

clean:
	$(MAKE) -C $(BACKEND_DIR) clean
	$(MAKE) -C $(FRONTEND_DIR) clean
