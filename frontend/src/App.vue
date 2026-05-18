<template>
  <main class="page-shell">
    <section class="hero-panel">
      <div class="hero-copy">
        <p class="eyebrow">Hadoop Text Retrieval</p>
        <h1>Search arXiv abstracts through a Hadoop-powered TF-IDF pipeline.</h1>
      </div>

      <div class="hero-status">
        <div class="status-badge" :class="overview?.ready ? 'is-ready' : 'is-waiting'">
          <span class="status-dot"></span>
          <span>{{ statusBadgeText }}</span>
        </div>
        <div class="dataset-summary">
          <p class="dataset-summary-label">Active dataset</p>
          <p class="dataset-summary-name">{{ activeDatasetName }}</p>
          <p class="dataset-summary-path">{{ activeDatasetPath || 'Dataset path is not available yet.' }}</p>
        </div>
        <p class="status-line">
          {{ healthText }}
        </p>
        <p class="status-line subtle">
          {{ buildStatusText }}
        </p>
        <div v-if="activeBuildProgress" class="build-progress-card">
          <div class="build-progress-head">
            <span>{{ progressStageLabel }}</span>
            <span>{{ activeBuildProgress.percent }}%</span>
          </div>
          <div class="progress-bar progress-bar-hero" aria-hidden="true">
            <div
              class="progress-bar-fill"
              :style="{ width: `${Math.max(activeBuildProgress.percent, 4)}%` }"
            ></div>
          </div>
          <p>{{ activeBuildProgress.message }}</p>
          <span>
            Step {{ activeBuildProgress.currentStep }} of {{ activeBuildProgress.totalSteps }} /
            {{ formatDuration(activeBuildProgress.elapsedMillis) }}
          </span>
        </div>
        <button class="ghost-button" :disabled="analysisPending || uploadPending" @click="runAnalysis">
          {{ analysisPending ? 'Analyzing...' : analysisButtonText }}
        </button>
      </div>
    </section>

    <section class="search-panel">
      <div class="search-heading">
        <p class="section-label">Dataset Setup</p>
        <h2>Use the default corpus or upload your own</h2>
      </div>

      <div v-if="uiError" class="error-strip">
        {{ uiError }}
      </div>

      <div class="upload-panel">
        <div class="upload-copy">
          <p class="section-label">Dataset Import</p>
          <p class="upload-hint">
            A default dataset is already available for this demo. Upload one or more `.json` or
            `.jsonl` files only if you want to replace it, then run analysis explicitly. The
            current upload limit is 1 GB per request.
          </p>
          <p class="upload-meta dataset-meta">
            {{ analysisScopeText }}
          </p>
        </div>
        <input
          ref="uploadInput"
          class="sr-only"
          type="file"
          accept=".json,.jsonl,application/json"
          multiple
          @change="handleUploadSelection"
        />
        <input
          ref="uploadFolderInput"
          class="sr-only"
          type="file"
          multiple
          webkitdirectory
          directory
          @change="handleUploadSelection"
        />
        <div class="upload-actions">
          <button type="button" class="secondary-button" @click="openUploadPicker">
            Choose files
          </button>
          <button type="button" class="secondary-button" @click="openUploadFolderPicker">
            Choose folder
          </button>
          <button
            type="button"
            class="primary-button"
            :disabled="uploadPending || !selectedUploadFiles.length"
            @click="uploadCorpus"
          >
            {{ uploadPending ? 'Uploading...' : 'Upload dataset' }}
          </button>
          <button
            type="button"
            class="ghost-button"
            :disabled="analysisPending || uploadPending"
            @click="runAnalysis"
          >
            {{ analysisPending ? 'Analyzing...' : analysisButtonText }}
          </button>
        </div>
        <p class="upload-meta">
          {{ selectedUploadFiles.length ? uploadSelectionText : 'No files selected.' }}
        </p>
        <div v-if="selectedUploadFiles.length" class="keyword-chip-row">
          <span
            v-for="file in selectedUploadFiles"
            :key="uploadFileKey(file)"
            class="keyword-chip secondary"
          >
            {{ uploadFileLabel(file) }}
          </span>
        </div>
      </div>

      <p v-if="!analysisReady" class="setup-note">
        {{ setupGuidanceText }}
      </p>

      <div v-if="uploadResult" class="success-strip">
        {{ uploadResult }}
      </div>

      <div v-if="uploadWarnings.length" class="warning-strip">
        <span v-for="warning in uploadWarnings" :key="warning">{{ warning }}</span>
      </div>

      <div v-if="searchWarnings.length" class="warning-strip">
        <span v-for="warning in searchWarnings" :key="warning">{{ warning }}</span>
      </div>
    </section>

    <template v-if="analysisReady">
      <section class="metrics-grid">
        <article v-for="card in summaryCards" :key="card.label" class="metric-card">
          <p class="metric-label">{{ card.label }}</p>
          <p class="metric-value">{{ card.value }}</p>
          <p class="metric-detail">{{ card.detail }}</p>
        </article>
      </section>

      <section class="content-grid">
        <article class="panel overview-panel full-span">
          <div class="panel-head">
            <div>
              <p class="section-label">Corpus Overview</p>
              <h2>Year distribution</h2>
            </div>
            <p class="panel-meta">{{ formatNumber(totalDocuments) }} docs</p>
          </div>

          <div v-if="overview?.years?.length" class="year-list">
            <div v-for="year in overview.years" :key="year.year" class="year-row">
              <div class="year-row-head">
                <span class="year-label">{{ year.year }}</span>
                <span class="year-count">{{ formatNumber(year.recordCount) }}</span>
              </div>
              <div class="bar-track">
                <div class="bar-fill" :style="{ width: yearWidth(year.recordCount) }"></div>
              </div>
              <div v-if="year.keywords?.length" class="keyword-chip-row">
                <span
                  v-for="keyword in year.keywords"
                  :key="`${year.year}-${keyword.term}`"
                  class="keyword-chip"
                >
                  {{ keyword.term }} ({{ keyword.score.toFixed(2) }})
                </span>
              </div>
              <p v-else class="year-keyword-empty">No distinctive yearly keywords.</p>
            </div>
          </div>
        </article>
      </section>

      <section class="search-panel">
        <div class="search-heading">
          <p class="section-label">Retrieval Console</p>
          <h2>Query the corpus</h2>
        </div>

        <form class="search-form" @submit.prevent="runSearch">
          <label class="field field-query">
            <span>Query</span>
            <input
              v-model="query"
              type="text"
              placeholder="graph neural network, federated learning, diffusion planning..."
            />
          </label>

          <label class="field">
            <span>Year</span>
            <select v-model="selectedYear">
              <option value="">All years</option>
              <option v-for="year in yearOptions" :key="year" :value="String(year)">
                {{ year }}
              </option>
            </select>
          </label>

          <label class="field">
            <span>Category</span>
            <select v-model="selectedCategory">
              <option value="">All categories</option>
              <option
                v-for="category in categoryOptions"
                :key="category.name"
                :value="category.name"
              >
                {{ category.name }}
              </option>
            </select>
          </label>

          <button class="primary-button" :disabled="searchPending">
            {{ searchPending ? 'Searching...' : 'Run Search' }}
          </button>
        </form>

        <div v-if="searchWarnings.length" class="warning-strip">
          <span v-for="warning in searchWarnings" :key="warning">{{ warning }}</span>
        </div>
      </section>

      <section class="content-grid results-grid">
        <article class="panel results-panel">
          <div class="panel-head">
            <div class="panel-title-block">
              <p class="section-label">Results</p>
              <h2>Ranked hits</h2>
              <p class="panel-caption">{{ resultsQueryText }}</p>
            </div>
            <p class="panel-meta">
              {{ searchResponse ? `${formatNumber(searchResponse.totalHits)} hits` : 'No search yet' }}
            </p>
          </div>

          <div v-if="searchPending" class="empty-state">Searching the corpus...</div>
          <div v-else-if="!searchResults.length" class="empty-state">
            Run a query to inspect ranked search results and document keywords.
          </div>
          <div v-else class="results-shell">
            <div class="scroll-progress">
              <div class="progress-copy">
                <span>{{ resultsCoverageText }}</span>
                <span>{{ Math.round(resultsScrollProgress) }}%</span>
              </div>
              <div class="progress-bar" aria-hidden="true">
                <div
                  class="progress-bar-fill"
                  :style="{ width: `${Math.max(resultsScrollProgress, 6)}%` }"
                ></div>
              </div>
            </div>
            <div ref="resultsScroller" class="results-scroll" @scroll="updateResultsScrollProgress">
              <button
                v-for="result in searchResults"
                :key="result.id"
                type="button"
                class="result-card"
                :class="{ active: selectedDocument?.document?.id === result.id }"
                @click="loadDocument(result.id)"
              >
                <div class="result-header">
                  <span class="result-year">{{ result.year }}</span>
                  <span class="result-score">score {{ result.score.toFixed(3) }}</span>
                </div>
                <h3>{{ result.title }}</h3>
                <p class="result-snippet">{{ result.abstractSnippet }}</p>
                <div class="result-meta">
                  <span>{{ result.primaryCategory }}</span>
                  <span>{{ result.authors || 'Unknown authors' }}</span>
                </div>
                <div class="keyword-chip-row">
                  <span
                    v-for="term in result.matchedTerms"
                    :key="`${result.id}-${term}`"
                    class="keyword-chip match"
                  >
                    {{ term }}
                  </span>
                  <span
                    v-for="keyword in result.keywords.slice(0, 4)"
                    :key="`${result.id}-${keyword.term}`"
                    class="keyword-chip secondary"
                  >
                    {{ keyword.term }}
                  </span>
                </div>
              </button>
            </div>
          </div>
        </article>

        <article class="panel detail-panel">
          <div class="panel-head">
            <div class="panel-title-block">
              <p class="section-label">Document Detail</p>
              <h2>{{ selectedDocument?.document?.title ?? 'Select a result' }}</h2>
              <p class="panel-caption">{{ detailSummaryText }}</p>
            </div>
            <p class="panel-meta">{{ detailPending ? 'Loading...' : selectedDocument?.status ?? 'idle' }}</p>
          </div>

          <div v-if="detailPending" class="empty-state">Loading document detail...</div>
          <div v-else-if="!selectedDocument?.ready" class="empty-state">
            Pick a result to inspect the stored document fields and extracted keywords.
          </div>
          <div v-else class="detail-scroll">
            <div class="detail-body">
              <div class="detail-meta-grid">
                <div>
                  <span class="detail-label">arXiv ID</span>
                  <p>{{ selectedDocument.document.id }}</p>
                </div>
                <div>
                  <span class="detail-label">Year / Month</span>
                  <p>{{ selectedDocument.document.year }} / {{ selectedDocument.document.month }}</p>
                </div>
                <div>
                  <span class="detail-label">Primary category</span>
                  <p>{{ selectedDocument.document.primaryCategory }}</p>
                </div>
                <div>
                  <span class="detail-label">Updated</span>
                  <p>{{ selectedDocument.document.updateDate || 'n/a' }}</p>
                </div>
              </div>

              <div class="detail-section">
                <span class="detail-label">Authors</span>
                <p>{{ selectedDocument.document.authors || 'Unknown authors' }}</p>
              </div>

              <div class="detail-section">
                <span class="detail-label">Categories</span>
                <div class="keyword-chip-row">
                  <span
                    v-for="category in selectedDocument.document.categories"
                    :key="category"
                    class="keyword-chip secondary"
                  >
                    {{ category }}
                  </span>
                </div>
              </div>

              <div class="detail-section">
                <span class="detail-label">Abstract</span>
                <p class="detail-abstract">{{ selectedDocument.document.abstractText }}</p>
              </div>

              <div class="detail-section">
                <span class="detail-label">Extracted keywords</span>
                <div class="keyword-chip-row">
                  <span
                    v-for="keyword in selectedDocument.keywords"
                    :key="`${selectedDocument.document.id}-${keyword.term}`"
                    class="keyword-chip"
                  >
                    {{ keyword.term }}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </article>
      </section>
    </template>
  </main>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue';

const query = ref('');
const selectedYear = ref('');
const selectedCategory = ref('');

const overview = ref(null);
const health = ref(null);
const searchResponse = ref(null);
const selectedDocument = ref(null);
const resultsScroller = ref(null);
const uploadInput = ref(null);
const uploadFolderInput = ref(null);

const searchPending = ref(false);
const detailPending = ref(false);
const analysisPending = ref(false);
const uploadPending = ref(false);
const uiError = ref('');
const resultsScrollProgress = ref(0);
const selectedUploadFiles = ref([]);
const uploadResult = ref('');
const uploadWarnings = ref([]);
let analysisPollRun = 0;

const yearOptions = computed(() =>
  (overview.value?.years ?? []).map((entry) => entry.year).sort((left, right) => right - left),
);

const categoryOptions = computed(() => overview.value?.topCategories ?? []);

const totalDocuments = computed(() => overview.value?.recordCount ?? 0);

const maxYearCount = computed(() =>
  Math.max(...(overview.value?.years ?? []).map((entry) => entry.recordCount), 1),
);

const searchResults = computed(() => searchResponse.value?.results ?? []);

const searchWarnings = computed(() => searchResponse.value?.warnings ?? []);

const activeDatasetPath = computed(
  () => overview.value?.datasetDir || health.value?.corpus?.build?.datasetDir || '',
);

const activeDatasetName = computed(() => {
  if (overview.value?.datasetName) {
    return overview.value.datasetName;
  }
  return datasetNameFromPath(activeDatasetPath.value) || 'No dataset selected';
});

const analysisReady = computed(() => overview.value?.ready ?? false);

const activeBuildProgress = computed(
  () => overview.value?.build?.progress ?? health.value?.corpus?.build?.progress ?? null,
);

const progressStageLabel = computed(() => {
  const stage = activeBuildProgress.value?.stage;
  const labels = {
    queued: 'Queued',
    starting: 'Starting',
    connect: 'Connecting',
    'stage-input': 'Staging input',
    'term-statistics': 'Term statistics',
    'scored-terms': 'TF-IDF and index',
    'document-keywords': 'Document keywords',
    snapshot: 'Loading snapshot',
    complete: 'Complete',
  };
  return labels[stage] ?? stage ?? 'Running';
});

const statusBadgeText = computed(() => {
  const status = overview.value?.build?.status;
  if (overview.value?.ready) {
    return 'Index ready';
  }
  if (status === 'staged') {
    return 'Dataset staged';
  }
  if (status === 'reloading') {
    return 'Analysis running';
  }
  return 'Waiting for analysis';
});

const analysisButtonText = computed(() =>
  overview.value?.ready ? 'Re-run Analysis' : 'Run Analysis',
);

const healthText = computed(() => {
  if (!health.value) {
    return 'Backend status is loading.';
  }
  const corpus = health.value.corpus;
  const buildMillis = corpus?.build?.buildMillis ?? 0;
  return `Backend ${health.value.status}; corpus ${corpus?.ready ? 'online' : 'not ready'}; active dataset ${activeDatasetName.value}; last completed build ${buildMillis} ms.`;
});

const buildStatusText = computed(() => {
  const build = overview.value?.build;
  if (!build) {
    return 'Corpus build metadata is loading.';
  }
  if (build.status === 'staged') {
    return `Dataset "${activeDatasetName.value}" is staged. Submit analysis to build the search index.`;
  }
  if (build.status === 'not-analyzed') {
    return `No analysis has run yet for "${activeDatasetName.value}". Submit analysis to build the search index.`;
  }
  if (build.status === 'reloading') {
    return activeBuildProgress.value?.message
      ?? `Background analysis is running for "${activeDatasetName.value}".`;
  }
  if (build.status === 'reload-failed') {
    return build.warnings?.at(-1) ?? 'The last background reload failed.';
  }
  return `Dataset "${activeDatasetName.value}" index status ${build.status}; vocabulary ${formatNumber(build.vocabularySize)}; postings ${formatNumber(build.indexedPostingCount)}.`;
});

const analysisScopeText = computed(() => {
  if (!activeDatasetPath.value) {
    return 'No dataset is configured yet. Upload a dataset before running analysis.';
  }
  return `Analysis is currently scoped to "${activeDatasetName.value}" at ${activeDatasetPath.value}.`;
});

const setupGuidanceText = computed(() => {
  if (activeDatasetPath.value) {
    return `The default dataset "${activeDatasetName.value}" is already available. Run analysis to unlock corpus statistics and retrieval, or upload a replacement dataset first.`;
  }
  return 'Upload a dataset and run analysis before corpus statistics and ranked retrieval become available.';
});

const resultsQueryText = computed(() => {
  const currentQuery = searchResponse.value?.query?.trim() || query.value.trim();
  return currentQuery ? `Query: ${currentQuery}` : 'Query: not set';
});

const detailSummaryText = computed(() => {
  if (!selectedDocument.value?.ready) {
    return 'Choose a result from the left window.';
  }
  const document = selectedDocument.value.document;
  return `${document.year} / ${document.primaryCategory} / ${document.id}`;
});

const resultsCoverageText = computed(() => {
  if (!searchResponse.value) {
    return 'Waiting for a query.';
  }
  const displayed = searchResults.value.length;
  const totalHits = searchResponse.value.totalHits ?? displayed;
  return `Showing ${formatNumber(displayed)} of ${formatNumber(totalHits)} hits`;
});

const uploadSelectionText = computed(() => {
  const fileCount = selectedUploadFiles.value.length;
  const totalBytes = selectedUploadFiles.value.reduce((sum, file) => sum + file.size, 0);
  return `${fileCount} file${fileCount > 1 ? 's' : ''} selected / ${formatNumber(totalBytes)} bytes`;
});

const summaryCards = computed(() => {
  const build = overview.value?.build;
  return [
    {
      label: 'Corpus size',
      value: formatNumber(overview.value?.recordCount ?? 0),
      detail: `${overview.value?.minYear ?? 'n/a'} to ${overview.value?.maxYear ?? 'n/a'}`,
    },
    {
      label: 'Vocabulary',
      value: formatNumber(build?.vocabularySize ?? 0),
      detail: `${formatNumber(build?.indexedTermCount ?? 0)} indexed terms`,
    },
    {
      label: 'Postings',
      value: formatNumber(build?.indexedPostingCount ?? 0),
      detail: 'Local inverted index footprint',
    },
    {
      label: 'Build time',
      value: `${build?.buildMillis ?? 0} ms`,
      detail: build?.status ?? 'unknown',
    },
  ];
});

function formatNumber(value) {
  return new Intl.NumberFormat('en-US').format(value ?? 0);
}

function formatDuration(milliseconds) {
  const totalSeconds = Math.max(0, Math.round((milliseconds ?? 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes <= 0) {
    return `${seconds}s elapsed`;
  }
  return `${minutes}m ${seconds.toString().padStart(2, '0')}s elapsed`;
}

function datasetNameFromPath(datasetPath) {
  if (!datasetPath) {
    return '';
  }
  const segments = datasetPath.split(/[\\/]/).filter(Boolean);
  return segments.at(-1) ?? datasetPath;
}

function yearWidth(recordCount) {
  return `${Math.max(8, (recordCount / maxYearCount.value) * 100)}%`;
}

function updateResultsScrollProgress() {
  const element = resultsScroller.value;
  if (!element) {
    resultsScrollProgress.value = 0;
    return;
  }
  const maxScroll = element.scrollHeight - element.clientHeight;
  if (maxScroll <= 0) {
    resultsScrollProgress.value = searchResults.value.length ? 100 : 0;
    return;
  }
  resultsScrollProgress.value = Math.min(100, (element.scrollTop / maxScroll) * 100);
}

async function resetResultsScroller() {
  await nextTick();
  if (resultsScroller.value) {
    resultsScroller.value.scrollTop = 0;
  }
  updateResultsScrollProgress();
}

const apiBase = (import.meta.env.VITE_API_BASE?.trim() ?? '').replace(/\/$/, '');

function apiUrl(path) {
  const normalized = path.startsWith('/') ? path : `/${path}`;
  return apiBase ? `${apiBase}${normalized}` : normalized;
}

async function apiFetch(path, options = {}) {
  const headers = {
    ...(options.headers ?? {}),
  };
  if (!(options.body instanceof FormData) && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(apiUrl(path), {
    headers,
    ...options,
  });
  if (!response.ok) {
    const contentType = response.headers.get('content-type') ?? '';
    let message = `Request failed: ${response.status}`;
    if (contentType.includes('application/json')) {
      const payload = await response.json();
      message = payload.message || payload.error || message;
    } else {
      const text = await response.text();
      if (text.trim()) {
        message = text.trim();
      }
    }
    throw new Error(message);
  }
  return response.json();
}

async function loadDashboard({ throwOnError = false } = {}) {
  try {
    const [healthData, overviewData] = await Promise.all([
      apiFetch('/api/health'),
      apiFetch('/api/overview'),
    ]);
    health.value = healthData;
    overview.value = overviewData;
    return overviewData;
  } catch (error) {
    uiError.value = error instanceof Error ? error.message : 'Failed to load dashboard data.';
    if (throwOnError) {
      throw error;
    }
    return null;
  }
}

function openUploadPicker() {
  uploadInput.value?.click();
}

function openUploadFolderPicker() {
  uploadFolderInput.value?.click();
}

function isUploadFileSupported(file) {
  const lowercaseName = file.name.toLowerCase();
  return lowercaseName.endsWith('.json') || lowercaseName.endsWith('.jsonl');
}

function uploadFileLabel(file) {
  return file.webkitRelativePath || file.name;
}

function uploadFileKey(file) {
  return `${uploadFileLabel(file)}-${file.size}`;
}

function handleUploadSelection(event) {
  const files = Array.from(event.target.files ?? []);
  const supportedFiles = files.filter(isUploadFileSupported);
  const skippedCount = files.length - supportedFiles.length;

  selectedUploadFiles.value = supportedFiles;
  selectedDocument.value = null;
  searchResponse.value = null;
  resultsScrollProgress.value = 0;
  uploadResult.value = '';
  uploadWarnings.value = skippedCount
    ? [`Skipped ${skippedCount} non-JSON file(s). Only .json and .jsonl are uploaded.`]
    : [];
}

function resetUploadSelection() {
  selectedUploadFiles.value = [];
  if (uploadInput.value) {
    uploadInput.value.value = '';
  }
  if (uploadFolderInput.value) {
    uploadFolderInput.value.value = '';
  }
}

async function runSearch() {
  searchPending.value = true;
  selectedDocument.value = null;
  try {
    uiError.value = '';
    const params = new URLSearchParams();
    params.set('q', query.value);
    if (selectedYear.value) {
      params.set('year', selectedYear.value);
    }
    if (selectedCategory.value) {
      params.set('category', selectedCategory.value);
    }
    const response = await apiFetch(`/api/search?${params.toString()}`);
    searchResponse.value = response;
    await resetResultsScroller();
    if (response.results?.length) {
      await loadDocument(response.results[0].id);
    }
  } catch (error) {
    uiError.value = error instanceof Error ? error.message : 'Search request failed.';
  } finally {
    searchPending.value = false;
  }
}

async function loadDocument(documentId) {
  detailPending.value = true;
  try {
    uiError.value = '';
    selectedDocument.value = await apiFetch(`/api/documents/${documentId}`);
  } catch (error) {
    uiError.value = error instanceof Error ? error.message : 'Failed to load document detail.';
  } finally {
    detailPending.value = false;
  }
}

async function uploadCorpus() {
  if (!selectedUploadFiles.value.length) {
    return;
  }
  uploadPending.value = true;
  uploadResult.value = '';
  uploadWarnings.value = [];
  try {
    uiError.value = '';
    const formData = new FormData();
    selectedUploadFiles.value.forEach((file) => formData.append('files', file));
    const response = await apiFetch('/api/corpus/upload', {
      method: 'POST',
      body: formData,
    });
    uploadResult.value = `Staged ${formatNumber(response.importedRecordCount)} records from ${formatNumber(response.fileCount)} file(s). Run analysis to build the index.`;
    uploadWarnings.value = response.warnings ?? [];
    resetUploadSelection();
    selectedDocument.value = null;
    searchResponse.value = null;
    resultsScrollProgress.value = 0;
    await loadDashboard({ throwOnError: true });
  } catch (error) {
    uiError.value = error instanceof Error ? error.message : 'Failed to upload dataset files.';
  } finally {
    uploadPending.value = false;
  }
}

async function runAnalysis() {
  analysisPending.value = true;
  try {
    uiError.value = '';
    const response = await apiFetch('/api/corpus/analyze', { method: 'POST' });
    await loadDashboard({ throwOnError: true });
    if (response.status === 'reloading' || overview.value?.build?.status === 'reloading') {
      void pollAnalysisCompletion();
      return;
    }
  } catch (error) {
    uiError.value = error instanceof Error ? error.message : 'Failed to run corpus analysis.';
  } finally {
    if (overview.value?.build?.status !== 'reloading') {
      analysisPending.value = false;
    }
  }
}

async function pollAnalysisCompletion() {
  const pollRun = ++analysisPollRun;
  while (analysisPending.value && pollRun === analysisPollRun) {
    await new Promise((resolve) => window.setTimeout(resolve, 1200));
    try {
      await loadDashboard({ throwOnError: true });
    } catch {
      analysisPending.value = false;
      return;
    }
    if (overview.value?.build?.status === 'reloading') {
      continue;
    }
    analysisPending.value = false;
  }
}

onMounted(async () => {
  window.addEventListener('resize', updateResultsScrollProgress);
  await loadDashboard();
});

onUnmounted(() => {
  analysisPollRun += 1;
  window.removeEventListener('resize', updateResultsScrollProgress);
});
</script>
