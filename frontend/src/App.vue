<template>
  <main class="shell">
    <section class="hero">
      <p class="eyebrow">Final Assignment</p>
      <h1>Vue 3 web UI scaffold</h1>
      <p class="lede">
        This interface will sit on top of the Spring Boot backend and the Hadoop processing module.
      </p>
      <div class="status">{{ statusText }}</div>
    </section>

    <section class="grid">
      <article class="card">
        <h2>Frontend</h2>
        <p>Vue 3 and Vite for search, upload, and result views.</p>
      </article>
      <article class="card">
        <h2>Backend</h2>
        <p>Spring Boot API layer with a placeholder health route.</p>
      </article>
      <article class="card">
        <h2>Hadoop</h2>
        <p>Reserved for HDFS and YARN orchestration and job execution.</p>
      </article>
    </section>
  </main>
</template>

<script setup>
import { onMounted, ref } from 'vue';

const statusText = ref('Backend status: checking...');

onMounted(async () => {
  try {
    const response = await fetch('/api/health');
    const data = await response.json();
    statusText.value = `Backend status: ${data.status} | module: ${data.processing}`;
  } catch {
    statusText.value = 'Backend status: unavailable';
  }
});
</script>
