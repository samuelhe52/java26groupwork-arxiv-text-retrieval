# Hadoop 三任务流水线阅读指南

这份说明是给组员看的代码阅读指南。它解释了当前 Hadoop 流水线的结构、为什么现在是 3 个任务而不是 5 个任务，以及应该先读哪些文件。

## 1. 总体流程

当前 cluster 模式下的流水线：

```text
staged input JSONL
    -> Job 1: TermStatisticsJob
    -> Job 2: ScoredTermsJob
    -> Job 3: DocumentKeywordsJob
    -> CorpusIndexService rebuilds the in-memory search snapshot
```

更具体一点：

```text
input
  -> term-statistics/{tf, df}
  -> scored-terms/{tfidf, index}
  -> keywords
  -> backend search snapshot
```

流水线调度入口在：

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/HadoopProcessingService.java`

`processDataset(...)` 里串起了这 3 个任务。

## 2. 为什么现在是 3 个任务

旧版本有 5 个独立任务：

1. `TermFrequencyJob`
2. `DocumentFrequencyJob`
3. `TfIdfJob`
4. `InvertedIndexJob`
5. `DocumentKeywordsJob`

现在前两个被合并了，中间两个也被合并了：

1. `TermStatisticsJob`
   一次 MapReduce 同时产出 `tf` 和 `df`。
2. `ScoredTermsJob`
   读取 `tf` 和 `df`，再一次性产出 `tfidf` 和 `index`。
3. `DocumentKeywordsJob`
   继续读取 `tfidf`，为每篇文档提取关键词。

所以从后端消费角度看，逻辑产物没有变，仍然是：

- `tf`
- `df`
- `tfidf`
- `index`
- `keywords`

变化的是这些产物在内部是如何生成的，而不是后端最终依赖的逻辑结果。

## 3. 任务 1：`TermStatisticsJob`

文件：

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/jobs/TermStatisticsJob.java`

作用：

- 读取已经整理好的 JSONL 文档。
- 对每篇文档分词。
- 统计词在单篇文档中的出现次数。
- 统计词在整个语料中出现于多少篇文档。

输出目录：

- `term-statistics/tf`
- `term-statistics/df`

工作方式：

- Mapper 先解析一篇文档，并构建这篇文档内部的 `termCounts`。
- 对每个词，mapper 会发出两类键值：
  - `TF\t<docId>\t<term>` -> count
  - `DF\t<term>` -> `1`
- Reducer 把值求和，再通过 `MultipleOutputs` 分别写入不同目录。

重要细节：

- 这个任务保持了后端原先就依赖的文本格式：
  - `tf`: `<docId>\t<term>\t<count>`
  - `df`: `<term>\t<count>`

## 4. 任务 2：`ScoredTermsJob`

文件：

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/jobs/ScoredTermsJob.java`

作用：

- 把 `tf` 和 `df` 连接起来。
- 为每个 `(term, document)` 对计算 TF-IDF 分数。
- 同时生成倒排索引条目。

输入：

- `term-statistics/tf`
- `term-statistics/df`

输出：

- `scored-terms/tfidf`
- `scored-terms/index`

工作方式：

- 它使用 `MultipleInputs`。
- 一个 mapper 读取 TF 行，并把它们改写成按 `term` 分组的值。
- 另一个 mapper 读取 DF 行，也按 `term` 分组。
- Reducer 会同时拿到某个词对应的所有 TF 行和那一行 DF。
- Reducer 先算一次 IDF，再为包含该词的每篇文档计算 TF-IDF。
- Reducer 最后写出：
  - 所有 TF-IDF 行到 `tfidf`
  - 满足条件的 posting 行到 `index`

重要细节：

- `index` 会受 `app.corpus.max-document-frequency-ratio` 过滤。
- 很常见的词仍然可能出现在 `tfidf` 里，但会被排除在倒排索引之外。

输出文本格式：

- `tfidf`: `<term>\t<docId>\t<score>`
- `index`: `<term>\t<docId>\t<score>`

## 5. 任务 3：`DocumentKeywordsJob`

文件：

- `backend/src/main/java/com/java26groupwork/finalassignment/hadoop/jobs/DocumentKeywordsJob.java`

作用：

- 读取全部 TF-IDF 行。
- 按文档分组。
- 为每篇文档只保留分数最高的前 K 个关键词。

为什么这个任务仍然单独保留：

- `ScoredTermsJob` 的核心分组维度是词。
- `DocumentKeywordsJob` 的核心分组维度是文档。
- 如果再继续强行合并，reducer 逻辑会明显更绕，也更难读。

输出文本格式：

- `keywords`: `<docId>\tterm1|score1,term2|score2,...`

## 6. 后端如何消费这些输出

Hadoop 任务完成后，后端会读取这些产物目录，并重建内存中的搜索快照。

核心文件：

- `backend/src/main/java/com/java26groupwork/finalassignment/corpus/CorpusIndexService.java`

建议重点看的位置：

- `buildSnapshot(...)`

它会读取：

- 整理后的输入文档
- `df`
- `keywords`
- `tf`
- `index`

为什么已经有了 `tfidf` 和 `index`，还要继续读取 `tf`：

- 后端仍然需要 `tf` 来重建一些聚合统计，例如：
  - 全语料高频词
  - 每年关键词摘要
  - 每年 token 总量

所以这条流水线不仅是在建搜索索引，也是在建 API 和前端摘要页面会用到的统计信息。

## 7. 推荐阅读顺序

如果想尽快看懂代码，建议按这个顺序读：

1. `HadoopProcessingService`
   先理解整体调度和各个产物目录。
2. `TermStatisticsJob`
   看清楚 `tf` 和 `df` 是怎么来的。
3. `ScoredTermsJob`
   看清楚 `tfidf` 和 `index` 是怎么来的。
4. `DocumentKeywordsJob`
   看清楚文档关键词是怎么选出来的。
5. `CorpusIndexService.buildSnapshot(...)`
   看清楚 Hadoop 输出最终如何变成运行时搜索快照。

## 8. 一句话心智模型

- 任务 1 负责计数，任务 2 负责打分并构建 posting，任务 3 负责把分数整理成每篇文档的关键词摘要。
