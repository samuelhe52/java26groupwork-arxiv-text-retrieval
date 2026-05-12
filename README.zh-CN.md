# Hadoop 文本检索系统

[English](README.md)

这是一个面向 Java 期末作业的 Hadoop/YARN 文本检索应用。项目范围是围绕领域文本语料完成 TF-IDF 计算、提取代表性关键词、构建倒排索引，并通过 Web 应用提供检索与结果展示能力。

当前语料基于 arXiv 摘要数据，本仓库中的本地脚本用于先生成按类别过滤的 JSONL 数据集，再上传到 HDFS。整个项目重点展示分布式存储、分布式计算和 Java Web 后端如何组成一条完整的检索流程。

## 已锁定的技术选择

- 后端：Spring Boot 3.4.1，Java 17
- 后端构建：Maven
- 后端入口：`backend/mvnw`
- Hadoop 客户端：3.4.1
- 前端：Vue 3 + Vite
- 前端语言：JavaScript
- 前端运行时：Node 20 LTS
- Java 包命名空间：`com.java26groupwork.finalassignment`
- 数据脚本：默认使用仓库相对路径，可通过环境变量覆盖
- 协作约定：见 [docs/PROJECT_DECISIONS.md](docs/PROJECT_DECISIONS.md)

## 什么是 TF-IDF

TF-IDF 是 `词频 - 逆文档频率`。

- `TF` 表示某个词在单篇文档中出现的频率。
- `IDF` 表示某个词在整个语料库中的稀有程度或区分能力。
- 一个词的 TF-IDF 分数越高，通常说明它对当前文档更重要，同时又不是在所有文档里都很常见。

在这个项目中，TF-IDF 用于提取文档关键词，并为搜索结果排序提供依据。

## 项目范围

- 将文本语料存储到 HDFS
- 在 YARN 上运行 MapReduce 任务，完成 TF、DF 和 TF-IDF 计算
- 为每篇文档提取 Top 关键词
- 构建面向关键词检索的倒排索引
- 提供用于搜索、语料概览和结果展示的 Web 界面

## 仓库结构

- `backend/`：Spring Boot 后端，负责 API、任务调度和检索逻辑
- `frontend/`：基于 Vite 的前端，用于搜索和可视化页面
- `datasets/`：本地数据集快照和处理后的语料文件
- `scripts/`：用于组装和清洗 arXiv 数据集的数据准备脚本
- `Makefile`：顶层常用命令入口，包含后端、前端和数据归档任务

## 后端环境变量

后端支持两种 Hadoop 模式：

- `local`：本地开发模式，不依赖集群
- `cluster`：读取真实 Hadoop 客户端 XML 配置，并连接到 HDFS/YARN

后端会按以下顺序寻找 Hadoop 配置目录：

1. `HADOOP_CONF_DIR`
2. `HADOOP_HOME/etc/hadoop`
3. `${user.home}/hadoop/etc/hadoop`

这样仓库本身就不需要写死某台机器的路径。无论是 macOS、Linux 还是 WSL，每个成员都可以通过自己的环境变量指向本机 Hadoop 配置目录。

示例：

```bash
# macOS Orb
export HADOOP_CONF_DIR=/Users/<your-name>/OrbStack/ubuntu/opt/hadoop-3.4.1/etc/hadoop

# Linux 或 WSL
export HADOOP_CONF_DIR=/opt/hadoop-3.4.1/etc/hadoop
```

## 本地开发

平时开发后端时，默认使用 `local` 模式即可，不需要 HDFS、YARN，也不需要先启动集群。

```bash
make backend-run
```

在这个模式下，后端使用 `file:///`，适合先完成 Web/API 逻辑和本地联调。

## 集群联调与生产测试

当你需要连接真实 Hadoop 集群做联调或生产环境测试时，先设置 `HADOOP_CONF_DIR`，再以 `cluster` profile 启动后端：

```bash
cd backend
HADOOP_CONF_DIR=/path/to/etc/hadoop ./mvnw -q spring-boot:run -Dspring-boot.run.profiles=cluster
```

在 `cluster` 模式下，后端会读取：

- `core-site.xml`
- `hdfs-site.xml`
- 如果存在则读取 `yarn-site.xml`

因此，当前活跃 NameNode 的发现、HDFS HA 行为以及 YARN HA 行为都交给集群自己的 Hadoop 客户端配置处理，而不是写死在应用代码里。

## 当前数据准备脚本

- `scripts/assemble_arxiv_snapshot_dataset.py`：生成按年份组织的本地 arXiv `cs.LG` 数据集
- `scripts/cleanup_arxiv_dataset_primary_cs.py`：在已有数据集基础上进一步筛出 primary category 为 `cs.*` 的子集
- `scripts/preflight_arxiv_dataset.py`：在进入 Java 或 Hadoop 之前，先做 Python 版语料检查、2019 年以来的年度关键词和 TF-IDF 烟雾测试

## 数据获取说明

小组成员可以直接从微信群下载对应的 `.7z` 压缩包，并在本地解压后直接使用，无需自行重新打包数据集。

## 常用命令

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
