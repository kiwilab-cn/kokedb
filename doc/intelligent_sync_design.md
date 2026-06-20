# 智能同步设计：自适应刷新频率 + 增量推断 + LLM 辅助

> 三阶段实现计划。规则内核必须独立可用；LLM 是可选增强（配 Anthropic key 才启用）。
> 所有"推断出来"的增量配置一律 **先 suggested → 影子校验通过 → 才自动启用**。

## 0. 现状基线（已实现，本设计在其上扩展）

- `src/task-manager/src/incremental.rs`
  - `detect_primary_keys` —— 读 `information_schema` 拿主键
  - `detect_watermark_column` —— 按候选名 + timestamp 类型找"更新时间"列
  - `merge_snapshot` —— 按 PK 做 UPSERT 合并
- `src/task-manager/src/runner.rs::plan_sync` —— 决策 Full / Incremental / SkipUnchanged
- `force_full_every`（默认 10）—— 周期性全量对账修硬删除
- 调度：`catalog.rs::create_catalog_scheduler_job` 单个全局 cron `KOKEDB_CACHE_JOB_INTERVAL`（30 min），整 catalog 一起刷
- meta 表：`system.table_sync_state`、`system.query_table_daily_stats`（每表每日查询次数，现成的"热度"信号）、`system.table_arrow_schema`

**缺口**：① 频率不分表；② 无常规命名/无 timestamp 时直接退全量，不尝试推断；③ 无 APPEND；④ 无校验闸门；⑤ 无 LLM。

---

## 1. 数据模型变更（meta）

新增表 `system.table_sync_policy`（每表的"推断结果 + 生效配置 + 来源 + 状态机"）：

```sql
CREATE TABLE IF NOT EXISTS system.table_sync_policy (
    catalog          VARCHAR(255) NOT NULL,
    schema_name      VARCHAR(255) NOT NULL,
    table_name       VARCHAR(255) NOT NULL,
    -- 推断出的增量配置
    inc_mode         VARCHAR(16)  NOT NULL DEFAULT 'full',   -- full | upsert | append
    watermark_column VARCHAR(255),
    pk_columns       VARCHAR(1024),
    -- 状态机：none -> (probation) -> active | rejected
    inc_status       VARCHAR(16)  NOT NULL DEFAULT 'none',
    inc_tier         VARCHAR(16)  NOT NULL DEFAULT 'audited', -- trusted|probation|audited (§3.2)
    source           VARCHAR(16)  NOT NULL DEFAULT 'rule',   -- rule | llm | user
    confidence       REAL,                                   -- 0..1
    reason           TEXT,                                   -- 人类可读理由（含 LLM 推理）
    -- 分层抽检调度（§3.2/§3.3）
    next_audit_at    TIMESTAMPTZ,
    audit_kind       VARCHAR(16)  NOT NULL DEFAULT 'window', -- window | full
    audit_passes     INTEGER      NOT NULL DEFAULT 0,        -- 连续通过次数（越大越信任→拉长间隔）
    divergence_count INTEGER      NOT NULL DEFAULT 0,        -- 累计不一致次数（达阈值→reject）
    -- 自适应频率
    refresh_bucket   VARCHAR(16)  NOT NULL DEFAULT 'normal', -- fast|normal|slow|cold (见 §2)
    refresh_interval_sec INTEGER,                            -- 解析后的秒数
    -- 观测信号快照（用于打分 & SHOW TABLE METADATA）
    est_row_count    BIGINT,
    est_size_bytes   BIGINT,
    churn_per_hour   REAL,                                   -- 估算的写入行/小时
    access_per_day   REAL,                                   -- 来自 query_table_daily_stats
    -- 校验
    last_validated_at TIMESTAMPTZ,
    validation_result VARCHAR(16),                           -- pass | diverged | n/a
    schema_hash      VARCHAR(64),                            -- 复用 LLM/规则推断的缓存键
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (catalog, schema_name, table_name)
);
```

`table_sync_state` 保持不变（运行期 watermark/incremental_runs）；`table_sync_policy` 是"决策层"，二者分离：policy 决定**怎么同步**，state 记录**同步到哪了**。

---

## 2. Phase 1：自适应刷新频率（纯规则，先落地）

### 2.1 信号采集（都从远程 PG / 本地 meta 廉价拿）
新文件 `src/task-manager/src/table_signals.rs`：

| 信号 | 来源 | SQL |
|---|---|---|
| 行数估算 | 远程 `pg_class.reltuples` | `SELECT reltuples FROM pg_class WHERE oid='schema.table'::regclass` |
| 表大小 | 远程 `pg_total_relation_size` | `SELECT pg_total_relation_size('schema.table'::regclass)` |
| 写入 churn | 远程 `pg_stat_user_tables` | `n_tup_ins+n_tup_upd+n_tup_del` 两次采样差 / 时间差；首次用 `last_autovacuum`/stats reset 估 |
| 查询热度 | 本地 `system.query_table_daily_stats` | 近 7 天 `SUM(query_count)/7` |

churn 也可不采两次：直接复用同步时记录的 `last_watermark` 推进量 + `last_sync_at` 间隔，零额外查询。

### 2.2 打分 → 分档
```
freshness_value = log1p(access_per_day)         // 越热越想要新鲜
update_pressure = log1p(churn_per_hour)         // 变得越快越要勤刷
cost            = log1p(est_size_bytes)         // 越大刷一次越贵

score = w_a*freshness_value + w_u*update_pressure - w_c*cost
```
分档（避免上千条 cron，且稳定）：

| bucket | 条件（score 分位 / 硬规则） | interval |
|---|---|---|
| fast | score 高 且 churn>0 | 2 min |
| normal | 默认 | 15 min |
| slow | 大表 + 低 churn | 1 h |
| cold | churn≈0 或近 30 天无访问 | 6 h（或仅按需 `REFRESH`）|

权重 `w_*` 走 env，给保守默认。结果写 `table_sync_policy.refresh_bucket/refresh_interval_sec`。

### 2.3 调度器改造（已实现：tick + 周期重评估）
现在：每 catalog 一个 cron 全量遍历。改为**逐表到期触发**——比"每档一个 cron"更简单、且支持连续区间：

- 每 catalog 一个**每分钟 tick**（`adaptive::tick_refresh_due`）：查 `get_due_refresh_tables`
  （`last_sync_at + refresh_interval_sec <= now` 的表），逐个 `refresh_single_table` 入队；
  用 `TaskManager::has_inflight_table_task` 去重，避免同步耗时超过 tick 时堆积重复任务。
- 同一 tick 闭包里按计数器每 `KOKEDB_REEVALUATE_INTERVAL_MIN` 分钟跑一次
  `adaptive::reevaluate_catalog`：重新 `select_tables_for_policy`（顺带发现新表）+ 采信号 +
  打分 + upsert policy。
- `KOKEDB_ADAPTIVE_REFRESH=false` 回退到旧的单 cron 全量同步。

改动落在 `catalog.rs::create_catalog_scheduler_job`、新模块 `task-manager/src/{table_signals,adaptive}.rs`、
`cache_sync_task.rs`(抽出 `select_tables_for_policy`)、meta 的 `table_sync_policy` 访问器。
向后兼容：没有 policy 行的表不会被 tick 选中，由首次同步 + 重评估补种。

---

## 3. Phase 2：增量推断 + 安全闸门

### 3.1 放宽 watermark 检测（规则）
`detect_watermark_column` 升级为 `infer_incremental_strategy`：
1. **UPSERT**：有 PK + 任一 `timestamp/timestamptz` 列。多候选时排序：候选名命中 > `pg_stats` 单调相关性高（`correlation`≈1）> NOT NULL > 有索引。
2. **APPEND**：无更新型 watermark，但 PK 是单调 `bigint/identity/serial`（`pg_get_serial_sequence` 或 `GENERATED AS IDENTITY`）→ 仅追加新行（不处理 UPDATE）。
3. 否则 **full**。

输出 `{mode, watermark, pk, confidence, reason}`，写 `table_sync_policy`，状态 `suggested`。

### 3.2 分层校验：置信度 → 对账强度，随履历衰减

核心思想：校验成本和置信度成反比，且一张表的对账频率随"通过履历"自动衰减、随失败反弹。
分层只是这条衰减曲线的**起点**不同。

新增 policy 字段：`inc_tier`(trusted|probation|audited)、`next_audit_at`、`audit_passes`、
`divergence_count`、`audit_kind`(window|full)。

```
infer → 按 confidence 分层：
  T1 trusted   ──直切──────────────► active(只靠周期全量兜底，不额外对账)
  T2 probation ──试用(≤3d 或 N 次一致)──► active(audited, 低频抽检)
  T3 audited   ──直切但永久低频抽检────► active(audited, 起始抽检频率更高)

active(audited) 每到 next_audit_at：
  cheap-audit(window count+checksum)
    pass     → audit_passes++; 拉长 next_audit_at（履历越久越信任）
    diverge  → 跑一次 full 自愈; divergence_count++
                 < 阈值 → 留在 audited 并缩短抽检间隔
                 ≥ 阈值 → reject(永久 full, 记 reason)
```

**T1 = "确认的直接增量"，但"确认"必须是可检测的强保证**，而不是"看起来像"：
- 时间戳列有 `DEFAULT now()` **且** `pg_trigger` 里有 UPDATE 时刷新该列的触发器 → watermark 由 DB 强制维护，可信；
- 或表有 append-only 约束 / 不可变语义。
检不出强保证的 PK+timestamp 落 **T2**，模糊/LLM 推断落 **T3**。

**两种对账，成本不同**：
- *cheap-audit（窗口化）*：只比最近一天/最近分区的 `count(*)` + 列校验和（行级 hash 聚合）。常态高频用它，便宜。
- *full reconcile*：已有的 `force_full_every` 周期全量，作为**稀有**的彻底兜底 + 失败自愈。

**迟滞防抖**：单次 diverge 不判死——先全量自愈，连续失败到阈值才降级。避免一次 bulk update 造成误杀抖动。

### 3.3 校验调度：复用 TaskManager，不另起线程池
加 `TaskType::ShadowValidate`（`TaskPriority::Low`）+ 一个周期 sweeper：扫 `table_sync_policy`
里 `next_audit_at <= now` 的表，入队低优先级抽检任务。低优先级保证审计永远不饿死真正的同步，
并复用现成的队列/背压/优雅关闭。这就是"队列+线程池定时对账"，但用现有基建。

### 3.4 改动点
`plan_sync` 读 `table_sync_policy.inc_tier/inc_status`：`active` 才真增量；`probation` 跑全量 +
影子比对；`rejected` 永远全量。新文件 `src/task-manager/src/sync_validation.rs`（对账逻辑）+
sweeper 接入 `task_manager.rs`。

---

## 4. Phase 3：LLM 辅助层（Anthropic 原生，配 key 才启用）

### 4.1 触发条件（省钱：只在规则模糊时调）
规则给出 `confidence < 阈值`（如无明确 watermark、PK 非单调、列名非英文/语义模糊）→ 才调 LLM。按 `schema_hash` 缓存结果，schema 不变不重复调。

### 4.2 接入（Anthropic Messages API）
新文件 `src/task-manager/src/llm.rs`，env：
```
KOKEDB_LLM_PROVIDER=anthropic
KOKEDB_LLM_API_KEY=sk-ant-...
KOKEDB_LLM_MODEL=claude-opus-4-8      # 难例；便宜场景可配 sonnet
KOKEDB_LLM_BASE_URL=https://api.anthropic.com   # 可选，自建网关
```
POST `/v1/messages`，header `x-api-key` + `anthropic-version: 2023-06-01`。

### 4.3 Prompt（**只发 schema + 聚合统计，绝不发行数据**）
输入：列名/类型/NOT NULL、PK、索引、列注释、`reltuples`、候选时间列的 `min/max/distinct`、churn 计数。
要求模型返回**严格 JSON**（用 tool_use / response schema 强约束）：
```json
{
  "inc_mode": "upsert|append|full",
  "watermark_column": "string|null",
  "pk_columns": ["..."],
  "refresh_bucket": "fast|normal|slow|cold",
  "confidence": 0.0,
  "reason": "为什么这么判断（人类可读）"
}
```

### 4.4 安全
- LLM 输出**不直接生效**：同样进 `suggested`，过 §3.2 影子校验才 `active`。LLM 只是"更聪明的推断器"，不绕过校验。
- `source='llm'`、`reason` 落库，可解释、可审计。
- 失败/超时/未配 key → 静默回退纯规则。隐私开关 `KOKEDB_LLM_SEND_STATS=true|false`（false 时只发 schema）。

---

## 5. 可观测：`SHOW TABLE METADATA`

填上 `doc/cache_draft.sql` 已设计的命令，直接读 `table_sync_policy`：
```
SHOW TABLE METADATA FROM catalog.db.table
-> is_partitioned / detected_timestamp_cols / pk / inc_mode / inc_status /
   source / confidence / reason / refresh_bucket / est_size / churn / access
```
让"为什么这张表是这个同步策略"完全透明。解析/执行复用本次 `REFRESH CACHE` 已搭好的 命令链路（AST→spec→resolver→CatalogCommand）。

---

## 6. 配置项汇总（env）

```
# 自适应频率
KOKEDB_ADAPTIVE_REFRESH=true
KOKEDB_REFRESH_BUCKET_FAST_SEC=120
KOKEDB_REFRESH_BUCKET_NORMAL_SEC=900
KOKEDB_REFRESH_BUCKET_SLOW_SEC=3600
KOKEDB_REFRESH_BUCKET_COLD_SEC=21600
KOKEDB_REFRESH_SCORE_W_ACCESS=1.0
KOKEDB_REFRESH_SCORE_W_CHURN=1.0
KOKEDB_REFRESH_SCORE_W_COST=0.5
KOKEDB_REEVALUATE_INTERVAL_MIN=60
# 增量推断 + 分层校验
KOKEDB_INFER_INCREMENTAL=true
KOKEDB_PROBATION_DAYS=3             # T2 试用期上限
KOKEDB_PROBATION_PASSES=2          # 或连续一致几次即毕业（谁先到）
KOKEDB_AUDIT_BASE_INTERVAL_MIN=1440   # audited 起始抽检间隔（随 audit_passes 拉长）
KOKEDB_AUDIT_DIVERGENCE_MAX=2      # 连续/累计不一致达此值 → reject 永久 full
KOKEDB_AUDIT_WINDOW=1d             # 廉价抽检的对账窗口（只比最近 1 天）
# LLM（可选）
KOKEDB_LLM_PROVIDER / _API_KEY / _MODEL / _BASE_URL / _SEND_STATS
KOKEDB_LLM_CONFIDENCE_THRESHOLD=0.6 # 规则低于此才调 LLM
```

---

## 7. 实施顺序与风险

| 步 | 内容 | 风险 | 价值 |
|---|---|---|---|
| 1 | `table_sync_policy` 表 + 信号采集 + 打分 + 分档调度 | 低（不碰正确性，只改频率）| 高，立竿见影 |
| 2 | 推断升级 + 状态机 + 影子校验 | 中（校验逻辑要严谨，但有 full 兜底）| 高 |
| 3 | LLM 层 + `SHOW TABLE METADATA` | 低（可选、回退安全）| 锦上添花 |

**总原则**：任何阶段失败都安全回退到"全量 + 全局频率"的现有行为；增量永远要么是显式已验证的，要么是过了影子校验的。
