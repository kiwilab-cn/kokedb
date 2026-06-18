-- ============================================================================
-- Remote Catalog Cache Management SQL - Complete Design Document
-- 远程 Catalog 缓存管理 SQL - 完整设计文档
-- 版本: v2.0 优化版
-- ============================================================================

-- ============================================================================
-- 一、架构设计概览
-- ============================================================================

/*
核心设计原则：

1. 三层策略继承体系（清晰的配置层级）
   ┌─────────────────────────────────────────────────────────────┐
   │ CATALOG POLICY (全局默认策略)                                │
   │  - 定义缓存范围选择逻辑 (SMART/TOPK/SELECT/ALL/NONE)         │
   │  - 全局资源配额和默认值                                       │
   │  - 作用于整个 Catalog                                        │
   └─────────────────────────────────────────────────────────────┘
                         ↓ 继承/覆盖
   ┌─────────────────────────────────────────────────────────────┐
   │ DATABASE POLICY (库级策略) - 可选                            │
   │  - 为特定库设置统一的特殊规则                                 │
   │  - 覆盖 Catalog 的默认值                                     │
   │  - 典型场景: 临时库禁用、核心库高优先级、归档库低频刷新        │
   └─────────────────────────────────────────────────────────────┘
                         ↓ 继承/覆盖
   ┌─────────────────────────────────────────────────────────────┐
   │ TABLE POLICY (表级策略) - 可选                               │
   │  - 为特定表设置精细化配置                                     │
   │  - 增量配置、刷新频率、缓存范围等                             │
   │  - 最高优先级，覆盖上层所有配置                               │
   └─────────────────────────────────────────────────────────────┘
                         ↓ 自动生成
   ┌─────────────────────────────────────────────────────────────┐
   │ CACHE SCHEDULE (调度计划)                                    │
   │  - 从 Policy 自动生成的调度配置                              │
   │  - 定义 WHEN 和 HOW（刷新时间、频率、策略）                   │
   │  - 用户只读/可暂停恢复                                        │
   └─────────────────────────────────────────────────────────────┘
                         ↓ 每次触发
   ┌─────────────────────────────────────────────────────────────┐
   │ CACHE JOB (执行实例)                                         │
   │  - 单次缓存刷新的执行记录                                     │
   │  - 包含状态、日志、性能指标                                   │
   │  - 完全由系统管理                                            │
   └─────────────────────────────────────────────────────────────┘

2. 策略继承规则
   - 子层级未配置的属性 → 从父层级继承
   - 子层级显式配置的属性 → 覆盖父层级
   - 查询时显示 source 标识来源（CATALOG/DATABASE/TABLE）

3. SCHEDULE vs JOB 分离（调度系统标准模式）
   - SCHEDULE = 调度计划（长期存在，类似 cron job 配置）
   - JOB = 执行实例（短暂存在，类似进程运行记录）
   - 每次 Schedule 触发或手动刷新都创建新 Job

4. 四种 Catalog Policy 模式
   - SMART: 智能选择（多维度评分，推荐默认）
   - TOPK: 按单一指标选择 Top K 张表
   - SELECT: 显式指定（模式匹配或列表）
   - ALL: 缓存所有表（资源充足时）
   - NONE: 禁用缓存
*/


-- ============================================================================
-- 二、CATALOG POLICY 管理（全局默认策略）
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 2.1 设置 Catalog 策略
-- ----------------------------------------------------------------------------

-- 模式 1: SMART - 智能选择（推荐，适合大部分场景）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'SMART',                    -- 智能选择模式
    
    -- 资源配额
    max_tables = 100,                  -- 最多缓存 100 张表
    max_cache_size = '500GB',          -- 总缓存大小上限
    
    -- 智能选择参数
    min_access_frequency = 10,         -- 最小访问频率（次/小时）
    score_weights = '{
        "access_frequency": 0.5,       -- 访问频率权重 50%
        "table_size": 0.2,             -- 表大小权重 20%
        "update_frequency": 0.3        -- 更新频率权重 30%
    }',
    
    -- 全局默认值（会被子层级继承）
    default_priority = 'NORMAL',       -- 默认优先级: LOW | NORMAL | HIGH
    default_refresh_interval = '10m',  -- 默认刷新间隔
    default_incremental_mode = 'AUTO'  -- 默认增量模式: AUTO | APPEND | UPSERT
);

-- 模式 2: TOPK - 按规则选择 Top K（资源有限场景）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'TOPK',
    
    -- Top K 参数
    top_k = 50,                        -- 选择 Top 50 张表
    rank_by = 'access_frequency',      -- 排序依据: access_frequency | table_size | update_frequency
    rank_order = 'DESC',               -- DESC(降序) | ASC(升序)
    
    -- 过滤条件（可选）
    filter_databases = 'dw,ods',       -- 只考虑这些库
    filter_min_size = '1GB',           -- 只考虑大于 1GB 的表
    exclude_patterns = 'tmp_%,test_%', -- 排除模式
    
    -- 全局配额
    max_cache_size = '500GB',
    default_priority = 'NORMAL'
);

-- 模式 3: SELECT - 显式指定（精确控制）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'SELECT',
    
    -- 模式匹配（推荐）
    include_patterns = 'dw.fact_%,dw.dim_%,ods.order_%',
    exclude_patterns = 'dw.fact_tmp_%,*.%_backup',
    
    -- 或显式列表（与 patterns 二选一）
    -- include_tables = 'db1.table1,db2.table2',
    
    max_cache_size = '500GB'
);

-- 模式 4: ALL - 缓存所有表（资源充足）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'ALL',
    exclude_databases = 'tmp,test',    -- 排除某些库
    exclude_patterns = 'tmp_%,test_%',
    max_cache_size = '1TB'
);

-- 模式 5: NONE - 禁用缓存
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'NONE'
);

-- ----------------------------------------------------------------------------
-- 2.2 查询 Catalog 策略
-- ----------------------------------------------------------------------------

-- 查看策略配置
SHOW CACHE POLICY FROM CATALOG catalog_name;
/*
返回示例:
+-------------------------+----------------------------------------+
| Property                | Value                                  |
+-------------------------+----------------------------------------+
| mode                    | SMART                                  |
| max_tables              | 100                                    |
| max_cache_size          | 500GB                                  |
| min_access_frequency    | 10                                     |
| default_priority        | NORMAL                                 |
| default_refresh_interval| 10m                                    |
| current_cached_tables   | 87                                     |
| current_cache_size      | 423GB                                  |
+-------------------------+----------------------------------------+
*/

-- 查看策略选中的表（SMART/TOPK 模式）
SHOW CACHE POLICY SELECTED_TABLES FROM CATALOG catalog_name
ORDER BY selection_score DESC;
/*
返回示例:
+----------+------------------+-------+----------+--------+----------+
| Database | Table            | Score | Access/h | Size   | Selected |
+----------+------------------+-------+----------+--------+----------+
| dw       | fact_orders      | 95.3  | 150      | 125GB  | YES      |
| dw       | fact_sales       | 87.2  | 120      | 98GB   | YES      |
| ods      | orders           | 76.5  | 80       | 200GB  | YES      |
+----------+------------------+-------+----------+--------+----------+
*/

-- 查看所有 Catalog 策略
SHOW CACHE POLICIES WHERE level = 'CATALOG';

-- ----------------------------------------------------------------------------
-- 2.3 删除 Catalog 策略
-- ----------------------------------------------------------------------------

ALTER CATALOG catalog_name UNSET CACHE POLICY;
-- 恢复系统默认（通常为 SMART 模式）


-- ============================================================================
-- 三、DATABASE POLICY 管理（库级特殊策略）
-- ============================================================================

/*
设计目的：
- 为特定 Database 设置统一的特殊规则
- 避免为库下所有表重复配置相同策略
- 典型场景：
  * 临时库全部禁用: tmp 库不缓存
  * 核心库高优先级: dw 库全部 HIGH + 5 分钟刷新
  * 归档库低频刷新: archive 库全部 1 小时刷新
*/

-- ----------------------------------------------------------------------------
-- 3.1 设置 Database 策略
-- ----------------------------------------------------------------------------

-- 示例 1: 核心业务库 - 统一高优先级
ALTER DATABASE catalog_name.dw
SET CACHE POLICY WITH (
    inherit_from_catalog = true,       -- 继承 Catalog 未覆盖的配置
    
    mode = 'ALL',                      -- 覆盖: 此库所有表都缓存
    default_priority = 'HIGH',         -- 覆盖: 默认高优先级
    default_refresh_interval = '5m',   -- 覆盖: 默认 5 分钟刷新
    
    -- Database 特有配置
    max_tables_per_database = 50,
    exclude_patterns = 'tmp_%'         -- 仍排除临时表
);

-- 示例 2: 临时库 - 完全禁用
ALTER DATABASE catalog_name.tmp
SET CACHE POLICY WITH (
    mode = 'NONE'                      -- 此库下所有表都不缓存
);

-- 示例 3: 归档库 - 低优先级 + 长周期
ALTER DATABASE catalog_name.archive
SET CACHE POLICY WITH (
    inherit_from_catalog = true,
    mode = 'TOPK',
    top_k = 10,                        -- 只缓存最热的 10 张表
    default_priority = 'LOW',
    default_refresh_interval = '1h'
);

-- ----------------------------------------------------------------------------
-- 3.2 查询 Database 策略
-- ----------------------------------------------------------------------------

-- 查看策略配置
SHOW CACHE POLICY FROM DATABASE catalog_name.database_name;

-- 查看生效配置（含继承链）
SHOW CACHE POLICY EFFECTIVE FROM DATABASE catalog_name.database_name;
/*
返回示例（显示继承来源）:
+-------------------------+------------------+----------------------+
| Property                | Value            | Source               |
+-------------------------+------------------+----------------------+
| mode                    | ALL              | DATABASE (override)  |
| default_priority        | HIGH             | DATABASE (override)  |
| default_refresh_interval| 5m               | DATABASE (override)  |
| max_cache_size          | 500GB            | CATALOG (inherited)  |
| max_tables              | 100              | CATALOG (inherited)  |
+-------------------------+------------------+----------------------+
*/

-- 查看所有 Database 策略
SHOW CACHE POLICIES WHERE level = 'DATABASE';

-- ----------------------------------------------------------------------------
-- 3.3 删除 Database 策略
-- ----------------------------------------------------------------------------

ALTER DATABASE catalog_name.database_name UNSET CACHE POLICY;
-- 删除后，此库下的表完全由 Catalog Policy 决定


-- ============================================================================
-- 四、TABLE POLICY 管理（表级精细策略）
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 4.1 设置 Table 策略
-- ----------------------------------------------------------------------------

-- 基础配置（使用自动检测，最简单）
ALTER TABLE catalog_name.db_name.table_name 
SET CACHE POLICY WITH (
    enable_cache = true,               -- 强制启用（即使未被上层选中）
    auto_detect = true                 -- 自动检测增量字段等配置
);

-- 完整配置（手动指定所有参数）
ALTER TABLE catalog_name.db_name.table_name 
SET CACHE POLICY WITH (
    -- 继承控制
    inherit_from_database = true,      -- 优先继承 Database Policy
    inherit_from_catalog = true,       -- 无 Database Policy 时继承 Catalog
    
    -- 基础配置
    enable_cache = true,               -- 是否缓存
    priority = 'HIGH',                 -- 覆盖继承的优先级
    
    -- 刷新配置
    refresh_mode = 'SCHEDULED',        -- SCHEDULED | MANUAL
    refresh_interval = '2m',           -- 覆盖继承的刷新间隔
    
    -- 增量更新配置
    enable_incremental = true,
    incremental_mode = 'UPSERT',       -- AUTO | APPEND | UPSERT | CUSTOM
    incremental_key = 'order_id',      -- 主键（UPSERT 必需）
    incremental_column = 'update_time',-- 增量判断字段
    incremental_filter = 'status != "deleted"', -- 过滤条件
    
    -- 缓存范围配置
    cache_columns = 'id,name,status,create_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY',
    
    -- 高级配置
    max_retry = 3,
    timeout = '30m'
);

-- 增量模式示例

-- 示例 1: APPEND 模式（只追加，日志表）
ALTER TABLE catalog_name.logs.access_logs 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'APPEND',       -- 只追加新数据
    incremental_column = 'log_time',   -- 无需主键
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

-- 示例 2: UPSERT 模式（支持更新，业务表）
ALTER TABLE catalog_name.dw.orders 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'UPSERT',       -- 支持插入和更新
    incremental_key = 'order_id',      -- 必需主键
    incremental_column = 'update_time'
);

-- 示例 3: 联合主键
ALTER TABLE catalog_name.dw.order_items 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id,item_id',  -- 联合主键
    incremental_column = 'update_time'
);

-- 示例 4: CUSTOM 模式（自定义增量逻辑）
ALTER TABLE catalog_name.app.users 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'CUSTOM',
    incremental_query = '''
        SELECT * FROM ${table}
        WHERE update_time > ${last_refresh_time}
          AND is_deleted = 0
          AND status IN ('active', 'pending')
    '''
);

-- ----------------------------------------------------------------------------
-- 4.2 批量设置 Table 策略
-- ----------------------------------------------------------------------------

ALTER TABLES LIKE 'catalog_name.dw.fact_%'
SET CACHE POLICY WITH (
    enable_cache = true,
    priority = 'HIGH',
    auto_detect = true
);

-- ----------------------------------------------------------------------------
-- 4.3 查询 Table 策略
-- ----------------------------------------------------------------------------

-- 查看策略配置
SHOW CACHE POLICY FROM TABLE catalog_name.db_name.table_name;

-- 查看生效配置（含完整继承链）
SHOW CACHE POLICY EFFECTIVE FROM TABLE catalog_name.db_name.table_name;
/*
返回示例:
+-------------------+------------------+------------------------+
| Property          | Value            | Source                 |
+-------------------+------------------+------------------------+
| enable_cache      | true             | TABLE (explicit)       |
| priority          | HIGH             | TABLE (override)       |
| refresh_interval  | 2m               | TABLE (override)       |
| incremental_mode  | UPSERT           | TABLE (explicit)       |
| incremental_key   | order_id         | TABLE (explicit)       |
| max_cache_size    | 500GB            | CATALOG (inherited)    |
| default_priority  | HIGH             | DATABASE (inherited)   |
+-------------------+------------------+------------------------+
*/

-- 查看表元数据（自动检测结果）
SHOW TABLE METADATA FROM catalog_name.db_name.table_name;
/*
返回示例:
+------------------------+----------------------------------+
| Property               | Value                            |
+------------------------+----------------------------------+
| is_partitioned         | true                             |
| partition_columns      | [dt]                             |
| primary_key            | [order_id]                       |
| detected_timestamp_cols| [update_time, create_time]       |
| recommended_mode       | UPSERT                           |
| table_size             | 125GB                            |
| row_count              | 150000000                        |
| update_frequency       | 5000 rows/min                    |
| access_frequency       | 120 queries/hour                 |
+------------------------+----------------------------------+
*/

-- ----------------------------------------------------------------------------
-- 4.4 删除 Table 策略
-- ----------------------------------------------------------------------------

ALTER TABLE catalog_name.db_name.table_name UNSET CACHE POLICY;
-- 删除后，此表完全由上层 Policy (Database 或 Catalog) 决定


-- ============================================================================
-- 五、策略应用和 SCHEDULE 生成
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 5.1 应用策略（生成 Schedule）
-- ----------------------------------------------------------------------------

-- 应用整个 Catalog 策略
APPLY CACHE POLICY FOR CATALOG catalog_name;
/*
执行逻辑:
1. 评估三层策略（Catalog → Database → Table）
2. 根据 Catalog Policy 选择要缓存的表
3. 为每张表计算最终生效配置（继承链合并）
4. 生成或更新 CACHE SCHEDULE
5. 启动调度器

输出示例:
Selected 87 tables based on SMART policy.
Generated 85 schedules (2 tables already have schedules).
Applied custom policies for 5 tables.
Scheduler started.
*/

-- 应用单个 Database 策略
APPLY CACHE POLICY FOR DATABASE catalog_name.database_name;

-- 应用单个 Table 策略
APPLY CACHE POLICY FOR TABLE catalog_name.db_name.table_name;

-- 强制重新评估（重新计算 SMART/TOPK 选择）
APPLY CACHE POLICY FOR CATALOG catalog_name 
WITH (force_reevaluate = true);

-- 预演模式（不实际应用，查看变更）
APPLY CACHE POLICY FOR CATALOG catalog_name 
WITH (
    dry_run = true,                    -- 不实际生成 Schedule
    show_changes = true                -- 显示变更内容
);
/*
返回预演结果:
Changes Preview:
  [ADD]    Schedule for 'dw.orders' (priority=HIGH, interval=2m)
  [UPDATE] Schedule for 'ods.logs' (interval: 10m -> 5m)
  [REMOVE] Schedule for 'tmp.test' (excluded by pattern)
  
Resource Impact:
  Current: 423GB / 500GB (85%)
  After:   487GB / 500GB (97%)
*/

-- ----------------------------------------------------------------------------
-- 5.2 查询 CACHE SCHEDULE（调度计划）
-- ----------------------------------------------------------------------------

-- 查看单表的 Schedule
SHOW CACHE SCHEDULE FROM TABLE catalog_name.db_name.table_name;
/*
返回示例:
+-------------------+----------------------------------+
| Property          | Value                            |
+-------------------+----------------------------------+
| schedule_id       | schedule_orders_20240101         |
| schedule_type     | AUTO_GENERATED                   |
| source_policy     | CATALOG:SMART + DATABASE:dw + TABLE:orders |
| status            | ACTIVE                           |
| selected_by       | CATALOG_POLICY (score: 95.3)     |
| schedule_config   |                                  |
|   refresh_mode    | SCHEDULED                        |
|   refresh_interval| 5m                               |
|   cron_expression | */5 * * * *                     |
|   next_run_time   | 2024-01-01 10:05:00             |
| cache_config      |                                  |
|   incremental_mode| UPSERT                           |
|   incremental_key | order_id                         |
|   priority        | HIGH                             |
| statistics        |                                  |
|   total_jobs      | 1247                             |
|   success_jobs    | 1245                             |
|   failed_jobs     | 2                                |
|   last_job_id     | job_orders_20240101_100000       |
|   last_job_status | SUCCESS                          |
+-------------------+----------------------------------+
*/

-- 查看所有 Schedule
SHOW CACHE SCHEDULES FROM CATALOG catalog_name
WHERE status = 'ACTIVE'
ORDER BY next_run_time;

-- 查看 Schedule 健康度
SHOW CACHE SCHEDULE HEALTH FROM CATALOG catalog_name
WHERE success_rate < 0.95
ORDER BY failure_count DESC;

-- ----------------------------------------------------------------------------
-- 5.3 SCHEDULE 生命周期管理
-- ----------------------------------------------------------------------------

-- 暂停/恢复（临时控制，不影响 Policy）
PAUSE CACHE SCHEDULE FOR TABLE catalog_name.db_name.table_name;
RESUME CACHE SCHEDULE FOR TABLE catalog_name.db_name.table_name;

-- 禁用/启用（需要手动恢复）
DISABLE CACHE SCHEDULE FOR TABLE catalog_name.db_name.table_name;
ENABLE CACHE SCHEDULE FOR TABLE catalog_name.db_name.table_name;

-- 删除 Schedule（停止调度，保留历史 Job）
DROP CACHE SCHEDULE FOR TABLE catalog_name.db_name.table_name;

-- 手动创建 Schedule（不依赖 Policy）
CREATE CACHE SCHEDULE schedule_custom_orders
ON TABLE catalog_name.db_name.table_name
WITH (
    refresh_mode = 'SCHEDULED',
    cron_expression = '0 */2 * * *',   -- 每 2 小时
    incremental_mode = 'CUSTOM',
    incremental_query = '''
        SELECT * FROM ${table}
        WHERE custom_condition(...)
    '''
);


-- ============================================================================
-- 六、CACHE JOB 管理（执行实例）
-- ============================================================================

/*
核心概念：
- JOB 是单次缓存刷新的执行记录
- 每次 Schedule 触发或手动刷新都创建新 Job
- 包含完整的执行信息：状态、日志、性能指标
- 类似于 Spark Job、Airflow Task Run
*/

-- ----------------------------------------------------------------------------
-- 6.1 查询 CACHE JOB
-- ----------------------------------------------------------------------------

-- 查看单个 Job 详情
SHOW CACHE JOB job_id;
/*
返回示例:
+-------------------+----------------------------------+
| Property          | Value                            |
+-------------------+----------------------------------+
| job_id            | job_orders_20240101_100000       |
| schedule_id       | schedule_orders_20240101         |
| table             | hive_catalog.dw.orders           |
| trigger_type      | SCHEDULED                        |
| trigger_time      | 2024-01-01 10:00:00             |
| start_time        | 2024-01-01 10:00:05             |
| end_time          | 2024-01-01 10:02:30             |
| duration          | 145s                             |
| status            | SUCCESS                          |
| refresh_mode      | INCREMENTAL                      |
| incremental_range | 2024-01-01 09:55:00 ~ 10:00:00  |
| records_scanned   | 125000                           |
| records_cached    | 123450                           |
| records_updated   | 98000                            |
| records_inserted  | 25450                            |
| cache_size_before | 5.2GB                            |
| cache_size_after  | 5.3GB                            |
| cache_size_delta  | +100MB                           |
| error_message     | NULL                             |
| retry_count       | 0                                |
+-------------------+----------------------------------+
*/

-- 查看 Job 列表
SHOW CACHE JOBS FROM TABLE catalog_name.db_name.table_name
WHERE start_time > CURRENT_DATE - INTERVAL 1 DAY
ORDER BY start_time DESC
LIMIT 20;

-- 查询失败的 Job
SHOW CACHE JOBS FROM CATALOG catalog_name
WHERE status = 'FAILED'
ORDER BY start_time DESC;

-- 查询运行中的 Job
SHOW CACHE JOBS WHERE status = 'RUNNING';

-- ----------------------------------------------------------------------------
-- 6.2 查询 Job 执行细节
-- ----------------------------------------------------------------------------

-- 查看 Job 执行日志
SHOW CACHE JOB LOGS FROM job_id;
/*
返回示例:
[10:00:05] Job started, type=INCREMENTAL
[10:00:06] Analyzing incremental range: update_time > '2024-01-01 09:55:00'
[10:00:10] Scanning source table: 125000 rows
[10:00:30] Computing delta: 98000 updates, 25450 inserts
[10:00:50] Applying changes to cache
[10:02:20] Cache updated successfully
[10:02:30] Job completed, duration=145s
*/

-- 查看 Job 性能指标
SHOW CACHE JOB METRICS FROM job_id;
/*
返回示例:
+-------------------+------------------+
| Metric            | Value            |
+-------------------+------------------+
| scan_rate         | 862 rows/s       |
| update_rate       | 675 rows/s       |
| cache_write_speed | 700 KB/s         |
| cpu_usage_avg     | 45%              |
| memory_usage_peak | 2.3GB            |
| network_bytes_in  | 850MB            |
| network_bytes_out | 120MB            |
+-------------------+------------------+
*/

-- ----------------------------------------------------------------------------
-- 6.3 Job 操作
-- ----------------------------------------------------------------------------

-- 重试失败的 Job
RETRY CACHE JOB job_id;

-- 取消运行中的 Job
CANCEL CACHE JOB job_id;

-- 清理历史 Job（保留策略）
DELETE CACHE JOBS FROM CATALOG catalog_name
WHERE end_time < CURRENT_DATE - INTERVAL 30 DAY
  AND status = 'SUCCESS';


-- ============================================================================
-- 七、数据刷新（触发 Job 执行）
-- ============================================================================

-- 手动触发刷新（创建新 Job）
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name;
-- 返回: job_id = job_orders_20240101_103000

-- 带选项的刷新
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name
WITH (
    mode = 'FULL',                     -- FULL(全量) | INCREMENTAL(增量)
    partitions = 'dt=20240101',        -- 指定分区
    async = true,                      -- 异步执行
    priority = 'HIGH'                  -- 优先级
);
-- 返回: job_id = job_orders_20240101_103001 (ASYNC)

-- 刷新多张表（批量创建 Job）
REFRESH CACHE FROM CATALOG catalog_name
WHERE database = 'dw'
  AND table_pattern LIKE 'fact_%'
WITH (parallel = 8);
-- 返回: Created 15 jobs, job_ids = [job1, job2, ...]

-- 查询刷新进度
SHOW CACHE REFRESH STATUS FROM CATALOG catalog_name;
/*
返回批量刷新进度:
+----------+--------+----------+---------+
| Status   | Count  | Progress | ETA     |
+----------+--------+----------+---------+
| PENDING  | 3      | -        | -       |
| RUNNING  | 5      | 60%      | 2m 30s  |
| SUCCESS  | 7      | 100%     | -       |
| FAILED   | 0      | -        | -       |
+----------+--------+----------+---------+
*/


-- ============================================================================
-- 八、监控和诊断
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 8.1 策略覆盖分析
-- ----------------------------------------------------------------------------

SHOW CACHE POLICY COVERAGE FROM CATALOG catalog_name;
/*
返回示例:
+----------+--------+----------+--------+---------------+
| Database | Tables | Selected | Cached | Coverage      |
+----------+--------+----------+--------+---------------+
| dw       | 150    | 45       | 45     | 30% / 100%    |
| ods      | 80     | 25       | 23     | 31% / 92%     |
| app      | 30     | 15       | 15     | 50% / 100%    |
+----------+--------+----------+--------+---------------+
| TOTAL    | 260    | 85       | 83     | 33% / 98%     |
+----------+--------+----------+--------+---------------+

说明:
- Selected: 被 Policy 选中的表数量
- Cached: 实际生成 Schedule 并缓存的表数量
- Coverage: Selected/Total (选中率) 和 Cached/Selected (成功率)
*/

-- ----------------------------------------------------------------------------
-- 8.2 资源使用分析
-- ----------------------------------------------------------------------------

SHOW CACHE RESOURCE USAGE FROM CATALOG catalog_name
ORDER BY cache_size DESC;
/*
返回示例:
+----------+------------------+------------+----------+----------+
| Database | Table            | Cache Size | Priority | Refresh  |
+----------+------------------+------------+----------+----------+
| dw       | fact_orders      | 125GB      | HIGH     | 2m       |
| dw       | fact_sales       | 98GB       | HIGH     | 5m       |
| ods      | orders           | 85GB       | NORMAL   | 10m      |
+----------+------------------+------------+----------+----------+
*/

-- 按维度聚合
SHOW CACHE RESOURCE USAGE FROM CATALOG catalog_name
GROUP BY database, priority
WITH (
    METRICS = [
        'SUM(cache_size) as total_size',
        'COUNT(*) as table_count',
        'AVG(refresh_interval) as avg_interval'
    ]
);

-- ----------------------------------------------------------------------------
-- 8.3 性能分析
-- ----------------------------------------------------------------------------

-- Job 性能分析（时间窗口）
ANALYZE CACHE JOB PERFORMANCE 
FROM CATALOG catalog_name
WHERE start_time BETWEEN '2024-01-01' AND '2024-01-07'
GROUP BY DATE(start_time)
WITH (
    COMPUTE (
        avg_duration = AVG(duration),
        p95_duration = PERCENTILE(duration, 0.95),
        total_cache_size = SUM(cache_size_delta),
        avg_throughput = AVG(records_cached / duration)
    )
);

-- Schedule 健康度分析
ANALYZE CACHE SCHEDULE HEALTH FROM CATALOG catalog_name
WITH (suggest_optimization = true);
/*
返回分析报告:
Summary:
  Total Schedules: 87
  Healthy: 82 (94%)
  Warning: 3 (3%)
  Critical: 2 (2%)

Issues:
  [CRITICAL] Table 'dw.orders' has 15 consecutive failures
  [CRITICAL] Table 'ods.logs' avg duration (45m) > timeout (30m)
  [WARNING] Table 'dw.dim_users' success rate 92% (< 95% threshold)

Suggestions:
  1. Check incremental_query for 'dw.orders'
  2. Increase timeout for 'ods.logs' to 1h
  3. Review 'dw.dim_users' data quality issues
*/

-- ----------------------------------------------------------------------------
-- 8.4 智能诊断
-- ----------------------------------------------------------------------------

DIAGNOSE CACHE FROM CATALOG catalog_name
WITH (
    check_policy_conflicts = true,     -- 检查策略冲突
    check_resource_limits = true,      -- 检查资源限制
    check_schedule_anomalies = true,   -- 检查调度异常
    check_job_failures = true,         -- 检查执行失败
    suggest_optimization = true        -- 生成优化建议
);
/*
返回诊断报告:
Issues Found:
  [ERROR] Database 'dw' cache size (520GB) exceeds limit (500GB)
  [ERROR] Table 'dw.orders' has conflicting policies (DATABASE vs TABLE)
  [WARN]  Schedule 'schedule_logs' has 5 consecutive failures
  [WARN]  15 tables have refresh_interval < 5m (may cause high load)
  
Suggestions:
  1. Increase max_cache_size to 600GB or reduce cached tables
  2. Remove conflicting TABLE policy for 'dw.orders'
  3. Check data source connection for 'schedule_logs'
  4. Consider increasing refresh_interval for high-frequency tables
  5. Enable incremental mode for 3 full-refresh tables (save 40% time)
*/

-- ----------------------------------------------------------------------------
-- 8.5 策略验证
-- ----------------------------------------------------------------------------

VALIDATE CACHE POLICIES FROM CATALOG catalog_name;
/*
检查项:
- Catalog Policy 参数合法性
- Database/Table Policy 配置冲突
- 资源配额是否超限
- 增量配置是否正确（主键、增量字段）
- Schedule 调度频率是否合理

返回验证结果:
Validation Results:
  ✓ Catalog Policy configuration is valid
  ✓ Resource quota within limits (423GB / 500GB)
  ✗ 2 policy conflicts found
  ✗ 3 incremental configurations have issues
  ✓ All schedule frequencies are reasonable

Details:
  [CONFLICT] Table 'dw.orders' has both DATABASE and TABLE policy
  [CONFLICT] Table 'ods.logs' policy conflicts with parent
  [ERROR] Table 'dw.fact_sales' missing incremental_key for UPSERT mode
  [ERROR] Table 'app.users' incremental_column 'updated_at' does not exist
  [WARN] Table 'dw.dim_products' using FULL refresh for 100GB table
*/


-- ============================================================================
-- 九、高级特性
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 9.1 策略继承查询
-- ----------------------------------------------------------------------------

-- 查看完整继承链
SHOW CACHE POLICY INHERITANCE FROM TABLE catalog_name.db_name.table_name;
/*
返回层级继承关系:
Level 1: CATALOG hive_catalog
  - mode: SMART
  - max_cache_size: 500GB
  - default_priority: NORMAL
  - default_refresh_interval: 10m
  
Level 2: DATABASE hive_catalog.dw (inherits from CATALOG)
  - mode: ALL (override)
  - default_priority: HIGH (override)
  - default_refresh_interval: 5m (override)
  - max_cache_size: 500GB (inherited)
  
Level 3: TABLE hive_catalog.dw.orders (inherits from DATABASE)
  - enable_cache: true (explicit)
  - priority: HIGH (inherited from DATABASE.default_priority)
  - refresh_interval: 2m (override)
  - incremental_mode: UPSERT (explicit)
  - max_cache_size: 500GB (inherited from CATALOG)
*/

-- ----------------------------------------------------------------------------
-- 9.2 条件策略（动态配置）
-- ----------------------------------------------------------------------------

ALTER TABLE catalog_name.db_name.table_name
SET CACHE POLICY WITH (
    -- 条件表达式（运行时计算）
    priority = CASE 
        WHEN HOUR(CURRENT_TIME) BETWEEN 9 AND 18 THEN 'HIGH'
        ELSE 'NORMAL'
    END,
    
    refresh_interval = CASE
        WHEN DAY_OF_WEEK(CURRENT_DATE) IN (1, 7) THEN '1h'
        ELSE '10m'
    END,
    
    cache_partitions = CASE
        WHEN table_size < '10GB' THEN 'TRUE'
        ELSE 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
    END
);

-- ----------------------------------------------------------------------------
-- 9.3 策略模板
-- ----------------------------------------------------------------------------

-- 创建模板
CREATE CACHE POLICY TEMPLATE high_frequency_fact_table AS (
    priority = 'HIGH',
    refresh_interval = '2m',
    incremental_mode = 'UPSERT',
    auto_detect = true,
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- 应用模板
ALTER TABLE catalog_name.dw.fact_orders
SET CACHE POLICY FROM TEMPLATE high_frequency_fact_table;

-- 查看模板
SHOW CACHE POLICY TEMPLATES;

-- 删除模板
DROP CACHE POLICY TEMPLATE high_frequency_fact_table;

-- ----------------------------------------------------------------------------
-- 9.4 策略版本管理
-- ----------------------------------------------------------------------------

-- 保存当前策略为版本
SAVE CACHE POLICY VERSION 'v1.0_production'
FROM CATALOG catalog_name
WITH COMMENT 'Initial production configuration';

-- 查看历史版本
SHOW CACHE POLICY VERSIONS FROM CATALOG catalog_name;

-- 对比版本差异
DIFF CACHE POLICY VERSIONS 'v1.0_production' AND 'v2.0_optimized'
FROM CATALOG catalog_name;

-- 回滚到历史版本
RESTORE CACHE POLICY VERSION 'v1.0_production'
TO CATALOG catalog_name;


-- ============================================================================
-- 十、完整使用流程示例
-- ============================================================================

/*
场景：电商数据仓库缓存配置

需求:
1. 核心 dw 库需要高优先级 + 高频刷新
2. ods 库只缓存热点表
3. tmp 库完全禁用缓存
4. 订单表需要 2 分钟刷新
5. 日志表只保留 7 天数据
*/

-- Step 1: 设置 Catalog 全局策略（保守策略）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'SMART',
    max_tables = 100,
    max_cache_size = '500GB',
    min_access_frequency = 10,
    default_priority = 'NORMAL',
    default_refresh_interval = '10m'
);

-- Step 2: 设置核心库策略（dw 库高优先级）
ALTER DATABASE hive_catalog.dw
SET CACHE POLICY WITH (
    inherit_from_catalog = true,
    mode = 'ALL',
    default_priority = 'HIGH',
    default_refresh_interval = '5m',
    exclude_patterns = 'tmp_%'
);

-- Step 3: 设置 ODS 库策略（只缓存热点表）
ALTER DATABASE hive_catalog.ods
SET CACHE POLICY WITH (
    inherit_from_catalog = true,
    mode = 'TOPK',
    top_k = 20,
    rank_by = 'access_frequency'
);

-- Step 4: 禁用临时库
ALTER DATABASE hive_catalog.tmp
SET CACHE POLICY WITH (
    mode = 'NONE'
);

-- Step 5: 为核心订单表设置特殊策略
ALTER TABLE hive_catalog.dw.fact_orders 
SET CACHE POLICY WITH (
    enable_cache = true,
    priority = 'HIGH',
    refresh_interval = '2m',
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- Step 6: 为日志表设置策略
ALTER TABLE hive_catalog.ods.access_logs 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'APPEND',
    incremental_column = 'log_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

-- Step 7: 批量设置事实表策略
ALTER TABLES LIKE 'hive_catalog.dw.fact_%'
SET CACHE POLICY WITH (
    enable_cache = true,
    auto_detect = true,
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- Step 8: 应用策略（生成所有 Schedule）
APPLY CACHE POLICY FOR CATALOG hive_catalog;
/*
输出:
Evaluating policies...
  - CATALOG: SMART mode, max 100 tables
  - DATABASE 'dw': ALL mode, HIGH priority
  - DATABASE 'ods': TOPK mode, top 20 tables
  - DATABASE 'tmp': NONE mode (disabled)
  
Selected 87 tables:
  - dw: 45 tables (all tables, excluded tmp_*)
  - ods: 20 tables (top 20 by access frequency)
  - app: 22 tables (SMART selection)
  
Generated 87 schedules:
  - AUTO_GENERATED: 84 schedules
  - CUSTOM_POLICY: 3 schedules (fact_orders, access_logs, etc)
  
Scheduler started.
*/

-- Step 9: 验证配置
VALIDATE CACHE POLICIES FROM CATALOG hive_catalog;

-- Step 10: 查看策略覆盖情况
SHOW CACHE POLICY COVERAGE FROM CATALOG hive_catalog;

-- Step 11: 查看生成的 Schedule
SHOW CACHE SCHEDULES FROM CATALOG hive_catalog
ORDER BY priority DESC, next_run_time;

-- Step 12: 监控执行情况
-- 查看失败的 Job
SHOW CACHE JOBS WHERE status = 'FAILED'
ORDER BY start_time DESC
LIMIT 10;

-- 查看资源使用
SHOW CACHE RESOURCE USAGE FROM CATALOG hive_catalog;

-- Step 13: 定期诊断优化
DIAGNOSE CACHE FROM CATALOG hive_catalog
WITH (suggest_optimization = true);

-- Step 14: 根据建议调整策略（如果需要）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'SMART',
    max_tables = 120,                  -- 增加表数量
    max_cache_size = '600GB'           -- 增加配额
);

APPLY CACHE POLICY FOR CATALOG hive_catalog 
WITH (force_reevaluate = true);


-- ============================================================================
-- 十一、快速参考
-- ============================================================================

/*
-- 策略层级优先级（高到低）
TABLE POLICY > DATABASE POLICY > CATALOG POLICY

-- 常用命令速查

1. 设置策略
   ALTER CATALOG xxx SET CACHE POLICY WITH (...)
   ALTER DATABASE xxx SET CACHE POLICY WITH (...)
   ALTER TABLE xxx SET CACHE POLICY WITH (...)

2. 查看策略
   SHOW CACHE POLICY FROM CATALOG/DATABASE/TABLE xxx
   SHOW CACHE POLICY EFFECTIVE FROM xxx  -- 查看生效配置（含继承）

3. 应用策略
   APPLY CACHE POLICY FOR CATALOG/DATABASE/TABLE xxx

4. 查看 Schedule
   SHOW CACHE SCHEDULE FROM TABLE xxx
   SHOW CACHE SCHEDULES FROM CATALOG xxx

5. 查看 Job
   SHOW CACHE JOB job_id
   SHOW CACHE JOBS FROM TABLE xxx
   SHOW CACHE JOB LOGS/METRICS FROM job_id

6. 手动刷新
   REFRESH CACHE FROM TABLE xxx
   REFRESH CACHE FROM CATALOG xxx

7. 监控诊断
   SHOW CACHE POLICY COVERAGE FROM CATALOG xxx
   SHOW CACHE RESOURCE USAGE FROM CATALOG xxx
   DIAGNOSE CACHE FROM CATALOG xxx
   ANALYZE CACHE JOB PERFORMANCE FROM CATALOG xxx

-- Catalog Policy 模式选择

资源充足 → mode = 'ALL'
资源有限 → mode = 'TOPK' (top_k = N)
明确列表 → mode = 'SELECT' (include_patterns = '...')
自动选择 → mode = 'SMART' (推荐)
禁用缓存 → mode = 'NONE'

-- 增量模式选择

日志表（只追加）→ incremental_mode = 'APPEND'
业务表（可更新）→ incremental_mode = 'UPSERT' (需要 incremental_key)
复杂逻辑 → incremental_mode = 'CUSTOM' (自定义 SQL)
自动检测 → incremental_mode = 'AUTO' + auto_detect = true

-- 典型场景配置模板

1. 核心库（高优先级 + 高频刷新）
   ALTER DATABASE catalog.dw SET CACHE POLICY WITH (
       mode = 'ALL',
       default_priority = 'HIGH',
       default_refresh_interval = '5m'
   );

2. 临时库（完全禁用）
   ALTER DATABASE catalog.tmp SET CACHE POLICY WITH (mode = 'NONE');

3. 归档库（低优先级 + 低频刷新）
   ALTER DATABASE catalog.archive SET CACHE POLICY WITH (
       mode = 'TOPK', top_k = 10,
       default_priority = 'LOW',
       default_refresh_interval = '1h'
   );

4. 核心表（超高频刷新）
   ALTER TABLE catalog.db.important_table SET CACHE POLICY WITH (
       priority = 'HIGH',
       refresh_interval = '2m',
       incremental_mode = 'UPSERT',
       auto_detect = true
   );
*/


-- ============================================================================
-- 十二、设计优势总结
-- ============================================================================

/*
1. 三层策略继承体系
   ✓ 清晰的配置层级：CATALOG → DATABASE → TABLE
   ✓ 灵活的覆盖机制：子层级可覆盖父层级
   ✓ 明确的继承来源：查询时显示 source 标识

2. DATABASE 层级的价值
   ✓ 避免重复配置：一条 SQL 配置整个库
   ✓ 典型场景支持：临时库禁用、核心库高优先级
   ✓ 不增加复杂度：可选配置，默认继承 Catalog

3. SCHEDULE vs JOB 分离
   ✓ 职责清晰：Schedule = 计划，Job = 执行
   ✓ 查询方便：历史执行记录完整保留
   ✓ 符合标准：调度系统的通用设计模式

4. 图灵完备性
   ✓ 条件表达式：支持 CASE WHEN
   ✓ 自定义函数：支持 UDF
   ✓ 计算属性：支持派生字段
   ✓ 策略模板：可复用配置

5. 解析器友好
   ✓ 关键字明确：POLICY / SCHEDULE / JOB
   ✓ 层级清晰：FROM CATALOG / DATABASE / TABLE
   ✓ 语义化操作：SET / APPLY / REFRESH / DIAGNOSE

6. 用户友好
   ✓ 90% 场景：只需设置 Catalog Policy
   ✓ 9% 场景：为特殊库/表添加策略
   ✓ 1% 场景：使用高级特性（模板、版本管理等）
   ✓ 自动检测：auto_detect 简化配置
   ✓ 智能诊断：DIAGNOSE 给出优化建议

7. 可维护性
   ✓ 策略与执行分离：调试友好
   ✓ 完整的监控体系：覆盖率、健康度、性能分析
   ✓ 版本管理：支持回滚和对比
   ✓ 验证机制：VALIDATE 检查配置合法性
*/

-- ============================================================================
-- 文档结束
-- ============================================================================
