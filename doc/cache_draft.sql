-- ============================================================================
-- Remote Catalog Cache Management SQL - Refined Policy Architecture
-- 精化的策略驱动远程 Catalog 缓存管理 SQL 语法定义
-- ============================================================================

-- ============================================================================
-- 架构设计理念（优化版）
-- ============================================================================
/*
核心思想: Two-Level Policy Architecture (两层策略架构)

1. 设计原则
   ┌──────────────────────────────────────────────────────────────┐
   │ CATALOG CACHE POLICY (Catalog 级策略) - 宏观控制             │
   │   - 定义"哪些表需要缓存"（缓存范围策略）                      │
   │   - 策略模式: SMART | TOPK | SELECT | ALL | NONE              │
   │   - 全局资源配额和优先级                                       │
   │   - 只关心大方向，不涉及增量等执行细节                         │
   └──────────────────────────────────────────────────────────────┘
                              ↓ 作用于
   ┌──────────────────────────────────────────────────────────────┐
   │ TABLE CACHE POLICY (Table 级策略) - 微观控制                  │
   │   - 定义"如何缓存这张表"（缓存执行策略）                      │
   │   - 增量配置: incremental_mode, incremental_key, etc.         │
   │   - 刷新配置: refresh_interval, priority                      │
   │   - 缓存范围: cache_columns, cache_partitions                 │
   │   - 只有被 Catalog Policy 选中的表才会生效                    │
   └──────────────────────────────────────────────────────────────┘
                              ↓ 自动生成
   ┌──────────────────────────────────────────────────────────────┐
   │ CACHE TASK (执行层) - 系统管理                                │
   │   - 从 Catalog Policy + Table Policy 派生                     │
   │   - 系统自动生成，用户只读                                     │
   │   - 负责实际的缓存刷新执行                                     │
   └──────────────────────────────────────────────────────────────┘

2. 两层策略的职责分工
   
   Catalog Policy (做什么)        Table Policy (怎么做)
   ├─ 缓存范围选择                ├─ 增量更新配置
   ├─ 全局资源配额                ├─ 刷新频率
   ├─ 默认优先级                  ├─ 缓存列/分区范围
   └─ 智能选择算法                └─ 自定义增量逻辑

3. 为什么不要 Database 级别策略？
   ✓ 避免三层继承的复杂性
   ✓ Catalog 策略已经可以通过模式匹配控制 Database
   ✓ Table 策略提供了足够的灵活性
   ✓ 两层架构更清晰：范围选择 vs 执行细节

4. Catalog Policy 的 4 种模式
   
   SMART - 智能选择（推荐）
     系统根据表的访问频率、大小、更新频率自动选择
     适合: 大部分场景
   
   TOPK - 按规则选择 Top K 张表
     基于访问频率/大小/更新频率排序，选择 Top K
     适合: 资源有限，只缓存热点表
   
   SELECT - 明确指定要缓存的表
     通过模式匹配或显式列表指定
     适合: 明确知道要缓存哪些表
   
   ALL - 缓存所有表
     所有表都缓存（资源允许的情况下）
     适合: 资源充足的场景
   
   NONE - 不缓存（禁用）
     关闭整个 Catalog 的缓存
     适合: 临时禁用或测试
*/


-- ============================================================================
-- 一、CATALOG CACHE POLICY 管理（宏观策略 - 决定缓存范围）
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1.1 设置 Catalog 缓存策略
-- ----------------------------------------------------------------------------

-- 模式 1: SMART - 智能选择（推荐，默认）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'SMART',                    -- 智能选择模式
    
    -- 智能选择参数
    max_tables = 100,                  -- 最多缓存 100 张表
    max_cache_size = '500GB',          -- 全局缓存大小上限
    min_access_frequency = 10,         -- 最小访问频率（次/小时）
    consider_table_size = true,        -- 考虑表大小
    consider_update_frequency = true,  -- 考虑更新频率
    
    -- 评分权重（可选，调优用）
    score_weights = '{
        "access_frequency": 0.5,
        "table_size": 0.2,
        "update_frequency": 0.3
    }',
    
    -- 全局默认配置
    default_priority = 'NORMAL',       -- 默认优先级
    default_refresh_interval = '10m'   -- 默认刷新间隔
);

-- 模式 2: TOPK - 按规则选择 Top K
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'TOPK',
    
    -- Top K 参数
    top_k = 50,                        -- 选择访问最频繁的 50 张表
    rank_by = 'access_frequency',      -- 排序依据: access_frequency | table_size | update_frequency
    rank_order = 'DESC',               -- 排序方向: DESC | ASC
    
    -- 过滤条件（可选）
    filter_min_size = '1GB',           -- 只考虑大于 1GB 的表
    filter_databases = 'dw,ods',       -- 只考虑这些数据库
    exclude_tables = 'tmp_%,test_%',   -- 排除临时表和测试表
    
    -- 全局配置
    max_cache_size = '500GB',
    default_priority = 'NORMAL'
);

-- 模式 3: SELECT - 明确指定表（精确控制）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'SELECT',
    
    -- 方式 1: 模式匹配（推荐）
    include_patterns = 'dw.fact_%,dw.dim_%,ods.order_%',
    exclude_patterns = 'dw.fact_tmp_%,ods.%_backup',
    
    -- 方式 2: 显式列表（可选，与 patterns 二选一）
    -- include_tables = 'db1.table1,db2.table2,db3.table3',
    
    -- 全局配置
    max_cache_size = '500GB',
    default_priority = 'NORMAL'
);

-- 模式 4: ALL - 缓存所有表（资源充足时）
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'ALL',
    
    -- 排除规则（可选）
    exclude_databases = 'tmp,test',    -- 排除某些数据库
    exclude_patterns = 'tmp_%,test_%', -- 排除临时表
    
    -- 全局配置
    max_cache_size = '1TB',            -- 需要足够大的配额
    default_priority = 'NORMAL'
);

-- 模式 5: NONE - 禁用缓存
ALTER CATALOG catalog_name 
SET CACHE POLICY WITH (
    mode = 'NONE'                      -- 关闭所有缓存
);


-- ----------------------------------------------------------------------------
-- 1.2 查看 Catalog 策略
-- ----------------------------------------------------------------------------

-- 查看策略配置
SHOW CACHE POLICY FROM CATALOG catalog_name;
/*
返回示例（SMART 模式）:
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

-- 查看所有 Catalog 策略
SHOW CACHE POLICIES;

-- 查看策略选中的表
SHOW CACHE POLICY SELECTED_TABLES FROM CATALOG catalog_name;
/*
返回被 Catalog Policy 选中的表列表:
+------------------+------------------+------------+--------+
| Database         | Table            | Score      | Reason |
+------------------+------------------+------------+--------+
| dw               | fact_orders      | 95.3       | HIGH_ACCESS + FREQUENT_UPDATE |
| dw               | fact_sales       | 87.2       | HIGH_ACCESS |
| ods              | orders           | 76.5       | MEDIUM_ACCESS + LARGE_SIZE |
+------------------+------------------+------------+--------+
*/


-- ----------------------------------------------------------------------------
-- 1.3 删除 Catalog 策略
-- ----------------------------------------------------------------------------

ALTER CATALOG catalog_name UNSET CACHE POLICY;
-- 恢复到系统默认（通常是 SMART 模式）


-- ============================================================================
-- 二、TABLE CACHE POLICY 管理（微观策略 - 决定如何缓存）
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 2.1 设置 Table 缓存策略
-- ----------------------------------------------------------------------------

-- 基础配置（使用自动检测，最简单）
ALTER TABLE catalog_name.db_name.table_name 
SET CACHE POLICY WITH (
    enable_cache = true,               -- 强制启用缓存（覆盖 Catalog 选择）
    auto_detect = true                 -- 自动检测增量字段等配置
);

-- 完整配置（手动指定所有参数）
ALTER TABLE catalog_name.db_name.table_name 
SET CACHE POLICY WITH (
    -- 基础配置
    enable_cache = true,               -- 是否缓存此表
    priority = 'HIGH',                 -- 优先级: LOW | NORMAL | HIGH
    
    -- 刷新配置
    refresh_mode = 'SCHEDULED',        -- SCHEDULED(定时) | MANUAL(手动)
    refresh_interval = '5m',           -- 刷新间隔
    
    -- 增量更新配置
    enable_incremental = true,         -- 启用增量更新
    incremental_mode = 'UPSERT',       -- APPEND | UPSERT | CUSTOM
    incremental_key = 'order_id',      -- 主键（UPSERT 必需）
    incremental_column = 'update_time',-- 增量判断字段
    
    -- 缓存范围配置
    cache_columns = 'id,name,status,create_time',  -- 只缓存这些列
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY',
    
    -- 高级配置
    max_retry = 3,
    timeout = '30m'
);

-- 增量模式示例

-- 示例 1: APPEND 模式（日志表）
ALTER TABLE catalog_name.logs.access_logs 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'APPEND',       -- 只追加，不更新
    incremental_column = 'log_time',   -- 无需主键
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

-- 示例 2: UPSERT 模式（业务表）
ALTER TABLE catalog_name.dw.orders 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'UPSERT',       -- 支持更新和插入
    incremental_key = 'order_id',      -- 必需主键
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
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

-- 示例 5: 自动检测（推荐，最简单）
ALTER TABLE catalog_name.db_name.any_table 
SET CACHE POLICY WITH (
    enable_cache = true,
    auto_detect = true,                -- 系统自动检测所有配置
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);


-- ----------------------------------------------------------------------------
-- 2.2 批量设置 Table 策略
-- ----------------------------------------------------------------------------

-- 为多张表设置相同策略
ALTER TABLES LIKE 'catalog_name.dw.fact_%'
SET CACHE POLICY WITH (
    enable_cache = true,
    priority = 'HIGH',
    auto_detect = true
);


-- ----------------------------------------------------------------------------
-- 2.3 查看 Table 策略
-- ----------------------------------------------------------------------------

-- 查看单表策略
SHOW CACHE POLICY FROM TABLE catalog_name.db_name.table_name;
/*
返回示例:
+-------------------+----------------------------------+--------------------+
| Property          | Value                            | Source             |
+-------------------+----------------------------------+--------------------+
| enable_cache      | true                             | TABLE              |
| priority          | HIGH                             | TABLE              |
| refresh_interval  | 5m                               | TABLE              |
| incremental_mode  | UPSERT                           | TABLE              |
| incremental_key   | order_id                         | TABLE              |
| default_priority  | NORMAL                           | CATALOG (default)  |
+-------------------+----------------------------------+--------------------+
*/

-- 查看所有配置了策略的表
SHOW CACHE POLICIES FROM CATALOG catalog_name WHERE level = 'TABLE';


-- ----------------------------------------------------------------------------
-- 2.4 删除 Table 策略
-- ----------------------------------------------------------------------------

ALTER TABLE catalog_name.db_name.table_name UNSET CACHE POLICY;
-- 删除后，此表是否缓存完全由 Catalog Policy 决定


-- ----------------------------------------------------------------------------
-- 2.5 查看表元数据（用于自动检测）
-- ----------------------------------------------------------------------------

SHOW TABLE METADATA FROM catalog_name.db_name.table_name;
/*
返回自动检测的结果:
+------------------------+----------------------------------+
| Property               | Value                            |
+------------------------+----------------------------------+
| is_partitioned         | true                             |
| partition_columns      | [dt]                             |
| primary_key            | [order_id]                       |
| detected_timestamp_cols| [update_time, create_time]       |
| recommended_incremental_mode | UPSERT                     |
| table_size             | 125GB                            |
| row_count              | 150000000                        |
| update_frequency       | 5000 rows/min                    |
| access_frequency       | 120 queries/hour                 |
+------------------------+----------------------------------+
*/


-- ============================================================================
-- 三、策略生效和 Task 管理
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 3.1 应用策略（生成或更新 Cache Task）
-- ----------------------------------------------------------------------------

-- 应用整个 Catalog 策略
APPLY CACHE POLICY FOR CATALOG catalog_name;
/*
执行逻辑:
1. 根据 Catalog Policy 选择要缓存的表
2. 对于每张选中的表:
   - 检查是否有 Table Policy
   - 如果有，使用 Table Policy 配置
   - 如果没有，使用 Catalog 默认配置 + 自动检测
3. 生成或更新 Cache Task
4. 启动调度任务
*/

-- 应用单表策略
APPLY CACHE POLICY FOR TABLE catalog_name.db_name.table_name;

-- 强制重新评估（重新计算 SMART/TOPK 选择）
APPLY CACHE POLICY FOR CATALOG catalog_name WITH (force_reevaluate = true);


-- ----------------------------------------------------------------------------
-- 3.2 查看 Cache Task（只读，系统自动管理）
-- ----------------------------------------------------------------------------

-- 查看单表 Task
SHOW CACHE TASK FROM TABLE catalog_name.db_name.table_name;
/*
返回示例:
+-------------------+----------------------------------+
| Property          | Value                            |
+-------------------+----------------------------------+
| task_id           | task_orders_auto_20240101        |
| task_type         | AUTO_GENERATED                   |
| source_policy     | CATALOG:SMART + TABLE:orders     |
| status            | RUNNING                          |
| selected_by       | CATALOG_POLICY (score: 95.3)     |
| refresh_mode      | SCHEDULED                        |
| refresh_interval  | 5m                               |
| next_run_time     | 2024-01-01 10:05:00             |
| incremental_mode  | UPSERT                           |
| incremental_key   | order_id                         |
| last_refresh_time | 2024-01-01 10:00:00             |
| last_refresh_mode | INCREMENTAL                      |
| cache_size        | 5.2GB                            |
| row_count         | 8500000                          |
+-------------------+----------------------------------+
*/

-- 查看所有 Task
SHOW CACHE TASKS FROM CATALOG catalog_name;
SHOW CACHE TASKS WHERE status = 'RUNNING';

-- 查看 Task 执行历史
SHOW CACHE HISTORY FROM TABLE catalog_name.db_name.table_name
WHERE start_time > CURRENT_DATE - INTERVAL 1 DAY
ORDER BY start_time DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 3.3 Task 临时控制（不影响 Policy）
-- ----------------------------------------------------------------------------

-- 临时暂停/恢复
PAUSE CACHE FOR TABLE catalog_name.db_name.table_name;
RESUME CACHE FOR TABLE catalog_name.db_name.table_name;

-- 临时禁用/启用（需要手动恢复）
DISABLE CACHE FOR TABLE catalog_name.db_name.table_name;
ENABLE CACHE FOR TABLE catalog_name.db_name.table_name;


-- ----------------------------------------------------------------------------
-- 3.4 自定义 Task（高级用户，完全绕过 Policy）
-- ----------------------------------------------------------------------------

-- 创建自定义 Task
CREATE CUSTOM CACHE TASK task_name
ON TABLE catalog_name.db_name.table_name
WITH (
    refresh_mode = 'MANUAL',
    incremental_mode = 'CUSTOM',
    incremental_query = '''
        SELECT * FROM ${table}
        WHERE complex_condition(...)
    '''
);

-- 删除自定义 Task（恢复为 Policy 驱动）
DROP CUSTOM CACHE TASK task_name;


-- ============================================================================
-- 四、数据刷新
-- ============================================================================

-- 刷新单表（触发 Task 执行）
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name;

-- 带选项的刷新
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name
WITH (
    mode = 'FULL',                     -- FULL(全量) | INCREMENTAL(增量)
    partitions = 'dt=20240101',        -- 指定分区
    async = true                       -- 异步执行
);

-- 刷新整个 Catalog
REFRESH CACHE FROM CATALOG catalog_name
WITH (parallel = 8);


-- ============================================================================
-- 五、监控和诊断
-- ============================================================================

-- 查看 Catalog Policy 覆盖情况
SHOW CACHE POLICY COVERAGE FROM CATALOG catalog_name;
/*
返回示例:
+------------------+--------+----------+------------+--------------+
| Database         | Tables | Selected | Cached     | Coverage     |
+------------------+--------+----------+------------+--------------+
| dw               | 150    | 45       | 45         | 30% / 100%   |
| ods              | 80     | 25       | 23         | 31% / 92%    |
| app              | 30     | 15       | 15         | 50% / 100%   |
+------------------+--------+----------+------------+--------------+
| TOTAL            | 260    | 85       | 83         | 33% / 98%    |
+------------------+--------+----------+------------+--------------+

说明:
- Selected: 被 Catalog Policy 选中的表数量
- Cached: 实际生成 Task 并缓存的表数量
- Coverage: Selected/Total (选中率) 和 Cached/Selected (缓存成功率)
*/

-- 查看策略选择详情（SMART/TOPK 模式）
SHOW CACHE POLICY SELECTION_DETAILS FROM CATALOG catalog_name
ORDER BY score DESC
LIMIT 50;
/*
返回示例:
+------------------+------------------+-------+--------+----------+----------+
| Database         | Table            | Score | Access | Size     | Selected |
+------------------+------------------+-------+--------+----------+----------+
| dw               | fact_orders      | 95.3  | 150/h  | 125GB    | YES      |
| dw               | fact_sales       | 87.2  | 120/h  | 98GB     | YES      |
| ods              | orders           | 76.5  | 80/h   | 200GB    | YES      |
| dw               | dim_users        | 45.2  | 30/h   | 5GB      | NO       |
+------------------+------------------+-------+--------+----------+----------+
*/

-- 查看缓存使用情况
SHOW CACHE USAGE FROM CATALOG catalog_name
ORDER BY cache_size DESC;

-- 查看失败的 Task
SHOW CACHE TASKS WHERE status = 'FAILED'
ORDER BY failure_time DESC;

-- 分析性能并给出优化建议
ANALYZE CACHE PERFORMANCE FROM CATALOG catalog_name
WITH (suggest_optimization = true);
/*
可能的建议:
- 调整 Catalog Policy 参数（max_tables, min_access_frequency）
- 为高访问表设置 Table Policy（更频繁刷新）
- 增加资源配额
- 优化增量配置
*/

-- 验证策略配置
VALIDATE CACHE POLICIES FROM CATALOG catalog_name;
/*
检查项:
- Catalog Policy 参数合法性
- Table Policy 配置冲突
- 资源配额是否超限
- 增量配置是否正确
*/


-- ============================================================================
-- 六、完整使用流程示例
-- ============================================================================

/*
===============================================================================
场景: 电商数据仓库缓存配置
===============================================================================

需求:
1. 核心事实表和维度表需要缓存
2. 资源有限，只能缓存最热的 50 张表
3. 订单表需要高频刷新（2分钟）
4. 日志表只需要最近 7 天的数据
*/

-- Step 1: 设置 Catalog 策略（选择要缓存的表）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'TOPK',                     -- 选择最热的表
    top_k = 50,                        -- 最多 50 张表
    rank_by = 'access_frequency',      -- 按访问频率排序
    filter_databases = 'dw,ods',       -- 只考虑这两个库
    exclude_patterns = 'tmp_%,test_%', -- 排除临时表
    max_cache_size = '500GB',          -- 资源配额
    default_refresh_interval = '10m'   -- 默认 10 分钟刷新
);

-- Step 2: 为核心订单表设置特殊策略
ALTER TABLE hive_catalog.dw.fact_orders 
SET CACHE POLICY WITH (
    enable_cache = true,               -- 强制缓存（即使不在 Top 50）
    priority = 'HIGH',
    refresh_interval = '2m',           -- 2 分钟刷新
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- Step 3: 为日志表设置策略
ALTER TABLE hive_catalog.ods.access_logs 
SET CACHE POLICY WITH (
    enable_cache = true,
    incremental_mode = 'APPEND',       -- 只追加
    incremental_column = 'log_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

-- Step 4: 为其他事实表批量设置策略（使用自动检测）
ALTER TABLES LIKE 'hive_catalog.dw.fact_%'
SET CACHE POLICY WITH (
    enable_cache = true,
    auto_detect = true,                -- 自动检测配置
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- Step 5: 应用策略（自动生成所有 Task）
APPLY CACHE POLICY FOR CATALOG hive_catalog;
/*
输出:
Selected 50 tables based on access frequency.
Generated 50 cache tasks.
Applied custom policies for 3 tables.
Started scheduled refresh jobs.
*/

-- Step 6: 查看策略生效情况
SHOW CACHE POLICY COVERAGE FROM CATALOG hive_catalog;
SHOW CACHE POLICY SELECTED_TABLES FROM CATALOG hive_catalog;

-- Step 7: 查看生成的 Task
SHOW CACHE TASKS FROM CATALOG hive_catalog;

-- Step 8: 系统自动运行，用户只需监控
SHOW CACHE TASKS WHERE status = 'FAILED';
SHOW CACHE USAGE FROM CATALOG hive_catalog;

-- Step 9: 定期查看性能建议
ANALYZE CACHE PERFORMANCE FROM CATALOG hive_catalog
WITH (suggest_optimization = true);

-- Step 10: 根据建议调整策略（如果需要）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'TOPK',
    top_k = 60,                        -- 增加到 60 张表
    max_cache_size = '600GB'           -- 增加配额
);
APPLY CACHE POLICY FOR CATALOG hive_catalog WITH (force_reevaluate = true);


-- ============================================================================
-- 七、策略模式对比和选择建议
-- ============================================================================

/*
+----------+----------------+------------------+------------------+
| Mode     | 适用场景        | 优点              | 缺点              |
+----------+----------------+------------------+------------------+
| SMART    | 大部分场景      | 自动化程度高      | 算法需要调优      |
|          | 不确定哪些表热   | 动态适应变化      | 需要统计数据      |
|          |                |                  |                  |
| TOPK     | 资源有限        | 简单直观          | 需要选择排序依据  |
|          | 只缓存热点表     | 可预测            | 可能错过重要表    |
|          |                |                  |                  |
| SELECT   | 明确知道要缓存   | 完全可控          | 需要手动维护      |
|          | 稳定的表列表     | 精确控制          | 无法动态适应      |
|          |                |                  |                  |
| ALL      | 资源充足        | 覆盖全面          | 资源消耗大        |
|          | 所有表都重要     | 配置简单          | 可能浪费资源      |
|          |                |                  |                  |
| NONE     | 临时禁用缓存     | 立即生效          | 需要重新启用      |
+----------+----------------+------------------+------------------+

推荐策略:
1. 初期: 使用 SMART 模式，让系统自动选择
2. 优化: 根据监控结果调整 SMART 参数
3. 精细: 为核心表添加 Table Policy
4. 特殊: 个别表使用 Custom Task
*/


-- ============================================================================
-- 八、系统架构优势总结
-- ============================================================================

/*
✅ 两层架构的优势

1. 职责清晰
   - Catalog Policy: 回答"缓存哪些表"（范围选择）
   - Table Policy: 回答"怎么缓存"（执行细节）
   - 两者互不干扰，各司其职

2. 用户友好
   - 90% 场景: 只需设置 Catalog Policy（SMART/TOPK）
   - 9% 场景: 为核心表添加 Table Policy
   - 1% 场景: 使用 Custom Task

3. 灵活性强
   - Catalog Policy 提供 4 种模式（SMART/TOPK/SELECT/ALL）
   - Table Policy 支持增量、分区、自定义等细节配置
   - Custom Task 支持完全自定义逻辑

4. 易于维护
   - Catalog Policy 调整自动传播到所有表
   - Table Policy 只影响单表，互不干扰
   - 策略和执行分离，调试友好

5. 性能优化
   - SMART/TOPK 避免缓存冷数据
   - Table Policy 精细控制刷新频率
   - 资源配额防止过载

6. 无 Database 层的原因
   - Catalog Policy 可通过模式匹配控制特定 Database
     例: filter_databases = 'dw,ods'
   - 避免三层继承的复杂性（Catalog -> Database -> Table）
   - 两层已经提供足够的灵活性
   - 更简单、更清晰的架构
*/


-- ============================================================================
-- 九、Catalog Policy 四种模式详细说明
-- ============================================================================

/*
===============================================================================
模式 1: SMART - 智能选择（推荐）
===============================================================================

工作原理:
  系统根据多个维度评分，自动选择最值得缓存的表

评分维度:
  1. access_frequency (访问频率)
     - 统计最近 24 小时的查询次数
     - 高频访问的表得分高
  
  2. table_size (表大小)
     - 中等大小的表得分高
     - 过小的表缓存收益不大
     - 过大的表消耗资源多
  
  3. update_frequency (更新频率)
     - 更新适中的表得分高
     - 更新太频繁增加刷新成本
     - 从不更新的表可能是静态数据

评分公式:
  score = w1 * normalize(access_frequency)
        + w2 * normalize(table_size_factor)  
        + w3 * normalize(update_frequency_factor)

示例配置:
*/
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'SMART',
    max_tables = 100,
    max_cache_size = '500GB',
    min_access_frequency = 10,         -- 至少 10 次/小时
    score_weights = '{
        "access_frequency": 0.5,       -- 访问频率权重 50%
        "table_size": 0.2,             -- 表大小权重 20%
        "update_frequency": 0.3        -- 更新频率权重 30%
    }'
);

/*
适合场景:
  ✓ 大部分生产环境（推荐默认模式）
  ✓ 表访问模式稳定
  ✓ 希望系统自动优化

优点:
  ✓ 自动适应访问模式变化
  ✓ 多维度评估，更合理
  ✓ 可通过权重调优

缺点:
  ✗ 需要收集统计数据
  ✗ 算法需要调优才能达到最佳效果


===============================================================================
模式 2: TOPK - 按规则选择 Top K
===============================================================================

工作原理:
  按单一维度排序，选择 Top K 张表

支持的排序维度:
  - access_frequency: 访问最频繁的 K 张表
  - table_size: 最大/最小的 K 张表
  - update_frequency: 更新最频繁/最少的 K 张表

示例配置:
*/
-- 示例 1: 选择访问最频繁的 50 张表
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'TOPK',
    top_k = 50,
    rank_by = 'access_frequency',
    rank_order = 'DESC',               -- 降序（最频繁的在前）
    filter_databases = 'dw,ods'        -- 只考虑这些库
);

-- 示例 2: 选择最大的 30 张表（大表优先缓存）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'TOPK',
    top_k = 30,
    rank_by = 'table_size',
    rank_order = 'DESC',
    filter_min_size = '10GB'           -- 只考虑大于 10GB 的表
);

/*
适合场景:
  ✓ 资源有限，只能缓存热点表
  ✓ 明确的排序标准
  ✓ 简单直观的选择逻辑

优点:
  ✓ 逻辑简单，易于理解
  ✓ 结果可预测
  ✓ 配置直观

缺点:
  ✗ 单一维度可能错过重要表
  ✗ 无法自动适应多变的访问模式


===============================================================================
模式 3: SELECT - 明确指定表
===============================================================================

工作原理:
  通过模式匹配或显式列表指定要缓存的表

支持两种方式:
  1. 模式匹配（推荐）: 使用通配符
  2. 显式列表: 明确列出表名

示例配置:
*/
-- 方式 1: 模式匹配（灵活）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'SELECT',
    include_patterns = 'dw.fact_%,dw.dim_%,ods.order_%',
    exclude_patterns = 'dw.fact_tmp_%,*.%_backup'
);

-- 方式 2: 显式列表（精确）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'SELECT',
    include_tables = 'dw.fact_orders,dw.fact_sales,ods.orders'
);

-- 方式 3: 混合使用
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'SELECT',
    include_patterns = 'dw.fact_%',    -- 所有事实表
    include_tables = 'ods.orders,app.users',  -- 加上这两张表
    exclude_patterns = '*_tmp'         -- 排除临时表
);

/*
适合场景:
  ✓ 明确知道要缓存哪些表
  ✓ 表列表相对稳定
  ✓ 需要精确控制

优点:
  ✓ 完全可控
  ✓ 结果确定
  ✓ 适合规范化命名的表

缺点:
  ✗ 需要手动维护
  ✗ 新表需要更新配置
  ✗ 无法自动适应变化


===============================================================================
模式 4: ALL - 缓存所有表
===============================================================================

工作原理:
  缓存 Catalog 下所有表（可排除部分表）

示例配置:
*/
-- 缓存所有表
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'ALL',
    max_cache_size = '1TB'
);

-- 缓存所有表（排除临时和测试数据）
ALTER CATALOG hive_catalog 
SET CACHE POLICY WITH (
    mode = 'ALL',
    exclude_databases = 'tmp,test',
    exclude_patterns = 'tmp_%,test_%,%_backup',
    max_cache_size = '1TB'
);

/*
适合场景:
  ✓ 资源充足（大容量 SSD/内存）
  ✓ 所有表都重要
  ✓ 追求极致查询性能

优点:
  ✓ 覆盖全面
  ✓ 配置简单
  ✓ 无遗漏

缺点:
  ✗ 资源消耗大
  ✗ 可能缓存冷数据
  ✗ 刷新开销大


===============================================================================
模式选择决策树
===============================================================================

资源充足？
├─ YES → 使用 ALL 模式
│   └─ 配置 exclude_patterns 排除不需要的表
│
└─ NO → 资源有限，需要选择
    │
    ├─ 明确知道要缓存哪些表？
    │   ├─ YES → 使用 SELECT 模式
    │   │   └─ 使用 include_patterns 或 include_tables
    │   │
    │   └─ NO → 需要自动选择
    │       │
    │       ├─ 希望多维度智能选择？
    │       │   └─ YES → 使用 SMART 模式（推荐）
    │       │       └─ 调整 score_weights 优化
    │       │
    │       └─ 只关心单一维度？
    │           └─ YES → 使用 TOPK 模式
    │               └─ 选择 rank_by (access_frequency/table_size)
*/


-- ============================================================================
-- 十、典型配置模板
-- ============================================================================

-- 模板 1: 小型团队（资源有限）
ALTER CATALOG my_catalog 
SET CACHE POLICY WITH (
    mode = 'TOPK',
    top_k = 20,                        -- 只缓存最热的 20 张表
    rank_by = 'access_frequency',
    max_cache_size = '100GB'
);

-- 模板 2: 中型企业（平衡性能和成本）
ALTER CATALOG my_catalog 
SET CACHE POLICY WITH (
    mode = 'SMART',
    max_tables = 100,
    max_cache_size = '500GB',
    min_access_frequency = 10
);

-- 模板 3: 大型企业（资源充足）
ALTER CATALOG my_catalog 
SET CACHE POLICY WITH (
    mode = 'ALL',
    exclude_databases = 'tmp,test',
    max_cache_size = '2TB'
);

-- 模板 4: 明确的核心表
ALTER CATALOG my_catalog 
SET CACHE POLICY WITH (
    mode = 'SELECT',
    include_patterns = 'prod.fact_%,prod.dim_%'
);


-- ============================================================================
-- 结束
-- ============================================================================
