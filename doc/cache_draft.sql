-- ============================================================================
-- Remote Catalog Cache Management SQL Syntax Definition
-- 用于管理远程 Catalog 本地缓存任务配置的 SQL 语法定义
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CATALOG 级别缓存策略管理
-- ----------------------------------------------------------------------------

-- 1.1 查看 Catalog 级别缓存策略
SHOW CACHE POLICY FROM CATALOG catalog_name;

-- 查看所有 Catalog 的缓存策略
SHOW CACHE POLICIES;

-- 1.2 创建/修改 Catalog 级别缓存策略
ALTER CATALOG catalog_name 
SET CACHE POLICY (
    refresh_interval = '5m',           -- 刷新间隔: 支持 s/m/h/d (秒/分/小时/天)
    refresh_mode = 'SCHEDULED',        -- 刷新模式: SCHEDULED(定时) | MANUAL(手动) | AUTO(自动)
    cache_ttl = '1h',                  -- 缓存生存时间
    enable_cache = true,               -- 是否启用缓存
    max_cache_size = '10GB',           -- 最大缓存大小
    priority = 'NORMAL'                -- 优先级: LOW | NORMAL | HIGH
);

-- 1.3 删除 Catalog 级别缓存策略(使用默认策略)
ALTER CATALOG catalog_name UNSET CACHE POLICY;

-- 1.4 查看 Catalog 缓存统计信息
SHOW CACHE STATS FROM CATALOG catalog_name;


-- ----------------------------------------------------------------------------
-- 2. TABLE 级别缓存任务管理
-- ----------------------------------------------------------------------------

-- 2.1 查看表级别缓存任务配置
SHOW CACHE TASK FROM TABLE catalog_name.db_name.table_name;

-- 查看指定 Catalog 下所有表的缓存任务
SHOW CACHE TASKS FROM CATALOG catalog_name;

-- 查看指定数据库下所有表的缓存任务
SHOW CACHE TASKS FROM DATABASE catalog_name.db_name;

-- 查看所有缓存任务(带过滤条件)
SHOW CACHE TASKS 
[WHERE status IN ('RUNNING', 'PAUSED', 'FAILED')]
[ORDER BY last_refresh_time DESC]
[LIMIT 100];

-- 2.2 创建表级别缓存任务
CREATE CACHE TASK [IF NOT EXISTS] task_name
ON TABLE catalog_name.db_name.table_name
WITH (
    refresh_interval = '10m',          -- 刷新间隔
    refresh_mode = 'SCHEDULED',        -- 刷新模式
    cache_columns = 'col1,col2,col3',  -- 指定缓存列(可选,默认全部)
    cache_partitions = 'dt>=20240101', -- 指定缓存分区范围(可选)
    priority = 'HIGH',                 -- 任务优先级
    enable_incremental = true,         -- 是否启用增量刷新
    incremental_key = 'id',            -- 增量主键字段(UPSERT模式必需)
    incremental_column = 'update_time',-- 增量判断字段(时间戳字段)
    incremental_mode = 'UPSERT',       -- 增量模式: APPEND(仅追加) | UPSERT(更新插入) | CUSTOM(自定义)
    max_retry = 3,                     -- 失败重试次数
    timeout = '30m'                    -- 刷新超时时间
);

-- 示例1: 多字段联合主键的增量配置
CREATE CACHE TASK task_orders_multi_key
ON TABLE catalog_name.db_name.orders
WITH (
    enable_incremental = true,
    incremental_key = 'user_id,order_id',        -- 联合主键
    incremental_column = 'update_time',
    incremental_mode = 'UPSERT'
);

-- 示例2: 日志表追加模式(分区表)
CREATE CACHE TASK task_access_logs
ON TABLE catalog_name.db_name.access_logs
WITH (
    enable_incremental = true,
    incremental_mode = 'APPEND',                 -- 只追加,不更新
    incremental_column = 'log_time',             -- 基于时间戳增量
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

-- 示例3: 订单表更新插入模式(分区表,支持跨分区更新)
CREATE CACHE TASK task_orders_upsert
ON TABLE catalog_name.db_name.orders
WITH (
    enable_incremental = true,
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',          -- 关键: 捕获所有更新
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- 示例4: 使用自定义增量查询逻辑(过滤软删除记录)
CREATE CACHE TASK task_users_custom
ON TABLE catalog_name.db_name.users
WITH (
    enable_incremental = true,
    incremental_mode = 'CUSTOM',
    incremental_query = '''
        SELECT * FROM ${table}
        WHERE update_time > ${last_refresh_time}
        AND is_deleted = 0
    '''                                          -- 自定义增量查询SQL
);

-- 示例5: 自动检测增量字段(推荐,最简单)
CREATE CACHE TASK task_auto_detect
ON TABLE catalog_name.db_name.users
WITH (
    enable_incremental = true,
    auto_detect = true,                          -- 自动检测主键、时间戳、分区字段
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- 2.3 修改表级别缓存任务
ALTER CACHE TASK task_name
SET (
    refresh_interval = '15m',
    priority = 'NORMAL',
    enable_incremental = false
);

-- 修改任务状态
ALTER CACHE TASK task_name PAUSE;    -- 暂停任务
ALTER CACHE TASK task_name RESUME;   -- 恢复任务
ALTER CACHE TASK task_name ENABLE;   -- 启用任务
ALTER CACHE TASK task_name DISABLE;  -- 禁用任务

-- 2.4 删除表级别缓存任务
DROP CACHE TASK [IF EXISTS] task_name;

-- 批量删除缓存任务
DROP CACHE TASKS FROM TABLE catalog_name.db_name.table_name;
DROP CACHE TASKS FROM DATABASE catalog_name.db_name;
DROP CACHE TASKS FROM CATALOG catalog_name;

-- 2.5 查看表缓存详细信息
DESCRIBE CACHE TASK task_name;

-- 查看缓存任务执行历史
SHOW CACHE TASK HISTORY FROM task_name 
[LIMIT 50];


-- ----------------------------------------------------------------------------
-- 3. 手动触发缓存刷新
-- ----------------------------------------------------------------------------

-- 3.1 刷新指定表的缓存(默认增量模式)
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name;

-- 3.2 带选项的刷新
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name
WITH (
    mode = 'INCREMENTAL',              -- FULL(全量) | INCREMENTAL(增量,默认)
    partitions = 'dt=20240101',        -- 指定刷新的分区(可选)
    force = true,                      -- 强制刷新(忽略缓存有效期)
    async = false                      -- 是否异步执行
);

/*
【mode 参数详细说明】

=============================================================================
一、INCREMENTAL (增量刷新,默认)
=============================================================================

原理: 只加载自上次刷新以来发生变化的数据

适用场景:
  ✓ 日常定时刷新
  ✓ 数据量大,全量加载耗时长
  ✓ 大部分数据不变,只有少量新增/更新

执行逻辑:
  1. 获取 last_refresh_time (上次刷新时间)
  2. 基于 incremental_column 过滤数据
  3. 根据 incremental_mode 合并到缓存

-- 非分区表增量刷新
示例: 用户表
*/
CREATE CACHE TASK users_cache
ON TABLE catalog.db.users
WITH (
    incremental_mode = 'UPSERT',
    incremental_key = 'user_id',
    incremental_column = 'update_time'
);

REFRESH CACHE FROM TABLE catalog.db.users;
-- 执行SQL: 
--   SELECT * FROM users WHERE update_time > '2024-01-24 10:00:00'
-- 合并逻辑:
--   MERGE INTO cache_users ON user_id
--   WHEN MATCHED THEN UPDATE
--   WHEN NOT MATCHED THEN INSERT

/*
-- 分区表增量刷新(系统自动分区优化)
示例: 订单表(按日期分区)
*/
CREATE CACHE TASK orders_cache
ON TABLE catalog.db.orders  -- PARTITIONED BY (dt)
WITH (
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

REFRESH CACHE FROM TABLE catalog.db.orders;
-- 执行逻辑:
-- Step 1: 检测表元数据
--   - 是分区表: Yes (分区字段: dt)
--   - 缓存分区范围: 最近30天
--   - last_refresh_time: 2024-01-24 10:00:00
--
-- Step 2: 智能分区扫描(自动优化)
--   方式1: 基于分区修改时间
--     SELECT partition_name, last_modified_time
--     FROM system.partitions
--     WHERE table_name = 'orders'
--       AND last_modified_time > '2024-01-24 10:00:00'
--     → 检测到变更分区: [20240122, 20240123, 20240124, 20240125]
--
--   方式2: 基于数据分布(如果方式1不支持)
--     SELECT dt, MAX(update_time) as max_update_time
--     FROM orders
--     WHERE dt >= CURRENT_DATE - INTERVAL 30 DAY
--     GROUP BY dt
--     HAVING max_update_time > '2024-01-24 10:00:00'
--
-- Step 3: 生成优化的增量SQL
--   SELECT * FROM orders
--   WHERE update_time > '2024-01-24 10:00:00'
--     AND dt IN ('20240122', '20240123', '20240124', '20240125')
--   -- ↑ 自动添加分区过滤,只扫描4个变更分区,而不是全部30个分区
--
-- Step 4: MERGE 到缓存(支持跨分区更新)
--   MERGE INTO cache_orders ON order_id ...
--   -- 订单可能在 dt=20240101 创建,在 dt=20240125 更新状态

/*
-- 日志表增量刷新(APPEND模式 + 分区优化)
示例: 访问日志表
*/
CREATE CACHE TASK logs_cache
ON TABLE catalog.db.access_logs  -- PARTITIONED BY (dt)
WITH (
    incremental_mode = 'APPEND',
    incremental_column = 'log_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

REFRESH CACHE FROM TABLE catalog.db.access_logs;
-- 执行逻辑:
-- Step 1: 检测日志表特征
--   - 是分区表: Yes
--   - 增量模式: APPEND (只追加)
--   - last_refresh_time: 2024-01-24 10:00:00
--
-- Step 2: 智能优化(日志表特殊处理)
--   日志表特点: 通常只往最新1-2个分区写入数据
--   系统策略: 只扫描最新分区 + 可能的延迟分区
--   scan_partitions = ['20240124', '20240125']  -- 只扫描最近2天
--
-- Step 3: 生成增量SQL
--   SELECT * FROM access_logs
--   WHERE log_time > '2024-01-24 10:00:00'
--     AND dt IN ('20240124', '20240125')
--
-- Step 4: INSERT 到缓存(APPEND模式)
--   INSERT INTO cache_logs SELECT * FROM incremental_data
--
-- Step 5: 自动清理过期分区
--   DELETE FROM cache_logs WHERE dt < CURRENT_DATE - INTERVAL 7 DAY

/*
=============================================================================
二、FULL (全量刷新)
=============================================================================

原理: 完全重新加载数据,删除旧缓存,全量插入新数据

适用场景:
  ✓ 首次加载缓存
  ✓ 数据修复或缓存损坏
  ✓ 源表发生大规模变更(如字段类型调整)
  ✓ 增量刷新累积错误需要重置
  ✗ 不适合大表的日常刷新(性能差)

执行逻辑:
  1. 清空缓存表(TRUNCATE 或 DELETE)
  2. 从源表全量加载数据
  3. 更新元数据(last_refresh_time, row_count等)

-- 非分区表全量刷新
示例: 用户表
*/
REFRESH CACHE FROM TABLE catalog.db.users WITH (mode = 'FULL');
-- 执行SQL:
-- Step 1: 清空缓存
--   TRUNCATE TABLE cache_users;
--
-- Step 2: 全量加载
--   INSERT INTO cache_users
--   SELECT * FROM users;
--
-- Step 3: 更新元数据
--   UPDATE cache_metadata SET
--     last_refresh_time = CURRENT_TIMESTAMP,
--     last_refresh_mode = 'FULL',
--     row_count = (SELECT COUNT(*) FROM cache_users);

/*
-- 分区表全量刷新(按分区重建)
示例: 订单表(按日期分区)
*/
REFRESH CACHE FROM TABLE catalog.db.orders WITH (mode = 'FULL');
-- 执行逻辑:
-- Step 1: 确定缓存分区范围
--   cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
--   target_partitions = ['20240101', '20240102', ..., '20240130']
--
-- Step 2: 清空所有缓存分区
--   DELETE FROM cache_orders
--   WHERE dt >= CURRENT_DATE - INTERVAL 30 DAY;
--   -- 或逐个分区 TRUNCATE:
--   -- TRUNCATE TABLE cache_orders PARTITION (dt='20240101');
--   -- TRUNCATE TABLE cache_orders PARTITION (dt='20240102');
--   -- ...
--
-- Step 3: 全量加载所有分区
--   INSERT INTO cache_orders
--   SELECT * FROM orders
--   WHERE dt >= CURRENT_DATE - INTERVAL 30 DAY;
--
-- Step 4: 优化(可选): 按分区并行加载
--   并行执行:
--   INSERT INTO cache_orders SELECT * FROM orders WHERE dt='20240101';
--   INSERT INTO cache_orders SELECT * FROM orders WHERE dt='20240102';
--   ...
--   (30个任务并行执行,提升性能)

/*
-- 分区表全量刷新(指定部分分区)
示例: 只重建最近3天的缓存
*/
REFRESH CACHE FROM TABLE catalog.db.orders 
WITH (
    mode = 'FULL',
    partitions = 'dt>=20240122,dt<=20240124'  -- 指定分区范围
);
-- 执行逻辑:
-- Step 1: 解析分区范围
--   target_partitions = ['20240122', '20240123', '20240124']
--
-- Step 2: 清空指定分区
--   DELETE FROM cache_orders
--   WHERE dt IN ('20240122', '20240123', '20240124');
--
-- Step 3: 重新加载指定分区
--   INSERT INTO cache_orders
--   SELECT * FROM orders
--   WHERE dt IN ('20240122', '20240123', '20240124');

/*
-- 分区表全量刷新(单个分区)
示例: 重建昨天的分区(数据修复场景)
*/
REFRESH CACHE FROM TABLE catalog.db.orders 
WITH (
    mode = 'FULL',
    partitions = 'dt=20240124'
);
-- 执行逻辑:
-- Step 1: 清空单个分区
--   TRUNCATE TABLE cache_orders PARTITION (dt='20240124');
--   -- 或: DELETE FROM cache_orders WHERE dt='20240124';
--
-- Step 2: 重新加载单个分区
--   INSERT INTO cache_orders
--   SELECT * FROM orders WHERE dt='20240124';

/*
=============================================================================
三、FULL vs INCREMENTAL 对比总结
=============================================================================

维度              | INCREMENTAL                  | FULL
------------------|------------------------------|---------------------------
执行速度          | 快(只处理增量数据)           | 慢(处理全部数据)
资源消耗          | 低                           | 高
适用频率          | 高频(分钟/小时级)            | 低频(天/周级)
数据一致性        | 累积误差风险                 | 完全一致
依赖条件          | 需要时间戳字段               | 无特殊要求
首次加载          | 不适用                       | 必须使用
数据修复          | 不适用                       | 推荐使用
跨分区更新        | 支持(UPSERT模式)             | 完全支持

-- 非分区表对比
场景: 100万用户表,每天新增/更新1000条

INCREMENTAL:
  - 扫描: 1000条
  - 耗时: ~1秒
  - 适合日常刷新

FULL:
  - 扫描: 100万条
  - 耗时: ~30秒
  - 适合首次加载或修复

-- 分区表对比
场景: 订单表,30个分区(每个100万行),每天2个分区有变化

INCREMENTAL:
  - 扫描分区: 2个(20240124, 20240125)
  - 扫描行数: ~2万条(变更数据)
  - 耗时: ~3秒
  - 适合日常刷新

FULL:
  - 扫描分区: 30个(全部)
  - 扫描行数: 3000万条
  - 耗时: ~5分钟
  - 适合首次加载或大规模变更后

=============================================================================
四、最佳实践建议
=============================================================================

1. 日常调度: 使用 INCREMENTAL
   - 定时任务配置为增量刷新
   - 性能好,资源消耗低

2. 首次启用缓存: 使用 FULL
   - 第一次刷新必须全量加载
   - 之后切换为增量刷新

3. 定期全量刷新(可选): 每周/每月执行一次 FULL
   - 修复可能的累积误差
   - 重建索引,优化性能
   - 建议在低峰期执行

4. 数据修复场景: 使用 FULL
   - 发现数据不一致时
   - 源表结构变更后
   - 缓存损坏需要重建

5. 分区表优化:
   - 日常: INCREMENTAL (自动只扫描变更分区)
   - 修复特定分区: FULL + partitions='指定分区'
   - 首次加载: FULL (全量加载所有分区)

示例: 完整的缓存任务生命周期
*/

-- 创建任务(配置增量策略)
CREATE CACHE TASK orders_cache
ON TABLE catalog.db.orders
WITH (
    refresh_interval = '10m',
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- 首次加载: 全量刷新
REFRESH CACHE FROM TABLE catalog.db.orders WITH (mode = 'FULL');

-- 日常刷新: 增量刷新(自动执行或手动触发)
REFRESH CACHE FROM TABLE catalog.db.orders;  -- 默认 INCREMENTAL

-- 修复昨天分区: 全量刷新单个分区
REFRESH CACHE FROM TABLE catalog.db.orders 
WITH (mode = 'FULL', partitions = 'dt=20240124');

-- 每周全量刷新: 重建整个缓存
REFRESH CACHE FROM TABLE catalog.db.orders WITH (mode = 'FULL');

/*
=============================================================================
*/

-- 3.3 刷新指定数据库的所有表缓存
REFRESH CACHE FROM DATABASE catalog_name.db_name
[WITH (parallel = 4)];                 -- 并行度

-- 3.4 刷新整个 Catalog 的缓存
REFRESH CACHE FROM CATALOG catalog_name
[WITH (parallel = 8, timeout = '1h')];

-- 3.5 按任务名刷新
REFRESH CACHE TASK task_name;

-- 3.6 异步刷新并返回任务ID
REFRESH CACHE FROM TABLE catalog_name.db_name.table_name ASYNC;
-- 返回结果示例: refresh_job_id: 'refresh_20240101_123456'

-- 查看刷新任务状态
SHOW REFRESH JOB refresh_job_id;

-- 取消刷新任务
CANCEL REFRESH JOB refresh_job_id;


-- ----------------------------------------------------------------------------
-- 4. 缓存数据查询和管理
-- ----------------------------------------------------------------------------

-- 4.1 清空指定表的缓存数据
CLEAR CACHE FROM TABLE catalog_name.db_name.table_name
[WHERE partition_spec];

-- 4.2 清空数据库级别缓存
CLEAR CACHE FROM DATABASE catalog_name.db_name;

-- 4.3 清空 Catalog 级别缓存
CLEAR CACHE FROM CATALOG catalog_name;

-- 4.4 查看缓存使用情况
SHOW CACHE USAGE 
[FROM CATALOG catalog_name]
[ORDER BY cache_size DESC];

-- 4.5 查看缓存命中率统计
SHOW CACHE HIT RATE 
FROM TABLE catalog_name.db_name.table_name
[INTERVAL '1d'];                       -- 统计时间区间


-- ----------------------------------------------------------------------------
-- 5. 批量操作和高级功能
-- ----------------------------------------------------------------------------

-- 5.1 批量创建缓存任务(基于模式匹配)
CREATE CACHE TASKS ON TABLES LIKE 'catalog_name.db_name.fact_%'
WITH (
    refresh_interval = '5m',
    refresh_mode = 'SCHEDULED'
);

-- 5.2 继承 Catalog 策略创建任务
CREATE CACHE TASK task_name
ON TABLE catalog_name.db_name.table_name
INHERIT FROM CATALOG POLICY;

-- 5.3 复制缓存任务配置
CREATE CACHE TASK new_task_name
ON TABLE catalog_name.db_name.new_table
LIKE CACHE TASK existing_task_name;

-- 5.4 导出缓存配置
EXPORT CACHE CONFIG 
FROM CATALOG catalog_name 
TO 'file:///path/to/config.json';

-- 5.5 导入缓存配置
IMPORT CACHE CONFIG 
FROM 'file:///path/to/config.json'
[OVERWRITE];


-- ----------------------------------------------------------------------------
-- 6. 监控和诊断
-- ----------------------------------------------------------------------------

-- 6.1 查看当前运行中的刷新任务
SHOW RUNNING REFRESH JOBS;

-- 6.2 查看失败的缓存任务
SHOW FAILED CACHE TASKS 
[WHERE failure_time > '2024-01-01']
[LIMIT 100];

-- 6.3 查看缓存任务依赖关系
SHOW CACHE TASK DEPENDENCIES FROM task_name;

-- 6.4 验证缓存数据一致性
VALIDATE CACHE FROM TABLE catalog_name.db_name.table_name;

-- 6.5 获取缓存性能建议
ANALYZE CACHE PERFORMANCE 
FROM CATALOG catalog_name
[SUGGEST OPTIMIZATION];


-- ============================================================================
-- 核心概念说明
-- ============================================================================

/*
=============================================================================
一、增量模式 (incremental_mode) - 只有3种
=============================================================================

这是 CREATE CACHE TASK 时配置的增量策略,决定如何合并增量数据到缓存。

1. APPEND - 仅追加模式
   ├─ 适用场景: 日志表、事件流、只INSERT的表
   ├─ 必需字段: incremental_column (时间戳)
   ├─ 可选字段: 无需主键
   ├─ 合并逻辑: 直接 INSERT,不检查重复
   └─ 性能: 最快

2. UPSERT - 更新插入模式 (默认推荐)
   ├─ 适用场景: 业务表、维度表、会UPDATE的表
   ├─ 必需字段: incremental_key (主键), incremental_column (时间戳)
   ├─ 合并逻辑: MERGE (存在则UPDATE,不存在则INSERT)
   ├─ 特性: 支持跨分区更新
   └─ 性能: 中等(需要MERGE)

3. CUSTOM - 自定义模式
   ├─ 适用场景: 复杂业务逻辑、特殊需求
   ├─ 必需字段: incremental_query (自定义SQL)
   ├─ 灵活性: 完全自定义增量逻辑
   └─ 复杂度: 最高

注意: 没有 PARTITION 模式!
  - 分区表的增量更新 = 普通表增量 + 系统自动分区优化
  - 系统自动检测分区并优化扫描策略
  - 用户无需区分分区表和非分区表,配置语法完全一致


=============================================================================
二、刷新模式 (REFRESH 命令的 mode 参数)
=============================================================================

这是手动或定时刷新时的执行模式,与 incremental_mode 是不同的概念。

1. INCREMENTAL - 增量刷新 (默认)
   ├─ 含义: 只加载变化的数据
   ├─ 依赖: 基于 last_refresh_time 和 incremental_column
   ├─ 性能: 快,资源消耗低
   └─ 适合: 日常定时刷新

2. FULL - 全量刷新
   ├─ 含义: 完全重新加载数据
   ├─ 逻辑: 清空缓存 + 全量INSERT
   ├─ 性能: 慢,资源消耗高
   └─ 适合: 首次加载、数据修复、定期重建

对比:
  incremental_mode (CREATE时配置): 定义"如何合并增量数据"
  REFRESH mode (执行时指定): 定义"加载全量还是增量"


=============================================================================
三、分区优化机制 (系统自动)
=============================================================================

系统会自动检测分区表并进行智能优化,用户无需特殊配置。

自动检测:
  1. 检测表是否分区: SHOW CREATE TABLE
  2. 识别分区字段: PARTITIONED BY (dt, region, ...)
  3. 分析分区统计信息: SHOW PARTITIONS

智能优化策略:

策略1: 基于分区修改时间(推荐,Hive/Iceberg支持)
  -- 查询分区元数据
  SELECT partition_name, last_modified_time
  FROM system.partitions
  WHERE table_name = 'orders'
    AND last_modified_time > ${last_refresh_time}
  
  -- 只刷新变更的分区
  affected_partitions = ['20240124', '20240125']

策略2: 基于数据分布(兜底方案)
  -- 快速扫描每个分区的时间戳范围
  SELECT dt, MAX(update_time) as max_time
  FROM orders
  WHERE dt >= CURRENT_DATE - 30
  GROUP BY dt
  HAVING max_time > ${last_refresh_time}

策略3: 日
*/
