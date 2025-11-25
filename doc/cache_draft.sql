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

策略3: 日志表优化(APPEND模式专用)
  -- 日志表特点: 通常只往最新1-2个分区写入
  -- 系统策略: 只扫描最新分区 + 可能的延迟分区
  scan_partitions = [today, yesterday]
  
  -- 自动清理过期分区
  DELETE FROM cache WHERE dt < CURRENT_DATE - ${cache_days}

用户配置示例(完全一致):

-- 非分区表
CREATE CACHE TASK users_cache
ON TABLE catalog.db.users
WITH (
    incremental_mode = 'UPSERT',
    incremental_key = 'user_id',
    incremental_column = 'update_time'
);

-- 分区表(配置完全相同!)
CREATE CACHE TASK orders_cache
ON TABLE catalog.db.orders  -- PARTITIONED BY (dt)
WITH (
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

系统自动处理分区优化:
  - 自动检测 dt 是分区字段
  - 自动添加分区过滤: AND dt IN (changed_partitions)
  - 自动清理过期分区: DELETE WHERE dt < ...
  - 用户无需关心这些细节!


=============================================================================
四、自动检测机制 (推荐使用)
=============================================================================

系统可以自动检测表的元数据,简化配置。

自动检测内容:
  1. 分区信息: 是否分区表、分区字段
  2. 主键: PRIMARY KEY 或 UNIQUE KEY
  3. 时间戳字段: update_time, updated_at, modify_time 等
  4. 推荐增量模式: 基于表特征自动推荐

启用自动检测:
*/

CREATE CACHE TASK auto_task
ON TABLE catalog.db.orders
WITH (
    enable_incremental = true,
    auto_detect = true,                 -- 启用自动检测
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

/*
系统自动配置为:
  incremental_key = 'order_id'         (自动检测到主键)
  incremental_column = 'update_time'   (自动检测到时间戳)
  incremental_mode = 'UPSERT'          (自动推荐)
  partition_columns = ['dt']           (自动检测到分区)

查看检测结果:
*/

SHOW TABLE METADATA FROM catalog.db.orders;
/*
输出示例:
+------------------+----------------------------------+
| Property         | Value                            |
+------------------+----------------------------------+
| is_partitioned   | true                             |
| partition_columns| [dt]                             |
| primary_key      | [order_id]                       |
| detected_timestamp_columns | [update_time, create_time] |
| recommended_incremental_mode | UPSERT                |
+------------------+----------------------------------+
*/


-- ============================================================================
-- 实战场景示例
-- ============================================================================

/*
=============================================================================
场景1: 订单表 - 分区表 + UPSERT模式 + 跨分区更新
=============================================================================

业务特点:
  - 按下单日期分区 (dt)
  - 订单状态会更新 (pending → paid → shipped → completed)
  - 订单可能在创建后几天才更新状态
  
示例数据:
  order_id | user_id | status    | create_time         | update_time         | dt
  ---------|---------|-----------|---------------------|---------------------|----------
  1001     | 5001    | completed | 2024-01-01 10:00:00 | 2024-01-05 15:30:00 | 20240101
  1002     | 5002    | shipped   | 2024-01-02 11:00:00 | 2024-01-04 09:20:00 | 20240102
  1003     | 5003    | pending   | 2024-01-05 14:00:00 | 2024-01-05 14:00:00 | 20240105
  
问题: 订单1001在 dt=20240101 分区创建,但在 dt=20240105 更新状态
*/

-- 配置
CREATE CACHE TASK orders_cache
ON TABLE hive_catalog.dw.orders
WITH (
    enable_incremental = true,
    incremental_mode = 'UPSERT',              -- 支持跨分区更新
    incremental_key = 'order_id',
    incremental_column = 'update_time',       -- 关键: 捕获所有更新
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
);

-- 首次全量加载
REFRESH CACHE FROM TABLE hive_catalog.dw.orders WITH (mode = 'FULL');
-- 加载最近30天所有订单

-- 增量刷新 (假设当前时间: 2024-01-05 16:00:00, last_refresh_time: 2024-01-05 08:00:00)
REFRESH CACHE FROM TABLE hive_catalog.dw.orders;

/*
执行逻辑:
  Step 1: 检测变更分区
    changed_partitions = ['20240101', '20240102', '20240104', '20240105']
    (系统检测这些分区在 08:00-16:00 间有数据变化)
  
  Step 2: 生成增量SQL
    SELECT * FROM orders
    WHERE update_time > '2024-01-05 08:00:00'
      AND dt IN ('20240101', '20240102', '20240104', '20240105')
    
    返回:
    order_id=1001, status=completed (dt=20240101,但刚更新)
    order_id=1002, status=shipped   (dt=20240102,状态变化)
    order_id=1003, status=pending   (dt=20240105,新订单)
  
  Step 3: MERGE到缓存 (支持跨分区更新)
    MERGE INTO cache_orders USING incremental_data
    ON cache_orders.order_id = incremental_data.order_id
    WHEN MATCHED THEN UPDATE SET status = incremental_data.status, ...
    WHEN NOT MATCHED THEN INSERT VALUES (...)
    
    结果:
    - order_id=1001 的状态被更新为 completed
    - order_id=1002 的状态被更新为 shipped
    - order_id=1003 被插入为新订单

关键点:
  ✓ update_time 捕获了所有变更(包括跨分区的)
  ✓ 分区过滤优化了扫描性能(只扫描4个分区而非30个)
  ✓ MERGE逻辑确保数据一致性
*/


/*
=============================================================================
场景2: 访问日志表 - 分区表 + APPEND模式 + 只追加
=============================================================================

业务特点:
  - 按日期分区 (dt)
  - 只追加新日志,不更新历史数据
  - 可能有1-2天的数据延迟
  
示例数据:
  log_id | user_id | action | log_time            | dt
  -------|---------|--------|---------------------|----------
  10001  | 5001    | click  | 2024-01-04 23:55:00 | 20240104
  10002  | 5002    | view   | 2024-01-05 00:05:00 | 20240105
  10003  | 5003    | click  | 2024-01-05 10:30:00 | 20240105
*/

-- 配置
CREATE CACHE TASK logs_cache
ON TABLE hive_catalog.logs.access_logs
WITH (
    enable_incremental = true,
    incremental_mode = 'APPEND',              -- 只追加
    incremental_column = 'log_time',          -- 无需主键
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY'
);

-- 首次全量加载
REFRESH CACHE FROM TABLE hive_catalog.logs.access_logs WITH (mode = 'FULL');

-- 增量刷新 (假设当前: 2024-01-05 12:00:00, last_refresh_time: 2024-01-05 08:00:00)
REFRESH CACHE FROM TABLE hive_catalog.logs.access_logs;

/*
执行逻辑:
  Step 1: 日志表优化(只扫描最新分区)
    系统识别: 这是日志表,通常只有最新1-2个分区有新数据
    scan_partitions = ['20240104', '20240105']  -- 只扫描最近2天
    
  Step 2: 生成增量SQL
    SELECT * FROM access_logs
    WHERE log_time > '2024-01-05 08:00:00'
      AND dt IN ('20240104', '20240105')
    
    返回:
    log_id=10003 (08:00之后的新日志)
  
  Step 3: INSERT到缓存 (APPEND模式)
    INSERT INTO cache_logs
    SELECT * FROM incremental_data;
    -- 直接插入,无需MERGE
  
  Step 4: 清理过期分区
    DELETE FROM cache_logs
    WHERE dt < CURRENT_DATE - INTERVAL 7 DAY;
    -- 自动清理8天前的数据

关键点:
  ✓ APPEND模式最简单,性能最好
  ✓ 系统自动只扫描最新分区
  ✓ 自动清理过期数据
*/


/*
=============================================================================
场景3: 用户表 - 非分区表 + UPSERT模式
=============================================================================

业务特点:
  - 非分区表
  - 用户信息会更新(昵称、头像、状态等)
  - 有新用户注册
*/

-- 配置
CREATE CACHE TASK users_cache
ON TABLE mysql_catalog.app.users
WITH (
    enable_incremental = true,
    incremental_mode = 'UPSERT',
    incremental_key = 'user_id',
    incremental_column = 'update_time'
);

-- 首次全量加载
REFRESH CACHE FROM TABLE mysql_catalog.app.users WITH (mode = 'FULL');

-- 增量刷新
REFRESH CACHE FROM TABLE mysql_catalog.app.users;

/*
执行逻辑:
  Step 1: 生成增量SQL (无分区,逻辑简单)
    SELECT * FROM users
    WHERE update_time > '2024-01-05 08:00:00'
  
  Step 2: MERGE到缓存
    MERGE INTO cache_users USING incremental_data
    ON cache_users.user_id = incremental_data.user_id
    WHEN MATCHED THEN UPDATE
    WHEN NOT MATCHED THEN INSERT

关键点:
  ✓ 非分区表配置最简单
  ✓ 增量逻辑与分区表完全一致
*/


/*
=============================================================================
场景4: 用户画像表 - 分区表 + 自定义模式 + 只缓存有效数据
=============================================================================

业务特点:
  - 按日期分区,每天全量更新
  - 有软删除标记 (is_deleted)
  - 只想缓存有效用户,节省空间
*/

-- 配置
CREATE CACHE TASK user_profile_cache
ON TABLE hive_catalog.dw.user_profile_daily
WITH (
    enable_incremental = true,
    incremental_mode = 'CUSTOM',
    incremental_query = '''
        SELECT * FROM ${table}
        WHERE update_time > ${last_refresh_time}
          AND is_deleted = 0
          AND dt >= CURRENT_DATE - INTERVAL 30 DAY
    '''
);

/*
说明:
  - CUSTOM模式允许完全自定义增量逻辑
  - 过滤 is_deleted = 0,只缓存有效数据
  - 自定义分区过滤逻辑
*/


/*
=============================================================================
场景5: 事实表 - 分区表 + 混合刷新策略
=============================================================================

业务需求:
  - 按月分区
  - 当月分区会更新,历史月份不变
  - 需要缓存最近3个月数据
  
策略: 当月增量 + 历史全量
*/

-- 配置
CREATE CACHE TASK fact_sales_cache
ON TABLE hive_catalog.dw.fact_sales
WITH (
    enable_incremental = true,
    incremental_mode = 'UPSERT',
    incremental_key = 'sale_id',
    incremental_column = 'update_time',
    cache_partitions = 'month >= DATE_FORMAT(DATE_SUB(CURRENT_DATE, 90), "yyyyMM")'
);

-- 日常增量刷新(只更新当月分区)
REFRESH CACHE FROM TABLE hive_catalog.dw.fact_sales;

-- 月初: 全量刷新上月分区(确保历史数据准确)
REFRESH CACHE FROM TABLE hive_catalog.dw.fact_sales
WITH (
    mode = 'FULL',
    partitions = 'month=202401'  -- 重建上月分区
);


-- ============================================================================
-- 最佳实践决策树
-- ============================================================================

/*
步骤1: 确定增量模式 (incremental_mode)

  表数据是否会UPDATE?
  ├─ 否 (只INSERT)
  │  └─ 使用 APPEND 模式
  │     - 日志表、事件流
  │     - 只需 incremental_column
  │     - 性能最好
  │
  └─ 是 (有UPDATE)
     ├─ 标准业务逻辑
     │  └─ 使用 UPSERT 模式 (推荐)
     │     - 订单表、用户表
     │     - 需要 incremental_key + incremental_column
     │     - 支持跨分区更新
     │
     └─ 复杂业务逻辑(软删除、多条件过滤等)
        └─ 使用 CUSTOM 模式
           - 完全自定义增量SQL
           - 灵活但复杂


步骤2: 配置分区范围 (cache_partitions)

  是否为分区表?
  ├─ 否 (非分区表)
  │  └─ 不需要配置 cache_partitions
  │
  └─ 是 (分区表)
     ├─ 缓存最近N天/月
     │  └─ cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY'
     │
     ├─ 缓存固定时间范围
     │  └─ cache_partitions = 'dt>=20240101,dt<=20240131'
     │
     └─ 缓存所有分区
        └─ 不配置 cache_partitions (默认全部)


步骤3: 选择刷新模式 (REFRESH mode)

  首次加载?
  ├─ 是
  │  └─ 使用 FULL 模式
  │     REFRESH CACHE ... WITH (mode = 'FULL')
  │
  └─ 否 (日常刷新)
     ├─ 正常情况
     │  └─ 使用 INCREMENTAL 模式 (默认)
     │     REFRESH CACHE ...
     │
     └─ 特殊情况(数据修复、缓存损坏)
        └─ 使用 FULL 模式
           REFRESH CACHE ... WITH (mode = 'FULL')


步骤4: 是否启用自动检测 (可选)

  表结构清晰(有主键、标准时间戳字段)?
  ├─ 是
  │  └─ 启用 auto_detect = true
  │     - 系统自动配置所有参数
  │     - 配置最简单
  │
  └─ 否 (特殊命名、复杂结构)
     └─ 手动指定所有参数
        - incremental_key
        - incremental_column
        - incremental_mode
*/


-- ============================================================================
-- 常见问题 FAQ
-- ============================================================================

/*
Q1: APPEND 和 UPSERT 如何选择?
A1: 
  - 数据只INSERT不UPDATE → APPEND
  - 数据会UPDATE → UPSERT
  - 不确定时建议用 UPSERT(更安全)

Q2: 分区表是否需要特殊配置?
A2: 
  - 不需要!配置与非分区表完全一致
  - 系统自动检测分区并优化性能
  - 只需指定 cache_partitions 限定缓存范围

Q3: 如何处理软删除(is_deleted)?
A3: 
  方案1 (推荐,简单): 使用 UPSERT,缓存所有数据
    - 查询时过滤: SELECT * FROM cache WHERE is_deleted = 0
  
  方案2 (节省空间): 使用 CUSTOM,只缓存有效数据
    - incremental_query 中过滤: WHERE is_deleted = 0
    - 缺点: 无法查询历史删除记录

Q4: 什么时候用 FULL 模式刷新?
A4:
  - 首次加载缓存 (必须)
  - 数据修复或一致性问题
  - 源表结构变更
  - 定期重建(建议每周/月,在低峰期)

Q5: 分区表增量刷新会扫描所有分区吗?
A5:
  - 不会!系统自动优化
  - 只扫描检测到有变化的分区
  - 比如: 30天的缓存范围,可能只扫描2-3个变更分区

Q6: 跨分区更新如何工作?
A6:
  - UPSERT 模式天然支持
  - 示例: 订单在 dt=20240101 创建,在 dt=20240105 更新状态
  - 增量SQL会扫描所有缓存范围内的分区,但只更新变化的记录

Q7: 如何验证缓存配置是否正确?
A7:
  -- 查看任务配置
  DESCRIBE CACHE TASK task_name;
  
  -- 查看自动检测结果
  SHOW TABLE METADATA FROM catalog.db.table;
  
  -- 查看执行计划(不实际执行)
  EXPLAIN REFRESH CACHE FROM TABLE catalog.db.table;

Q8: 增量刷新失败如何排查?
A8:
  -- 查看失败任务
  SHOW FAILED CACHE TASKS WHERE task_name = 'xxx';
  
  -- 查看执行历史
  SHOW CACHE TASK HISTORY FROM task_name LIMIT 50;
  
  -- 验证数据一致性
  VALIDATE CACHE FROM TABLE catalog.db.table;

Q9: 如何优化大表的首次加载?
A9:
  -- 方式1: 分批加载分区
  REFRESH CACHE FROM TABLE orders 
  WITH (mode = 'FULL', partitions = 'dt>=20240101,dt<=20240107');
  
  REFRESH CACHE FROM TABLE orders 
  WITH (mode = 'FULL', partitions = 'dt>=20240108,dt<=20240114');
  
  -- 方式2: 并行加载
  REFRESH CACHE FROM TABLE orders 
  WITH (mode = 'FULL', parallel = 8);

Q10: 定时任务如何配置?
A10:
  -- 创建定时任务
  CREATE CACHE TASK orders_cache
  ON TABLE catalog.db.orders
  WITH (
      refresh_mode = 'SCHEDULED',     -- 定时模式
      refresh_interval = '10m',       -- 每10分钟刷新
      incremental_mode = 'UPSERT',
      ...
  );
  
  -- 系统会自动按计划增量刷新
  -- 无需手动触发 REFRESH CACHE
*/


-- ============================================================================
-- 完整配置示例总结
-- ============================================================================

-- 1. 日志表(分区表 + APPEND)
CREATE CACHE TASK logs_cache
ON TABLE catalog.logs.access_logs
WITH (
    enable_incremental = true,
    incremental_mode = 'APPEND',
    incremental_column = 'log_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 7 DAY',
    refresh_interval = '5m',
    refresh_mode = 'SCHEDULED'
);

-- 2. 订单表(分区表 + UPSERT)
CREATE CACHE TASK orders_cache
ON TABLE catalog.dw.orders
WITH (
    enable_incremental = true,
    incremental_mode = 'UPSERT',
    incremental_key = 'order_id',
    incremental_column = 'update_time',
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY',
    refresh_interval = '10m',
    refresh_mode = 'SCHEDULED'
);

-- 3. 用户表(非分区表 + UPSERT)
CREATE CACHE TASK users_cache
ON TABLE catalog.app.users
WITH (
    enable_incremental = true,
    incremental_mode = 'UPSERT',
    incremental_key = 'user_id',
    incremental_column = 'update_time',
    refresh_interval = '15m',
    refresh_mode = 'SCHEDULED'
);

-- 4. 自动检测配置(最简单)
CREATE CACHE TASK auto_cache
ON TABLE catalog.db.any_table
WITH (
    enable_incremental = true,
    auto_detect = true,
    cache_partitions = 'dt >= CURRENT_DATE - INTERVAL 30 DAY',
    refresh_interval = '10m',
    refresh_mode = 'SCHEDULED'
);

-- ============================================================================
-- 结束
-- ============================================================================
