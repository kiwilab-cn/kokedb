use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_common::env::get_env_as;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, Mutex, Notify},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    error::TaskError,
    runner::{DataSyncExecutor, ResultRefresher, ResultRefresherSlot, TaskExecutor},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Debug, Clone)]
pub struct TaskManagerConfig {
    pub task_queue_size: usize,
    pub max_concurrent_tasks: usize,
    pub max_retries: usize,
    pub enable_metrics: bool,
}

impl Default for TaskManagerConfig {
    fn default() -> Self {
        Self {
            task_queue_size: 128,
            max_concurrent_tasks: 16,
            max_retries: 3,
            enable_metrics: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TaskStatus {
    Pending,
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    DataSync,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskConfig {
    DataSyncTaskConfig(CacheTableTaskConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheTableTaskConfig {
    pub dsn: String,
    pub source_table: String,
    pub local_table: String,
    pub catalog_name: String,
    pub batch_size: Option<usize>,
    pub timeout_seconds: Option<usize>,
    pub priority: TaskPriority,
    pub additional_params: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultRefreshTaskConfig {
    pub sql_id: u64,
    pub batch_size: Option<usize>,
    pub timeout_seconds: Option<usize>,
    pub priority: TaskPriority,
    pub additional_params: HashMap<String, String>,
}

impl CacheTableTaskConfig {
    //TODO: change to &str
    pub fn new(
        catalog_name: String,
        dsn: String,
        source_table: String,
        local_table: String,
    ) -> Self {
        Self {
            catalog_name,
            dsn,
            source_table,
            local_table,
            batch_size: Some(get_env_as("KOKEDB_READ_TABLE_BATCH_SIZE", 300000usize)),
            timeout_seconds: Some(get_env_as("KOKEDB_READ_TABLE_TIMEOUT", 3600usize)),
            priority: TaskPriority::Critical,
            additional_params: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub id: String,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub config: CacheTableTaskConfig,
    pub created_at: DateTime<Utc>,
    pub queued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub progress: f32, // 0.0 - 1.0
    pub retry_count: usize,
}

#[derive(Debug, Clone)]
pub struct TaskManagerStats {
    pub total_tasks: usize,
    pub status_counts: HashMap<TaskStatus, usize>,
    pub active_tasks: usize,
    pub queued_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub average_execution_time: Option<f64>,
    pub thread_pool_size: usize,
}

struct TaskWrapper {
    id: String,
    config: CacheTableTaskConfig,
    priority: TaskPriority,
    /// Monotonic enqueue sequence; breaks priority ties in FIFO order.
    seq: u64,
}

// Ordering for the priority queue (a max-heap): higher `priority` is popped
// first; among equal priorities the smaller `seq` (enqueued earlier) is popped
// first, giving stable FIFO behaviour within a priority level.
impl Ord for TaskWrapper {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for TaskWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for TaskWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for TaskWrapper {}

/// Shared priority queue of pending tasks plus a notifier the scheduler waits on.
#[derive(Clone)]
struct TaskQueue {
    heap: Arc<Mutex<BinaryHeap<TaskWrapper>>>,
    notify: Arc<Notify>,
    seq: Arc<AtomicU64>,
}

impl TaskQueue {
    fn new() -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            notify: Arc::new(Notify::new()),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Enqueue a task and wake the scheduler.
    async fn push(&self, id: String, config: CacheTableTaskConfig, priority: TaskPriority) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        self.heap.lock().await.push(TaskWrapper {
            id,
            config,
            priority,
            seq,
        });
        self.notify.notify_one();
    }

    async fn pop(&self) -> Option<TaskWrapper> {
        self.heap.lock().await.pop()
    }
}

#[derive(Clone)]
pub struct TaskManager {
    config: TaskManagerConfig,
    tasks: Arc<DashMap<String, TaskMetadata>>,
    task_handles: Arc<DashMap<String, JoinHandle<()>>>,
    executor: Arc<dyn TaskExecutor>,
    /// Settable slot for the result refresher, shared with the data-sync
    /// executor; filled by the query layer after construction.
    refresher_slot: ResultRefresherSlot,
    active_tasks: Arc<AtomicUsize>,
    queue: TaskQueue,
    shutdown_tx: Option<mpsc::UnboundedSender<()>>,
    is_shutting_down: Arc<std::sync::atomic::AtomicBool>,
}

impl TaskManager {
    pub async fn new(cache: LruResultCache) -> Result<Self, TaskError> {
        let config: TaskManagerConfig = TaskManagerConfig::default();
        Self::new_with(config, cache).await
    }

    pub async fn new_with(
        config: TaskManagerConfig,
        cache: LruResultCache,
    ) -> Result<Self, TaskError> {
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel();

        let refresher_slot: ResultRefresherSlot = Arc::new(std::sync::Mutex::new(None));

        let task_manager = Self {
            config: config.clone(),
            tasks: Arc::new(DashMap::new()),
            task_handles: Arc::new(DashMap::new()),
            executor: Arc::new(DataSyncExecutor::new(refresher_slot.clone())),
            refresher_slot,
            active_tasks: Arc::new(AtomicUsize::new(0)),
            queue: TaskQueue::new(),
            shutdown_tx: Some(shutdown_tx),
            is_shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        task_manager.start_scheduler(shutdown_rx, cache).await;

        Ok(task_manager)
    }

    async fn start_scheduler(
        &self,
        mut shutdown_rx: mpsc::UnboundedReceiver<()>,
        cache: LruResultCache,
    ) {
        let tasks = self.tasks.clone();
        let task_handles = self.task_handles.clone();
        let executor = self.executor.clone();
        let active_tasks = self.active_tasks.clone();
        let max_concurrent = self.config.max_concurrent_tasks;
        let max_retries = self.config.max_retries;
        let queue = self.queue.clone();
        let is_shutting_down = self.is_shutting_down.clone();

        tokio::spawn(async move {
            loop {
                if is_shutting_down.load(Ordering::Relaxed) {
                    break;
                }

                // Respect the concurrency cap before pulling more work; a freed
                // slot is picked up on the next iteration.
                if active_tasks.load(Ordering::Relaxed) >= max_concurrent {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }

                // Pop the highest-priority ready task, or wait until one arrives
                // (or shutdown is signalled).
                let task_wrapper = match queue.pop().await {
                    Some(tw) => tw,
                    None => {
                        tokio::select! {
                            _ = shutdown_rx.recv() => {
                                is_shutting_down.store(true, Ordering::Relaxed);
                                info!("Task scheduler received shutdown signal");
                                break;
                            }
                            _ = queue.notify.notified() => continue,
                        }
                    }
                };

                {
                            if let Some(mut task) = tasks.get_mut(&task_wrapper.id) {
                                task.status = TaskStatus::Running;
                                task.started_at = Some(Utc::now());
                            }

                            active_tasks.fetch_add(1, Ordering::Relaxed);

                            let task_id = task_wrapper.id.clone();
                            let task_config = task_wrapper.config.clone();
                            let tasks_clone = tasks.clone();
                            let task_handles_clone = task_handles.clone();
                            let executor_clone = executor.clone();
                            let active_tasks_clone = active_tasks.clone();
                            let retry_queue = queue.clone();
                            let cache = cache.clone();

                            let handle = tokio::spawn(async move {
                                let task_id_for_callback = task_id.clone();
                                let tasks_for_callback = tasks_clone.clone();
                                let progress_callback = Box::new(move |progress: f32| {
                                    if let Some(mut task) = tasks_for_callback.get_mut(&task_id_for_callback) {
                                        task.progress = progress;
                                    }
                                });

                                let result = executor_clone.execute(task_config.clone(), cache.clone(),  Some(progress_callback)).await;

                                match result {
                                    Ok(_) => {
                                        if let Some(mut task) = tasks_clone.get_mut(&task_id) {
                                            task.status = TaskStatus::Completed;
                                            task.completed_at = Some(Utc::now());
                                            task.progress = 1.0;
                                        }
                                    }
                                    Err(e) => {
                                        // Decide whether to retry while holding the entry briefly,
                                        // then release the lock before any await to avoid holding
                                        // a DashMap guard across suspension points.
                                        let retry = if let Some(mut task) = tasks_clone.get_mut(&task_id) {
                                            task.status = TaskStatus::Failed;
                                            task.error_message = Some(e.to_string());

                                            if task.retry_count < max_retries {
                                                task.retry_count += 1;
                                                task.status = TaskStatus::Pending;
                                                info!("Task {} failed with error: {}, retrying ({}/{})", task_id, &e, task.retry_count, max_retries);
                                                Some(task.retry_count)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        };

                                        // Re-enqueue with capped exponential backoff so a
                                        // persistently failing task does not hot-loop the queue.
                                        // The backoff runs in a detached task so this worker
                                        // frees its concurrency slot immediately instead of
                                        // sleeping while holding it.
                                        if let Some(attempt) = retry {
                                            let backoff_secs = (1u64 << attempt.min(6)).min(60);
                                            let retry_queue = retry_queue.clone();
                                            let tasks_retry = tasks_clone.clone();
                                            let retry_id = task_id.clone();
                                            let retry_config = task_config.clone();
                                            let retry_priority = retry_config.priority;
                                            tokio::spawn(async move {
                                                tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                                                retry_queue
                                                    .push(retry_id.clone(), retry_config, retry_priority)
                                                    .await;
                                                if let Some(mut task) = tasks_retry.get_mut(&retry_id) {
                                                    task.status = TaskStatus::Queued;
                                                    task.queued_at = Some(Utc::now());
                                                }
                                            });
                                        }
                                    }
                                }

                                task_handles_clone.remove(&task_id);
                                active_tasks_clone.fetch_sub(1, Ordering::Relaxed);
                            });

                            task_handles.insert(task_wrapper.id, handle);
                }
            }
        });
    }

    pub fn with_executor(mut self, executor: Arc<dyn TaskExecutor>) -> Self {
        self.executor = executor;
        self
    }

    /// Installs the result refresher used to proactively re-warm cached query
    /// results after a table sync. Wired by the query layer post-construction.
    pub fn set_result_refresher(&self, refresher: Arc<dyn ResultRefresher>) {
        if let Ok(mut slot) = self.refresher_slot.lock() {
            *slot = Some(refresher);
        }
    }

    /// Whether a sync task for `(catalog, source_table)` is currently pending,
    /// queued, or running. Used by the adaptive tick to avoid stacking duplicate
    /// refreshes when a sync runs longer than the tick interval.
    pub fn has_inflight_table_task(&self, catalog: &str, source_table: &str) -> bool {
        self.tasks.iter().any(|entry| {
            let t = entry.value();
            matches!(
                t.status,
                TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running
            ) && t.config.catalog_name == catalog
                && t.config.source_table == source_table
        })
    }

    pub async fn add_task(&self, config: CacheTableTaskConfig) -> Result<String, TaskError> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            return Err(TaskError::ExecutionFailed(
                "Task manager is shutting down".to_string(),
            ));
        }

        let task_id = Uuid::new_v4().to_string();

        let metadata = TaskMetadata {
            id: task_id.clone(),
            task_type: TaskType::DataSync,
            status: TaskStatus::Pending,
            config: config.clone(),
            created_at: Utc::now(),
            queued_at: None,
            started_at: None,
            completed_at: None,
            error_message: None,
            progress: 0.0,
            retry_count: 0,
        };

        self.tasks.insert(task_id.clone(), metadata);

        let priority = config.priority;
        self.queue.push(task_id.clone(), config, priority).await;

        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.status = TaskStatus::Queued;
            task.queued_at = Some(Utc::now());
        }

        Ok(task_id)
    }

    pub async fn add_tasks_batch(
        &self,
        configs: Vec<CacheTableTaskConfig>,
    ) -> Result<Vec<String>, TaskError> {
        let mut task_ids = Vec::with_capacity(configs.len());

        for config in configs {
            let task_id = self.add_task(config).await?;
            task_ids.push(task_id);
        }

        Ok(task_ids)
    }

    pub async fn cancel_task(&self, task_id: &str) -> Result<(), TaskError> {
        if let Some((_, handle)) = self.task_handles.remove(task_id) {
            handle.abort();
            self.active_tasks.fetch_sub(1, Ordering::Relaxed);
        }

        if let Some(mut task) = self.tasks.get_mut(task_id) {
            task.status = TaskStatus::Cancelled;
            Ok(())
        } else {
            Err(TaskError::TaskNotFound(task_id.to_string()))
        }
    }

    pub async fn cancel_tasks_batch(&self, task_ids: &[String]) -> Result<Vec<String>, TaskError> {
        let mut cancelled = Vec::new();

        for task_id in task_ids {
            if self.cancel_task(task_id).await.is_ok() {
                cancelled.push(task_id.clone());
            }
        }

        Ok(cancelled)
    }

    pub async fn get_task_status(&self, task_id: &str) -> Result<TaskMetadata, TaskError> {
        self.tasks
            .get(task_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| TaskError::TaskNotFound(task_id.to_string()))
    }

    pub async fn get_tasks_status(
        &self,
        task_ids: &[String],
    ) -> HashMap<String, Option<TaskMetadata>> {
        let mut results = HashMap::new();

        for task_id in task_ids {
            let status = self.tasks.get(task_id).map(|entry| entry.clone());
            results.insert(task_id.clone(), status);
        }

        results
    }

    pub async fn list_tasks_by_status(&self, status: TaskStatus) -> Vec<TaskMetadata> {
        self.tasks
            .iter()
            .filter(|entry| entry.value().status == status)
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub async fn list_tasks(&self) -> Vec<TaskMetadata> {
        self.tasks
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub async fn get_detailed_statistics(&self) -> TaskManagerStats {
        let mut status_counts = HashMap::new();
        let mut execution_times = Vec::new();

        for entry in self.tasks.iter() {
            let task = entry.value();
            *status_counts.entry(task.status.clone()).or_insert(0) += 1;

            if let (Some(started), Some(completed)) = (task.started_at, task.completed_at) {
                let duration = completed.signed_duration_since(started);
                execution_times.push(duration.num_milliseconds() as f64 / 1000.0);
            }
        }

        let average_execution_time = if execution_times.is_empty() {
            None
        } else {
            Some(execution_times.iter().sum::<f64>() / execution_times.len() as f64)
        };

        TaskManagerStats {
            total_tasks: self.tasks.len(),
            active_tasks: self.active_tasks.load(Ordering::Relaxed),
            queued_tasks: status_counts.get(&TaskStatus::Queued).copied().unwrap_or(0),
            completed_tasks: status_counts
                .get(&TaskStatus::Completed)
                .copied()
                .unwrap_or(0),
            failed_tasks: status_counts.get(&TaskStatus::Failed).copied().unwrap_or(0),
            status_counts,
            average_execution_time,
            thread_pool_size: self.config.max_concurrent_tasks,
        }
    }

    pub async fn get_statistics(&self) -> HashMap<TaskStatus, usize> {
        self.get_detailed_statistics().await.status_counts
    }

    pub async fn cleanup_completed_tasks(&self) -> usize {
        let mut cleaned = 0;

        let to_remove: Vec<String> = self
            .tasks
            .iter()
            .filter(|entry| {
                matches!(
                    entry.value().status,
                    TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
                )
            })
            .map(|entry| entry.key().clone())
            .collect();

        for task_id in to_remove {
            self.tasks.remove(&task_id);
            cleaned += 1;
        }

        cleaned
    }

    pub async fn wait_for_task(
        &self,
        task_id: &str,
        timeout: Option<tokio::time::Duration>,
    ) -> Result<TaskMetadata, TaskError> {
        let start_time = std::time::Instant::now();

        loop {
            if let Some(task) = self.tasks.get(task_id) {
                match task.status {
                    TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => {
                        return Ok(task.clone());
                    }
                    _ => {}
                }
            } else {
                return Err(TaskError::TaskNotFound(task_id.to_string()));
            }

            if let Some(timeout) = timeout {
                if start_time.elapsed() > timeout {
                    return Err(TaskError::ExecutionFailed("Task wait timeout".to_string()));
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_for_all_tasks(
        &self,
        timeout: Option<tokio::time::Duration>,
    ) -> Result<(), TaskError> {
        let start_time = std::time::Instant::now();

        loop {
            let has_running_tasks = self.tasks.iter().any(|entry| {
                matches!(
                    entry.value().status,
                    TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running
                )
            });

            if !has_running_tasks {
                break;
            }

            if let Some(timeout) = timeout {
                if start_time.elapsed() > timeout {
                    return Err(TaskError::ExecutionFailed(
                        "Wait for all tasks timeout".to_string(),
                    ));
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(())
    }

    pub fn get_runtime_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::new();
        info.insert("runtime_type".to_string(), "tokio".to_string());
        info.insert(
            "max_concurrent_tasks".to_string(),
            self.config.max_concurrent_tasks.to_string(),
        );
        info.insert(
            "active_tasks".to_string(),
            self.active_tasks.load(Ordering::Relaxed).to_string(),
        );
        info.insert("total_tasks".to_string(), self.tasks.len().to_string());
        info
    }

    pub async fn shutdown(&mut self) {
        self.is_shutting_down.store(true, Ordering::Relaxed);

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        let task_ids: Vec<String> = self
            .task_handles
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for task_id in task_ids {
            let _ = self.cancel_task(&task_id).await;
        }

        info!("Task manager shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use kokedb_cache::foyer_hybrid::LruResultCache;
    use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
    use log::info;

    use crate::task_manager::{
        CacheTableTaskConfig, TaskManager, TaskManagerConfig, TaskPriority, TaskQueue,
    };

    fn cfg(priority: TaskPriority) -> CacheTableTaskConfig {
        let mut c = CacheTableTaskConfig::new(
            "kokedb".to_string(),
            "dsn".to_string(),
            "public.t".to_string(),
            "public.t".to_string(),
        );
        c.priority = priority;
        c
    }

    #[tokio::test]
    async fn priority_queue_pops_highest_priority_then_fifo() {
        let q = TaskQueue::new();
        // Enqueue out of priority order; same priority must stay FIFO by seq.
        q.push("low".into(), cfg(TaskPriority::Low), TaskPriority::Low)
            .await;
        q.push("crit1".into(), cfg(TaskPriority::Critical), TaskPriority::Critical)
            .await;
        q.push("normal".into(), cfg(TaskPriority::Normal), TaskPriority::Normal)
            .await;
        q.push("crit2".into(), cfg(TaskPriority::Critical), TaskPriority::Critical)
            .await;

        let mut order = Vec::new();
        while let Some(tw) = q.pop().await {
            order.push(tw.id);
        }
        // Critical first (FIFO within: crit1 before crit2), then Normal, then Low.
        assert_eq!(order, vec!["crit1", "crit2", "normal", "low"]);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via `make integration-test`"]
    async fn test_task_manager_run_task() {
        // Ensure the meta store schema exists (the server does this on startup;
        // here we do it explicitly so the sync task's metadata writes succeed).
        PostgreSQLMetaCatalogProviderList::new()
            .await
            .unwrap()
            .init_db()
            .await
            .unwrap();

        let config = TaskManagerConfig::default();
        let cache = LruResultCache::new(100, 100).await.unwrap();
        let task_manager = TaskManager::new_with(config, cache).await.unwrap();
        let runtime_info = task_manager.get_runtime_info();
        info!("Runtime Info: {:?}", runtime_info);

        let dsn = std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".to_string());
        let task_config = CacheTableTaskConfig {
            dsn,
            source_table: "public.demo".to_string(),
            local_table: "public.demo".to_string(),
            batch_size: Some(1000),
            timeout_seconds: Some(100),
            priority: TaskPriority::Critical,
            additional_params: HashMap::new(),
            catalog_name: "kokedb".to_string(),
        };

        task_manager.add_task(task_config).await.unwrap();

        task_manager
            .wait_for_all_tasks(Some(tokio::time::Duration::from_secs(60)))
            .await
            .unwrap();

        let runtime_info = task_manager.get_runtime_info();
        info!("--->>Runtime Info: {:?}", runtime_info);

        let ret = task_manager.list_tasks().await;
        info!("=====ret===>>{:?}", ret);
    }
}
