use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use kokedb_common::env::get_env_as;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use crate::{
    error::TaskError,
    runner::{DataSyncExecutor, TaskExecutor},
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
}

#[derive(Clone)]
pub struct TaskManager {
    config: TaskManagerConfig,
    tasks: Arc<DashMap<String, TaskMetadata>>,
    task_handles: Arc<DashMap<String, JoinHandle<()>>>,
    executor: Arc<dyn TaskExecutor>,
    active_tasks: Arc<AtomicUsize>,
    task_queue_tx: mpsc::UnboundedSender<TaskWrapper>,
    task_queue_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<TaskWrapper>>>,
    shutdown_tx: Option<mpsc::UnboundedSender<()>>,
    is_shutting_down: Arc<std::sync::atomic::AtomicBool>,
}

impl TaskManager {
    pub async fn new() -> Result<Self, TaskError> {
        let config: TaskManagerConfig = TaskManagerConfig::default();
        Self::new_with(config).await
    }

    pub async fn new_with(config: TaskManagerConfig) -> Result<Self, TaskError> {
        let (task_queue_tx, task_queue_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel();

        let task_manager = Self {
            config: config.clone(),
            tasks: Arc::new(DashMap::new()),
            task_handles: Arc::new(DashMap::new()),
            executor: Arc::new(DataSyncExecutor),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            task_queue_tx,
            task_queue_rx: Arc::new(tokio::sync::Mutex::new(task_queue_rx)),
            shutdown_tx: Some(shutdown_tx),
            is_shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        task_manager.start_scheduler(shutdown_rx).await;

        Ok(task_manager)
    }

    async fn start_scheduler(&self, mut shutdown_rx: mpsc::UnboundedReceiver<()>) {
        let tasks = self.tasks.clone();
        let task_handles = self.task_handles.clone();
        let executor = self.executor.clone();
        let active_tasks = self.active_tasks.clone();
        let max_concurrent = self.config.max_concurrent_tasks;
        let max_retries = self.config.max_retries;
        let task_queue_rx = self.task_queue_rx.clone();
        let is_shutting_down = self.is_shutting_down.clone();

        tokio::spawn(async move {
            let mut queue_rx = task_queue_rx.lock().await;

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        is_shutting_down.store(true, Ordering::Relaxed);
                        info!("Task scheduler received shutdown signal");
                        break;
                    }

                    task_wrapper = queue_rx.recv() => {
                        if let Some(task_wrapper) = task_wrapper {
                            if active_tasks.load(Ordering::Relaxed) >= max_concurrent {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                continue;
                            }

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

                            let handle = tokio::spawn(async move {
                                let task_id_for_callback = task_id.clone();
                                let tasks_for_callback = tasks_clone.clone();
                                let progress_callback = Box::new(move |progress: f32| {
                                    if let Some(mut task) = tasks_for_callback.get_mut(&task_id_for_callback) {
                                        task.progress = progress;
                                    }
                                });

                                let result = executor_clone.execute(task_config.clone(), Some(progress_callback)).await;

                                match result {
                                    Ok(_) => {
                                        if let Some(mut task) = tasks_clone.get_mut(&task_id) {
                                            task.status = TaskStatus::Completed;
                                            task.completed_at = Some(Utc::now());
                                            task.progress = 1.0;
                                        }
                                    }
                                    Err(e) => {
                                        if let Some(mut task) = tasks_clone.get_mut(&task_id) {
                                            task.status = TaskStatus::Failed;
                                            task.error_message = Some(e.to_string());

                                            if task.retry_count < max_retries {
                                                task.retry_count += 1;
                                                task.status = TaskStatus::Pending;
                                                info!("Task {} failed, retrying ({}/{})", task_id, task.retry_count, max_retries);
                                            }
                                        }
                                    }
                                }

                                task_handles_clone.remove(&task_id);
                                active_tasks_clone.fetch_sub(1, Ordering::Relaxed);
                            });

                            task_handles.insert(task_wrapper.id, handle);
                        }
                    }
                }
            }
        });
    }

    pub fn with_executor(mut self, executor: Arc<dyn TaskExecutor>) -> Self {
        self.executor = executor;
        self
    }

    pub async fn add_task(&self, config: CacheTableTaskConfig) -> Result<String, TaskError> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            return Err(TaskError::ExecutionFailed(
                "Task manager is shutting down".to_string(),
            ));
        }

        let task_id = Uuid::new_v4().to_string();

        if matches!(config.priority, TaskPriority::Normal) && config.additional_params.is_empty() {}

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

        let task_wrapper = TaskWrapper {
            id: task_id.clone(),
            config,
        };

        if let Err(_) = self.task_queue_tx.send(task_wrapper) {
            return Err(TaskError::QueueFull);
        }

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

    use log::info;

    use crate::task_manager::{CacheTableTaskConfig, TaskManager, TaskManagerConfig, TaskPriority};

    #[tokio::test]
    async fn test_task_manager_run_task() {
        let config = TaskManagerConfig::default();
        let task_manager = TaskManager::new_with(config).await.unwrap();
        let runtime_info = task_manager.get_runtime_info();
        info!("Runtime Info: {:?}", runtime_info);

        let task_config = CacheTableTaskConfig {
            dsn: "postgresql://root:12345@192.168.0.227:25432/postgres".to_string(),
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
            .wait_for_all_tasks(Some(tokio::time::Duration::from_secs(3)))
            .await
            .unwrap();

        let runtime_info = task_manager.get_runtime_info();
        info!("--->>Runtime Info: {:?}", runtime_info);

        let ret = task_manager.list_tasks().await;
        info!("=====ret===>>{:?}", ret);
    }
}
