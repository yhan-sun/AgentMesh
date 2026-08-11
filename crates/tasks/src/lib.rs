//! TaskManager: runs tasks through adapters while persisting state.

pub mod manager;

pub use manager::{ExecutionMetadata, ManagedTaskRun, TaskError, TaskManager};
