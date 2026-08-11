//! TaskManager: runs tasks through adapters while persisting state.

pub mod manager;

pub use manager::{ManagedTaskRun, TaskError, TaskManager};
