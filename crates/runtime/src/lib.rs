//! Managed child-process lifecycle for coding agent CLIs.
//!
//! Adapters must not spawn processes directly; all process handling goes
//! through this crate.

pub mod error;
pub mod process;

pub use error::RuntimeError;
pub use process::{Process, ProcessCancelHandle, ProcessEvent, ProcessSpec};
