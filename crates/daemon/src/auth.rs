//! Daemon authentication: per-instance bearer token.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;

use rand::RngCore;

/// Generate a fresh 256-bit token as 64 hex characters.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Write the token file with restrictive permissions (best effort on Unix).
pub fn write_token(path: &Path, token: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(token.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Read the token file (trimmed).
pub fn read_token(path: &Path) -> std::io::Result<String> {
    Ok(fs::read_to_string(path)?.trim().to_string())
}
