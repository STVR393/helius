use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    AlreadyExists(String),
    #[error("{0}")]
    Config(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    DateParse(#[from] chrono::ParseError),
    #[error("{0}")]
    Http(String),
    #[error(transparent)]
    Clap(#[from] clap::Error),
}

impl AppError {
    pub fn missing_db(path: &Path) -> Self {
        Self::Config(format!(
            "database not initialized at {}; run `helius init` first",
            path.display()
        ))
    }

    pub fn invalid_ref(entity: &str, value: &str) -> Self {
        Self::NotFound(format!("{entity} `{value}` was not found"))
    }

    pub fn duplicate(entity: &str, value: &str) -> Self {
        Self::AlreadyExists(format!("{entity} `{value}` already exists"))
    }

    pub fn path_message(prefix: &str, path: PathBuf) -> Self {
        Self::Config(format!("{prefix}: {}", path.display()))
    }
}
