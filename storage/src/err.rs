use std::{fmt::Display, io};
use thiserror::Error;

#[derive(Debug, Eq, PartialEq)]
pub struct IoError {
    kind: io::ErrorKind,
    str: String,
}

impl Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("kind {:?}, detail {}", self.kind, self.str))
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("key not exist")]
    KeyNotExist,
    #[error("unknown error")]
    Unknown,
    #[error("value too large")]
    ValueTooLarge,
    #[error("io fail {0}")]
    Io(#[from] io::Error),
}

impl StorageError {
    pub fn is_io_not_found(&self) -> bool {
        if let Self::Io(i) = self {
            if io::ErrorKind::NotFound == i.kind() {
                return true;
            }
        }
        return false;
    }
}

impl PartialEq for StorageError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(l0), Self::Io(r0)) => return l0.kind() == r0.kind(),
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
