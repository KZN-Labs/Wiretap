use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),

    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("decode: {0}")]
    Decode(String),

    #[error("source closed before reaching tip")]
    SourceClosed,

    #[error("gap backfill failed at checkpoint {checkpoint}: {source}")]
    Backfill {
        checkpoint: u64,
        #[source]
        source: Box<Error>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
