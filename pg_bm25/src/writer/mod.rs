mod client;
mod directory;
mod index;
mod server;
mod transfer;

use crate::schema::SearchDocument;
pub use client::{Client, ClientError};
pub use directory::*;
pub use index::Writer;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub use server::{Server, ServerError};
use std::path::Path;
use tantivy::schema::Field;
use thiserror::Error;

// A layer of the client-server request structure that handles
// details about the action to be performed by the index writer.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum WriterRequest {
    Insert {
        directory: WriterDirectory,
        document: SearchDocument,
    },
    Delete {
        directory: WriterDirectory,
        field: Field,
        ctids: Vec<u64>,
    },
    DropIndex {
        directory: WriterDirectory,
    },
    Abort,
    Commit,
    Vacuum {
        directory: WriterDirectory,
    },
}

// A layer of the client-server request structure that handles
// details around actions the server should perform.
#[derive(Deserialize, Serialize)]
enum ServerRequest<T: Serialize> {
    /// Request with payload.
    Request(T),
    /// Initiate a data transfer using the pipe path given.
    Transfer(String),
    /// Close the writer server, should only be called by
    /// shutdown background worker.
    Shutdown,
}

/// This trait is the interface that binds the writer to the server.
/// The two systems are otherwise decoupled, so they can be tested
/// and re-used independently.
pub trait Handler<T: DeserializeOwned> {
    fn handle(&mut self, request: T) -> Result<(), ServerError>;
}

pub trait WriterClient<T: Serialize> {
    fn request(&mut self, request: T) -> Result<(), ClientError>;

    fn transfer<P: AsRef<Path>>(&mut self, pipe_path: P, request: T) -> Result<(), ClientError>;
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("unsupported value for attribute '{0}': {1}")]
    UnsupportedValue(String, String),

    #[error("could not dereference postgres datum")]
    DatumDeref,

    #[error("couldn't get writer for {0:?}: {1}")]
    GetWriterFailed(WriterDirectory, String),

    #[error("{0} has a type oid of InvalidOid")]
    InvalidOid(String),

    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("couldn't remove index files on drop_index: {0}")]
    DeleteDirectory(#[from] SearchDirectoryError),
}

#[cfg(test)]
mod tests {
    use super::SearchDocument;
    use crate::{fixtures::*, writer::WriterRequest};
    use rstest::*;
    use tantivy::schema::Field;

    #[rstest]
    fn test_writer_request_serialization(
        #[from(simple_doc)] document: SearchDocument,
        mock_dir: MockWriterDirectory,
    ) {
        // Setup insert writer request.
        let insert_request = WriterRequest::Insert {
            directory: mock_dir.writer_dir.clone(),
            document,
        };

        let ser = bincode::serialize(&insert_request).unwrap();
        let de: WriterRequest = bincode::deserialize(&ser).unwrap();

        // Ensure deserialized request is equal.
        assert_eq!(de, insert_request);

        // Setup delete writer request.
        let delete_request = WriterRequest::Delete {
            directory: mock_dir.writer_dir.clone(),
            field: Field::from_field_id(100),
            ctids: vec![99, 98, 97],
        };

        let ser = bincode::serialize(&delete_request).unwrap();
        let de: WriterRequest = bincode::deserialize(&ser).unwrap();

        // Ensure deserialized request is equal.
        assert_eq!(de, delete_request);
    }
}
