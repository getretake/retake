use crate::postgres::storage::block::{bm25_max_free_space, FileEntry};
use crate::postgres::storage::LinkedBytesList;
use pgrx::*;
use std::io::{Result, Write};
use std::path::{Path, PathBuf};
use tantivy::directory::{AntiCallToken, TerminatingWrite};

pub struct SegmentComponentWriter {
    path: PathBuf,
    header_blockno: pg_sys::BlockNumber,
    total_bytes: usize,
    buffer: ExactBuffer<{ bm25_max_free_space() }, LinkedBytesList>,
}

impl SegmentComponentWriter {
    pub unsafe fn new(relation_oid: pg_sys::Oid, path: &Path) -> Self {
        let segment_component = LinkedBytesList::create(relation_oid);

        Self {
            path: path.to_path_buf(),
            header_blockno: segment_component.header_blockno,
            total_bytes: 0,
            buffer: ExactBuffer {
                writer: segment_component,
                buffer: [0; bm25_max_free_space()],
                len: 0,
            },
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn file_entry(&self) -> FileEntry {
        FileEntry {
            staring_block: self.header_blockno,
            total_bytes: self.total_bytes,
        }
    }
}

impl Write for SegmentComponentWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        let many = self.buffer.write(data)?;
        self.total_bytes += data.len();
        Ok(many)
    }

    fn flush(&mut self) -> Result<()> {
        self.buffer.flush()
    }
}

impl TerminatingWrite for SegmentComponentWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> Result<()> {
        // this is a no-op -- the FileEntry for this segment component
        // is handled through Directory::save_meta()
        Ok(())
    }
}

/// Similar to `[std::io::BufWriter]` except it only writes in increments of the `const CAPACITY: usize`
/// capacity.  Except on `flush()` where any remaining bytes are written.
struct ExactBuffer<const CAPACITY: usize, W: Write> {
    writer: W,
    buffer: [u8; CAPACITY],
    len: usize,
}

impl<const CAPACITY: usize, W: Write> Drop for ExactBuffer<CAPACITY, W> {
    fn drop(&mut self) {
        self.flush().ok();
    }
}

impl<const CAPACITY: usize, W: Write> Write for ExactBuffer<CAPACITY, W> {
    fn write(&mut self, mut data: &[u8]) -> Result<usize> {
        let len = data.len();

        let mut end = len;
        if self.len < CAPACITY {
            // fill the buffer with what we can
            end = (CAPACITY - self.len).min(end);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    self.buffer.as_mut_ptr().add(self.len),
                    end,
                );
            }
            self.len += end;
            data = &data[end..];
        }

        if self.len == CAPACITY {
            // buffer is full -- write it out
            let _ = self.writer.write(&self.buffer)?;
            self.len = 0;
        }

        while data.len() >= CAPACITY {
            // data has at least as many bytes as our capacity
            // write it out in chunks of our capacity, to avoid copying it into the buffer
            end = CAPACITY;
            let _ = self.writer.write(&data[..end])?;
            data = &data[end..];
        }

        if !data.is_empty() {
            // copy the rest to our buffer -- it'll fit
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), self.buffer.as_mut_ptr(), data.len());
                self.len = data.len();
            }
        }

        Ok(len)
    }

    fn flush(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            let _ = self.writer.write(&self.buffer[0..self.len])?;
            self.len = 0;
        }
        Ok(())
    }
}
