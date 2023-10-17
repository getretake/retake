use pgrx::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Write};
use std::ffi::CStr;

#[derive(PostgresType, Serialize, Deserialize, Clone, Debug)]
#[repr(C)]
#[inoutfuncs]
pub struct Sparse {
    pub entries: Vec<(i32, f64)>,
    pub n: i32,
}

impl Sparse {
    pub fn new(entries: Vec<(i32, f64)>, n: i32) -> Self {
        Self { entries, n }
    }
}

impl Display for Sparse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut current_entry_index = 0;
        for i in 0..self.n {
            if current_entry_index < self.entries.len() && self.entries[current_entry_index].0 == i {
                write!(f, "{}", self.entries[current_entry_index].1)?;
                current_entry_index += 1;
            } else {
                write!(f, "0")?;
            }
            if i < self.n - 1 {
                write!(f, ",")?;
            }
        }
        write!(f, "]")
    }
}

impl InOutFuncs for Sparse {
    fn input(input: &CStr) -> Sparse {
        let s = input.to_str().unwrap().trim_matches('[').trim_matches(']');
        let parts: Vec<&str> = s.split(',').collect();

        let mut entries = Vec::new();
        for (position, value_str) in parts.iter().enumerate() {
            let value: f64 = value_str.parse().unwrap();
            if value != 0.0 {
                entries.push((position as i32, value));
            }
        }

        let n = parts.len() as i32;
        Sparse { entries, n }
    }

    fn output(&self, buffer: &mut StringInfo) {
        let mut output_vec = Vec::new();
        
        for i in 0..self.n {
            let value = self.entries.iter().find(|&&(position, _)| position == i)
                .map(|&(_, value)| value)
                .unwrap_or(0.0);
            
            output_vec.push(format!("{}", value));
        }
        
        let output_str = format!("[{}]", output_vec.join(","));
        buffer.write_fmt(format_args!("{}", output_str)).unwrap();
    }
}

pub struct SparseIndex {
    pub name: String,
}

impl SparseIndex {
    pub fn new(name: String) -> Self {
        info!("TODO: Create HNSW index");
        Self { name: name }
    }

    pub fn from_index_name(name: String) -> Self {
        info!("TODO: Retrieve HNSW index");
        Self { name: name }
    }

    pub fn insert(&mut self, sparse_vector: Sparse, heap_tid: pg_sys::ItemPointerData) {
        info!(
            "TODO: Insert {:?} with ID {:?} into index",
            sparse_vector, heap_tid
        );
    }

    pub fn search(self, sparse_vector: Sparse) -> Vec<pg_sys::ItemPointerData> {
        info!("TODO: Implement HNSW search to return results sorted by ID {:?}", sparse_vector);
        vec![]
    }

    pub fn bulk_delete(
        &self,
        stats_binding: *mut pg_sys::IndexBulkDeleteResult,
        callback: pg_sys::IndexBulkDeleteCallback,
        callback_state: *mut ::std::os::raw::c_void,
    ) {
        info!("TODO: Implement delete")
    }
}
