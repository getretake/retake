use crate::postgres::storage::block::{MergeLockData, MERGE_LOCK};
use crate::postgres::storage::buffer::{BufferManager, BufferMut};
use pgrx::pg_sys;
use tantivy::indexer::{MergeCandidate, MergePolicy};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::SegmentMeta;

#[derive(Debug, Clone)]
pub enum AllowedMergePolicy {
    None,
    NPlusOne(usize),
}

impl From<AllowedMergePolicy> for Box<dyn MergePolicy> {
    fn from(policy: AllowedMergePolicy) -> Self {
        match policy {
            AllowedMergePolicy::None => Box::new(NoMergePolicy),
            AllowedMergePolicy::NPlusOne(n) => Box::new(NPlusOneMergePolicy {
                n,
                min_num_segments: 2,
            }),
        }
    }
}

/// A tantivy [`MergePolicy`] that endeavours to keep a maximum number of segments "N", plus
/// one extra for leftovers.
///
/// It merges the smallest segments, accounting for deleted docs.
#[derive(Debug)]
pub struct NPlusOneMergePolicy {
    // the number of segments we want to maintain on disk
    pub n: usize,

    // the minimum number of segments to merge together
    // if we don't have this many, no merge is performed
    pub min_num_segments: usize,
}

impl MergePolicy for NPlusOneMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let n = self.n;
        let min_num_segments = self.min_num_segments;

        if segments.len() < n + min_num_segments {
            return vec![];
        }

        // collect a list of the segments and sort them largest-to-smallest, by # of alive docs
        let mut segments = segments.iter().collect::<Vec<_>>();
        segments.sort_unstable_by(|a, b| a.num_docs().cmp(&b.num_docs()).reverse());

        let mut candidate = MergeCandidate(vec![]);
        while segments.len() > n {
            let meta = segments.pop().unwrap();
            candidate.0.push(meta.id());
        }

        if candidate.0.len() < 2 {
            // nothing worth merging
            return vec![];
        }

        vec![candidate]
    }
}

/// Only one merge can happen at a time, so we need to lock the merge process
pub struct MergeLock(BufferMut);

impl MergeLock {
    // This lock is acquired by inserts if merge_on_insert is true
    // Merges should only happen if there is no other merge in progress
    // AND the effects of the previous merge are visible
    pub unsafe fn acquire_for_merge(relation_oid: pg_sys::Oid) -> Option<Self> {
        if !pg_sys::IsTransactionState() {
            return None;
        }

        let mut bman = BufferManager::new(relation_oid);

        if let Some(mut merge_lock) = bman.get_buffer_conditional(MERGE_LOCK) {
            let mut page = merge_lock.page_mut();
            let metadata = page.contents_mut::<MergeLockData>();
            let last_merge = metadata.last_merge;

            let snapshot = pg_sys::GetActiveSnapshot();

            if pg_sys::XidInMVCCSnapshot(last_merge, snapshot) {
                None
            } else {
                Some(MergeLock(merge_lock))
            }
        } else {
            None
        }
    }

    // This lock must be acquired before ambulkdelete calls commit() on the index
    // We ask for an exclusive lock because ambulkdelete must delete all dead ctids
    pub unsafe fn acquire_for_delete(relation_oid: pg_sys::Oid) -> Self {
        let mut bman = BufferManager::new(relation_oid);
        let merge_lock = bman.get_buffer_mut(MERGE_LOCK);
        MergeLock(merge_lock)
    }
}

impl Drop for MergeLock {
    fn drop(&mut self) {
        unsafe {
            if pg_sys::IsTransactionState() {
                let mut page = self.0.page_mut();
                let metadata = page.contents_mut::<MergeLockData>();
                metadata.last_merge = pg_sys::GetCurrentTransactionId();
            }
        }
    }
}
