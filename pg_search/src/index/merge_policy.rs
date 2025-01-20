use crate::postgres::storage::block::{MergeLockData, MERGE_LOCK};
use crate::postgres::storage::buffer::{BufferManager, PinnedBuffer};
use pgrx::pg_sys;
use tantivy::indexer::{MergeCandidate, MergePolicy};
use tantivy::SegmentMeta;

#[derive(Debug, Clone)]
pub enum AllowedMergePolicy {
    None,
    NPlusOne,
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
#[derive(Debug)]
pub struct MergeLock {
    num_segments: u32,
    _buffer: PinnedBuffer,
}

impl MergeLock {
    // This lock is acquired by inserts that attempt to merge segments
    // Merges should only happen if there is no other merge in progress
    // AND the effects of the previous merge are visible
    pub unsafe fn acquire_for_merge(relation_oid: pg_sys::Oid) -> Option<Self> {
        if !pg_sys::IsTransactionState() {
            return None;
        }

        let mut bman = BufferManager::new(relation_oid);

        if let Some(mut merge_lock) = bman.get_buffer_for_cleanup_conditional(
            MERGE_LOCK,
            pg_sys::GetAccessStrategy(pg_sys::BufferAccessStrategyType::BAS_NORMAL),
        ) {
            let mut page = merge_lock.page_mut();
            let metadata = page.contents_mut::<MergeLockData>();
            let last_merge = metadata.last_merge;
            let snapshot = pg_sys::GetActiveSnapshot();
            let last_merge_visible = {
                // this is the first merge that's ever happened
                !pg_sys::TransactionIdIsNormal(last_merge)
                // the last merge was committed by the current transaction, so it's visible
                || pg_sys::TransactionIdIsCurrentTransactionId(last_merge)
                // the last merge is visible to the current transaction
                || !pg_sys::XidInMVCCSnapshot(last_merge, snapshot)
            };

            if last_merge_visible {
                metadata.last_merge = pg_sys::GetCurrentTransactionId();
                Some(MergeLock {
                    num_segments: metadata.num_segments,
                    _buffer: merge_lock.unlock(),
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    // This lock must be acquired before ambulkdelete calls commit() on the index
    // We ask for an exclusive lock because ambulkdelete must delete all dead ctids
    pub unsafe fn acquire_for_delete(relation_oid: pg_sys::Oid) -> Self {
        let mut bman = BufferManager::new(relation_oid);
        let mut merge_lock = bman.get_buffer_for_cleanup(
            MERGE_LOCK,
            pg_sys::GetAccessStrategy(pg_sys::BufferAccessStrategyType::BAS_NORMAL),
        );
        let mut page = merge_lock.page_mut();
        let metadata = page.contents_mut::<MergeLockData>();
        metadata.last_vacuum = pg_sys::ReadNextFullTransactionId().value as pg_sys::TransactionId;

        MergeLock {
            num_segments: metadata.num_segments,
            _buffer: merge_lock.unlock(),
        }
    }

    pub fn num_segments(&self) -> u32 {
        self.num_segments
    }
}

pub unsafe fn set_num_segments(relation_oid: pg_sys::Oid, num_segments: u32) {
    let mut bman = BufferManager::new(relation_oid);
    let mut buffer = bman.get_buffer_mut(MERGE_LOCK);
    let mut page = buffer.page_mut();
    let metadata = page.contents_mut::<MergeLockData>();
    metadata.num_segments = num_segments;
}
