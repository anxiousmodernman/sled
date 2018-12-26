use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

type Item = usize;
type Version = usize;

fn bump_gte(a: &AtomicUsize, to: Version) {
    let mut current = a.load(Ordering::Acquire);
    while current < to as Version {
        let last = a.compare_and_swap(
            current,
            to as Version,
            Ordering::SeqCst,
        );
        if last == current {
            // we succeeded.
            return;
        }
        current = last;
    }
}

#[derive(Debug, Clone)]
pub enum Error {
    /// This transaction failed due to a conflicting concurrent access
    Conflict,
    /// This transaction failed due to an explicit abort from the user
    Aborted,
    /// An issue with the underlying storage system was encountered
    Storage(crate::Error<()>),
}

/// The result of a transactional operation
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Default)]
pub(crate) struct Vsn {
    stable_wts: AtomicUsize,
    pending_wts: AtomicUsize,
    rts: AtomicUsize,
}

pub struct Transactor {
    items: pagetable::PageTable<Vsn>,
}

impl Transactor {}

/// A transaction
#[derive(Debug)]
pub struct Tx<'a, M>
where
    M: super::mvcc::Mvcc,
{
    id: Version,
    mvcc: &'a M,
    reads: Vec<(Item, Version)>,
    writes: Vec<Item>,
    aborted: bool,
}

impl<'a, M> Tx<'a, M>
where
    M: super::mvcc::Mvcc,
{
    fn abort_and_clean(
        &'a self,
        w_vsns: BTreeMap<&'a [u8], Arc<Vsn>>,
        n: usize,
    ) -> Result<()> {
        for (_k, w_vsn) in w_vsns.into_iter().take(n) {
            let res = w_vsn.pending_wts.compare_and_swap(
                self.id,
                0,
                Ordering::SeqCst,
            );
            assert_eq!(
                res, self.id,
                "somehow another transaction aborted ours"
            );
        }

        Err(Error::Aborted)
    }

    /// Complete the transaction
    pub fn commit(self) -> Result<()> {
        if self.aborted {
            return Err(Error::Aborted);
        }

        let reads = self
            .mvcc
            .versions_for_keys(self.reads.keys().map(|k| &**k));

        let writes = self
            .mvcc
            .versions_for_keys(self.writes.keys().map(|k| &**k));

        // install pending
        let mut worked = 0;
        for (_k, w_vsn) in &writes {
            match w_vsn.pending_wts.compare_and_swap(
                0,
                self.id,
                Ordering::SeqCst,
            ) {
                0 => {
                    worked += 1;
                }
                _ => {
                    return self.abort_and_clean(writes, worked);
                }
            }
        }

        // update rts
        for (_k, r_vsn) in &reads {
            bump_gte(&r_vsn.rts, self.id);
        }

        // check version consistency
        for (k, r_vsn) in &reads {
            let cur_wts = r_vsn.stable_wts.load(Ordering::Acquire);
            let read_wts = self.reads[*k];

            if cur_wts != read_wts {
                // Another transaction modified our readset
                // before we could commit
                return self.abort_and_clean(writes, worked);
            }

            if cur_wts > self.id {
                // we read a version from a later transaction than
                // ours, and we have to throw away our work because
                // we broke serializability.
                return self.abort_and_clean(writes, worked);
            }

            let pending_wts =
                r_vsn.pending_wts.load(Ordering::Acquire);

            if pending_wts != 0 && pending_wts < self.id {
                // encountered a pending transaction in our
                // read set that was earlier ours. we don't
                // know if it will commit or not, so we
                // can either block on it to finish
                // or just pessimistically abort.
                return self.abort_and_clean(writes, worked);
            }
        }

        let mut w_current_wts = Vec::with_capacity(self.writes.len());

        for (k, w_vsn) in &writes {
            let cur_rts = w_vsn.rts.load(Ordering::Acquire);

            if cur_rts > self.id {
                // a transaction with a later timestamp read
                // the current stable version before we could
                // commit. if we commit anyway, we could
                // cause write skew etc... for them, breaking
                // serializability.
                return self.abort_and_clean(writes, worked);
            }

            let cur_wts = w_vsn.stable_wts.load(Ordering::Acquire);

            if cur_wts > self.id {
                // our transaction was beaten by another with
                // a later timestamp.
                return self.abort_and_clean(writes, worked);
            }

            w_current_wts.push(cur_wts);
        }

        // log

        // commit
        for (i, (_k, w_vsn)) in writes.iter().enumerate() {
            let read_cur_wts = w_current_wts[self.writes.len() - i];
            let res1 = w_vsn.stable_wts.compare_and_swap(
                read_cur_wts,
                self.id,
                Ordering::SeqCst,
            );
            assert_eq!(
                res1,
                read_cur_wts,
                "another transaction unexpectedly modified our stable_wts",
            );

            let res2 = w_vsn.stable_wts.compare_and_swap(
                self.id,
                0,
                Ordering::SeqCst,
            );
            assert_eq!(
                res2,
                self.id,
                "another transaction unexpectedly modified our pending_wts",
            );
        }

        Ok(())
    }
}
