#![allow(unused)]

use std::{
    collections::{BTreeMap, BTreeSet},
    hash::Hash,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use super::*;

const VERSION_BYTES: usize = std::mem::size_of::<usize>();

/// High-level transaction helpers
pub mod tx {
    /// An error type signalling how a transaction failed
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
}

#[derive(Debug, Clone)]
enum Update {
    Set(Vec<u8>),
    Merge(Vec<u8>),
    Del,
}

#[derive(Debug, Default)]
struct Vsn {
    stable_wts: AtomicUsize,
    pending_wts: AtomicUsize,
    rts: AtomicUsize,
}

/// A transaction
#[derive(Debug)]
pub struct Tx<'a> {
    id: usize,
    db: &'a Db,
    reads: BTreeMap<Vec<u8>, usize>,
    writes: BTreeMap<Vec<u8>, Update>,
    aborted: bool,
}

impl<'a> Tx<'a> {
    fn abort_and_clean(
        &'a self,
        w_vsns: BTreeMap<&'a [u8], Arc<Vsn>>,
        n: usize,
    ) -> tx::Result<()> {
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

        Err(tx::Error::Aborted)
    }

    /// Complete the transaction
    pub fn commit(self) -> tx::Result<()> {
        if self.aborted {
            return Err(tx::Error::Aborted);
        }

        let reads = self
            .db
            .versions_for_keys(self.reads.keys().map(|k| &**k));

        let writes = self
            .db
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

    /// Give up
    pub fn abort(&mut self) {
        self.aborted = true;
    }

    /// Retrieve a value from the `Db` if it exists.
    pub fn get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
    ) -> Result<Option<Vec<u8>>, ()> {
        // check writeset
        if let Some(update) = self.writes.get(key.as_ref()) {
            match update {
                Update::Del => return Ok(None),
                Update::Merge(..) => unimplemented!(
                    "transactional merges not supported yet"
                ),
                Update::Set(v) => return Ok(Some(v.clone())),
            }
        }

        // check readset
        if let Some(observed) = self.reads.get(key.as_ref()) {
            if *observed == 0 {
                return Ok(None);
            }
            return self
                .db
                .bounded_get(key, *observed)
                .map(|opt| opt.map(|(vsn, val)| (*val).to_vec()));
        }

        // pull key from local cache
        if let Some((vsn, val)) =
            self.db.bounded_get(&key, self.id)?
        {
            self.reads.insert(key.as_ref().to_vec(), vsn);
            Ok(Some((*val).to_vec()))
        } else {
            self.reads.insert(key.as_ref().to_vec(), 0);
            Ok(None)
        }
    }

    /// Set a key to a new value
    pub fn set<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: Value,
    ) -> Result<(), ()> {
        self.writes
            .insert(key.as_ref().to_vec(), Update::Set(value));

        Ok(())
    }

    /// Delete a value
    pub fn del<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), ()> {
        self.writes.insert(key.as_ref().to_vec(), Update::Del);

        Ok(())
    }
}

/// A `Tree` that supports transactional operations.
#[derive(Debug)]
pub struct Db {
    tree: Tree,
    versions: RwLock<BTreeMap<Vec<u8>, Arc<Vsn>>>,
    gc_threshold: AtomicUsize,
}

impl Db {
    /// Load existing or create a new `Db` with a default configuration.
    pub fn start_default<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Db, ()> {
        let tree = Tree::start_default(path)?;

        Ok(Db {
            tree,
            versions: RwLock::new(BTreeMap::new()),
            gc_threshold: 0.into(),
        })
    }

    /// Load existing or create a new `Db`.
    pub fn start(config: Config) -> Result<Db, ()> {
        let tree = Tree::start(config)?;

        Ok(Db {
            tree,
            versions: RwLock::new(BTreeMap::new()),
            gc_threshold: 0.into(),
        })
    }

    /// Flushes all dirty IO buffers and calls fsync.
    /// If this succeeds, it is guaranteed that
    /// all previous writes will be recovered if
    /// the system crashes.
    pub fn flush(&self) -> Result<(), ()> {
        self.tree.flush()
    }

    /// Run a transaction, retrying if a concurrent transaction
    /// causes a `Conflict`.
    pub fn tx<'a, F, R>(&'a self, f: F) -> Result<tx::Result<R>, ()>
    where
        F: Fn(&mut Tx<'a>) -> tx::Result<R>,
    {
        loop {
            let guard = pin();

            let id = self.tree.generate_id()?;

            let mut tx = Tx {
                id,
                reads: BTreeMap::new(),
                writes: BTreeMap::new(),
                db: &self,
                aborted: false,
            };

            let res = f(&mut tx);

            match tx.commit() {
                Ok(()) => return Ok(res),
                Err(tx::Error::Conflict) => {
                    // retry
                }
                Err(other) => return Ok(Err(other)),
            }
        }
    }

    /// Begin a new transaction.
    pub fn begin_tx(&self) -> Result<Tx<'_>, ()> {
        let id = self.tree.generate_id()?;

        Ok(Tx {
            id,
            reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            db: &self,
            aborted: false,
        })
    }

    fn bounded_get<K: AsRef<[u8]>>(
        &self,
        key: K,
        bound: usize,
    ) -> Result<Option<(usize, PinnedValue)>, ()> {
        let gc_threshold =
            self.gc_threshold.load(Ordering::Acquire) as usize;

        let versioned_key = version_key(&key, bound);

        let mut ret = None;

        for result in self.tree.range(key.as_ref()..&*versioned_key) {
            let (k, value) = result?;
            assert!(k.starts_with(key.as_ref()));
            if k.len() != versioned_key.len() {
                continue;
            }
            let version = version_of_key(&key);
            if version <= bound {
                if let Some((old_version, old_k, _)) = ret.take() {
                    // clean up old versions
                    if old_version <= gc_threshold {
                        self.tree.del(old_k)?;
                    }
                }
                ret = Some((version, k, value));
            } else {
                break;
            }
        }
        Ok(ret.map(|r| (r.0, r.2)))
    }

    fn versions_for_keys<'a, I>(
        &self,
        keys: I,
    ) -> BTreeMap<&'a [u8], Arc<Vsn>>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut ret = BTreeMap::new();

        // TODO use readers mostly
        let mut versions = self.versions.write().unwrap();

        for key in keys {
            let vsn = versions.entry(key.to_vec()).or_default();
            ret.insert(key, vsn.clone());
        }

        ret
    }
}

fn bump_gte(a: &AtomicUsize, to: usize) {
    let mut current = a.load(Ordering::Acquire);
    while current < to as usize {
        let last = a.compare_and_swap(
            current,
            to as usize,
            Ordering::SeqCst,
        );
        if last == current {
            // we succeeded.
            return;
        }
        current = last;
    }
}

fn version_key<K: AsRef<[u8]>>(key: K, version: usize) -> Vec<u8> {
    let target_len = key.as_ref().len() + VERSION_BYTES;
    let mut ret = Vec::with_capacity(target_len);
    unsafe {
        ret.set_len(target_len);
    }

    let version_buf: [u8; VERSION_BYTES] =
        unsafe { std::mem::transmute(version) };
    ret[0..key.as_ref().len()].copy_from_slice(key.as_ref());
    ret[key.as_ref().len()..target_len].copy_from_slice(&version_buf);
    ret
}

fn version_of_key<K: AsRef<[u8]>>(key: K) -> usize {
    let k = key.as_ref();
    assert!(k.len() >= VERSION_BYTES);
    let mut version_buf = [0u8; VERSION_BYTES];
    version_buf.copy_from_slice(&k[k.len() - VERSION_BYTES..]);

    unsafe { std::mem::transmute(version_buf) }
}

#[test]
fn basic_tx_functionality() -> Result<(), ()> {
    let config = ConfigBuilder::new().temporary(true).build();
    let db = Db::start(config)?;

    let mut tx1 = db.begin_tx()?;
    tx1.set(b"a", vec![1])?;
    tx1.commit().unwrap();

    let mut tx2 = db.begin_tx()?;
    let read = tx2.get(b"a").unwrap().unwrap();
    tx2.commit().unwrap();
    assert_eq!(*read, *vec![1u8]);

    Ok(())
}

#[test]
fn g0() -> Result<(), ()> {
    /*
    echo "Running g0 test."
    tell 0 "begin"
    tell 1 "begin"
    tell 0 "update test set value = 11 where id = 1"
    tell 1 "update test set value = 12 where id = 1"
    tell 0 "update test set value = 21 where id = 2"
    tell 0 "commit"
    tell 0 "select * from test"
    tell 1 "update test set value = 22 where id = 2"
    tell 1 "commit" # Rejected with ERROR: FoundationDB commit aborted: 1020 - not_committed
    tell 0 "select * from test" # Shows 1 => 11, 2 => 21
    tell 1 "select * from test" # Shows 1 => 11, 2 => 21
    */
    let config = ConfigBuilder::new().temporary(true).build();
    let db = Db::start(config)?;
    let mut tx0 = db.begin_tx()?;
    let mut tx1 = db.begin_tx()?;

    Ok(())
}
