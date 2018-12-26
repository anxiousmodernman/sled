use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use super::tx::Vsn;

pub trait Mvcc {
    fn gen_version(&self) -> usize {
        static ID: AtomicUsize = AtomicUsize::new(0);
        1 + ID.fetch_add(1, SeqCst)
    }

    fn read_up_to_version(&self, version: usize);

    fn write_atomic_batch(&self);

    fn versions_for_keys<'a, I>(
        &self,
        keys: I,
    ) -> BTreeMap<&'a [u8], Arc<Vsn>>
    where
        I: Iterator<Item = &'a [u8]>;
}
