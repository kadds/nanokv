use std::{
    ops::RangeBounds,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Bytes;

use super::{superversion::Lifetime, GetOption, Memtable};
use crate::{
    err::{Result, StorageError},
    iterator::{EqualFilter, KvIteratorItem, MergedIter, ScanIter},
    key::{InternalKey, Value},
};

#[derive(Debug)]
pub struct Imemtable {
    m: Memtable,
}

impl Imemtable {
    pub fn new(memtable: Memtable) -> Self {
        Self { m: memtable }
    }
}

impl Imemtable {
    pub fn min_max_user_key(&self) -> Option<(Bytes, Bytes)> {
        if !self.m.is_empty() {
            Some((self.m.first_key().user_key(), self.m.last_key().user_key()))
        } else {
            None
        }
    }

    pub fn min_seq(&self) -> u64 {
        self.m.min_seq()
    }

    pub fn max_seq(&self) -> u64 {
        self.m.max_seq()
    }

    pub fn entry_iter<'a>(&'a self) -> Box<dyn Iterator<Item = (InternalKey, Bytes)> + 'a> {
        Box::new(self.m.iter().cloned())
    }

    pub fn len(&self) -> usize {
        self.m.len()
    }

    pub fn is_empty(&self) -> bool {
        self.m.is_empty()
    }

    pub fn number(&self) -> u64 {
        self.m.number()
    }
}

impl Imemtable {
    pub fn get<'a>(
        &self,
        opt: &GetOption,
        key: Bytes,
        _lifetime: &Lifetime<'a>,
    ) -> Result<(InternalKey, Value)> {
        self.m.get(opt, key, _lifetime)
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &self,
        opt: &GetOption,
        range: R,
        _mark: &Lifetime<'a>,
    ) -> ScanIter<'a, (InternalKey, Value)> {
        self.m.scan(opt, range, _mark)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Imemtables {
    pub imemtables: Vec<Arc<Imemtable>>,
}

impl Imemtables {
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Imemtable>> {
        self.imemtables.iter()
    }

    pub fn empty(&self) -> bool {
        self.imemtables.is_empty()
    }

    pub fn push(&self, imemtable: Arc<Imemtable>) -> Self {
        let mut imemtables = self.imemtables.clone();
        imemtables.push(imemtable);

        Self { imemtables }
    }

    pub fn remove(&self, number: u64) -> Self {
        let mut imemtables = self.imemtables.clone();
        if let Some(idx) = imemtables
            .iter()
            .enumerate()
            .find(|val| val.1.number() == number)
        {
            imemtables.remove(idx.0);
        }
        Self { imemtables }
    }
}

impl Imemtables {
    pub fn get<'a>(
        &self,
        opt: &GetOption,
        key: Bytes,
        lifetime: &Lifetime<'a>,
    ) -> Result<(InternalKey, Value)> {
        for table in self.imemtables.iter().rev() {
            match table.get(opt, key.clone(), lifetime) {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if let StorageError::KeyNotExist = e {
                        return Err(e);
                    }
                }
            }
        }
        Err(StorageError::KeyNotExist)
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &self,
        opt: &GetOption,
        range: R,
        lifetime: &Lifetime<'a>,
    ) -> ScanIter<'a, (InternalKey, Value)> {
        let mut iters = Vec::new();
        for table in self.imemtables.iter().rev() {
            iters.push(table.scan(opt, range.clone(), lifetime));
        }

        ScanIter::new(MergedIter::new(iters))
    }
}

#[cfg(test)]
mod test {
    use crate::iterator::KvIteratorItem;

    use super::*;

    #[test]
    pub fn get_imemtable() {
        let (sorted_input, table, ver) = crate::test::init_itable();

        let opt = GetOption::default();
        let lifetime = Lifetime::default();

        // basic get
        assert_eq!(
            table.get(&opt, "101".into(), &lifetime).unwrap().seq(),
            sorted_input[1].1
        );

        // multi-version get
        let v3 = table.get(&opt, "133".into(), &lifetime);
        assert_eq!(v3.unwrap().seq(), ver - 3);

        // get snapshot
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1 + 1),
            "133".into(),
            &lifetime,
        );
        assert_eq!(v3.unwrap().seq(), sorted_input[33].1);
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1),
            "133".into(),
            &lifetime,
        );
        assert_eq!(v3.unwrap().seq(), sorted_input[33].1);

        // get deleted
        let v4 = table.get(&opt, "144".into(), &lifetime);
        assert!(v4.unwrap().deleted());

        let v4 = table.get(&GetOption::with_snapshot(ver - 2), "144".into(), &lifetime);
        assert_eq!(v4.unwrap().seq(), ver - 4);

        let v4 = table.get(&GetOption::with_snapshot(ver - 5), "144".into(), &lifetime);
        assert_eq!(v4.unwrap().seq(), sorted_input[44].1);

        // set & delete & set

        let v5 = table.get(&GetOption::with_snapshot(ver - 2), "155".into(), &lifetime);
        assert!(v5.unwrap().deleted());

        // scan tables
        assert_eq!(
            table
                .scan(&opt, Bytes::from("100")..=Bytes::from("110"), &lifetime)
                .count(),
            11
        );

        // full scan
        assert_eq!(table.scan(&opt, .., &lifetime).count(), 200);
        // filter deleted
        assert_eq!(
            table
                .scan(&opt, .., &lifetime)
                .filter(|(k, v)| !k.deleted())
                .count(),
            199
        );

        // scan all
        for (idx, entry) in table.scan(&opt, .., &lifetime).enumerate() {
            assert_eq!(sorted_input[idx].0, entry.0.user_key());
        }
    }
}
