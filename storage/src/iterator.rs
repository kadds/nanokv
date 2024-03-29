use std::{collections::BinaryHeap, marker::PhantomData};

use bytes::Bytes;

pub trait KvIterator: Iterator {
    fn prefetch(&mut self, n: usize);
}

pub trait KvIteratorItem: Clone {
    fn user_key_slice(&self) -> &[u8];
    fn user_key(&self) -> Bytes;
    fn seq(&self) -> u64;
    fn deleted(&self) -> bool;
}

struct MergedItem<T> {
    t: T,
    idx: usize,
}

impl<T> Ord for MergedItem<T>
where
    T: KvIteratorItem,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.t.user_key_slice().cmp(other.t.user_key_slice()) {
            std::cmp::Ordering::Equal => self.t.seq().cmp(&other.t.seq()),
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
        }
    }
}

impl<T> Eq for MergedItem<T> where T: KvIteratorItem {}

impl<T> PartialEq for MergedItem<T>
where
    T: KvIteratorItem,
{
    fn eq(&self, other: &Self) -> bool {
        self.t.user_key_slice() == other.t.user_key_slice() && self.t.seq() == other.t.seq()
    }
}

impl<T> PartialOrd for MergedItem<T>
where
    T: KvIteratorItem,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergedIter<'a, T> {
    iters: Vec<ScanIter<'a, T>>,
    heap: BinaryHeap<MergedItem<T>>,
    last_key: Option<Bytes>,
    init: bool,
}

impl<'a, T> MergedIter<'a, T>
where
    T: KvIteratorItem,
{
    pub fn new(iters: Vec<ScanIter<'a, T>>) -> Self {
        Self {
            iters,
            heap: BinaryHeap::new(),
            last_key: None,
            init: false,
        }
    }
}

impl<'a, T> KvIterator for MergedIter<'a, T>
where
    T: KvIteratorItem,
{
    fn prefetch(&mut self, _n: usize) {}
}

impl<'a, T> Iterator for MergedIter<'a, T>
where
    T: KvIteratorItem,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            for (idx, iter) in self.iters.iter_mut().enumerate() {
                if let Some(val) = iter.next() {
                    self.heap.push(MergedItem { t: val, idx });
                }
            }

            self.init = true;
        }
        loop {
            let item = self.heap.pop()?;

            if let Some(new_val) = self.iters[item.idx].next() {
                self.heap.push(MergedItem {
                    t: new_val,
                    idx: item.idx,
                });
            }
            if let Some(last_key) = &self.last_key {
                if last_key == item.t.user_key_slice() {
                    continue;
                }
            }
            self.last_key = Some(item.t.user_key());
            break Some(item.t);
        }
    }
}

pub struct ScanIter<'a, T> {
    inner: Box<dyn Iterator<Item = T> + 'a>,
    _pd: PhantomData<&'a ()>,
}

impl<'a, T> ScanIter<'a, T> {
    pub fn new<I: Iterator<Item = T> + 'a>(inner: I) -> Self {
        Self {
            inner: Box::new(inner),
            _pd: PhantomData::default(),
        }
    }
}

impl<'a, T> KvIterator for ScanIter<'a, T> {
    fn prefetch(&mut self, _n: usize) {}
}

impl<'a, T> Iterator for ScanIter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct EqualFilter<I>
where
    I: Iterator,
{
    iter: I,
    last: Option<I::Item>,
}

impl<I> EqualFilter<I>
where
    I: Iterator,
    I::Item: KvIteratorItem,
{
    pub(crate) fn new(iter: I) -> Self {
        Self { iter, last: None }
    }
}

impl<I> Iterator for EqualFilter<I>
where
    I: Iterator,
    I::Item: KvIteratorItem,
{
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let a = self.iter.next()?;
            if let Some(last) = &self.last {
                if last.user_key_slice() == a.user_key_slice() {
                    // item filtered
                    continue;
                }
            }
            self.last = Some(a.clone());
            break Some(a);
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

pub struct LevelIter<'a, T> {
    iters: Vec<ScanIter<'a, T>>,
    pos: usize,
}

impl<'a, T> LevelIter<'a, T>
where
    T: KvIteratorItem,
{
    pub fn new(iters: Vec<ScanIter<'a, T>>) -> Self {
        Self { iters, pos: 0 }
    }
}

impl<'a, T> Iterator for LevelIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos >= self.iters.len() {
                return None;
            }
            if let Some(v) = self.iters[self.pos].next() {
                return Some(v);
            }
            self.pos += 1;
        }
    }
}
