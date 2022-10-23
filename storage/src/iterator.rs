use std::{collections::BinaryHeap, sync::Arc};

use bytes::Bytes;
use log::info;

pub trait KvIterator: Iterator {
    fn prefetch(&mut self, n: usize);
}

pub trait KvIteratorItem {
    fn key(&self) -> &Bytes;
    fn version(&self) -> u64;
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
        match self.t.key().cmp(other.t.key()) {
            std::cmp::Ordering::Equal => self.t.version().cmp(&other.t.version()),
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
        self.t.key() == other.t.key() && self.t.version() == other.t.version()
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

pub struct MergedIter<T> {
    iters: Vec<ScanIter<T>>,
    heap: BinaryHeap<MergedItem<T>>,
    last_key: Option<Bytes>,
    init: bool,
}

impl<T> MergedIter<T>
where
    T: KvIteratorItem,
{
    pub fn new(iters: Vec<ScanIter<T>>) -> Self {
        Self {
            iters,
            heap: BinaryHeap::new(),
            last_key: None,
            init: false,
        }
    }
}

impl<T> KvIterator for MergedIter<T>
where
    T: KvIteratorItem,
{
    fn prefetch(&mut self, _n: usize) {}
}

impl<T> Iterator for MergedIter<T>
where
    T: KvIteratorItem,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            for (idx, iter) in self.iters.iter_mut().enumerate() {
                match iter.next() {
                    Some(val) => self.heap.push(MergedItem { t: val, idx }),
                    None => (),
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
                if last_key == item.t.key() {
                    continue;
                }
            }
            self.last_key = Some(item.t.key().clone());
            break Some(item.t);
        }
    }
}

pub trait IteratorContext {
    fn release(&mut self);
}

pub struct ScanIter<T> {
    inner: Box<dyn Iterator<Item = T>>,
    state: Option<Arc<dyn IteratorContext>>,
}

impl<T> ScanIter<T> {
    pub fn new<I: Iterator<Item = T> + 'static>(inner: I) -> Self {
        Self {
            inner: Box::new(inner),
            state: None,
        }
    }
    pub fn with(mut self, ctx: Arc<dyn IteratorContext>) -> Self {
        self.state = Some(ctx);
        self
    }
}

impl<T> KvIterator for ScanIter<T> {
    fn prefetch(&mut self, _n: usize) {}
}

impl<T> Iterator for ScanIter<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait EqualKey {
    fn equal_key(&self, other: &Self) -> bool;
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
    I::Item: EqualKey,
{
    pub(crate) fn new(iter: I) -> Self {
        Self { iter, last: None }
    }
}

impl<I> Iterator for EqualFilter<I>
where
    I: Iterator,
    I::Item: EqualKey + PartialEq<I::Item> + Clone,
{
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let a = self.iter.next()?;
            if let Some(last) = &self.last {
                if last.equal_key(&a) {
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
