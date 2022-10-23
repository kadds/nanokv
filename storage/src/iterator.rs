use std::{collections::BinaryHeap, marker::PhantomData, sync::Arc};

use bytes::Bytes;

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
        match self.t.key().cmp(&other.t.key()) {
            std::cmp::Ordering::Equal => self.t.version().cmp(&other.t.version()),
            o => o,
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
    fn prefetch(&mut self, n: usize) {}
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

pub struct ContextIter<I> {
    context: Vec<Box<dyn IteratorContext>>,
    inner: I,
}

impl<I> ContextIter<I> {
    pub fn new(inner: I) -> Self {
        Self {
            context: Vec::new(),
            inner,
        }
    }

    pub fn with_context(inner: I, ctx: Box<dyn IteratorContext>) -> Self {
        let context = vec![ctx];
        Self { context, inner }
    }
}

impl<I> KvIterator for ContextIter<I>
where
    I: Iterator,
{
    fn prefetch(&mut self, n: usize) {}
}

impl<I> Iterator for ContextIter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<I> Drop for ContextIter<I> {
    fn drop(&mut self) {
        for context in &mut self.context {
            context.release();
        }
    }
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
    fn prefetch(&mut self, n: usize) {}
}

impl<T> Iterator for ScanIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
