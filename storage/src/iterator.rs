pub trait KvIterator: Iterator {
    fn prefetch(&mut self, n: usize);
}

pub struct MergedIter<T> {
    iters: Vec<Box<dyn KvIterator<Item = T>>>,
}

impl<T> MergedIter<T> {
    pub fn new(iters: Vec<Box<dyn KvIterator<Item = T>>>) -> Self {
        Self { iters }
    }
}

impl<T> KvIterator for MergedIter<T> {
    fn prefetch(&mut self, n: usize) {
        todo!()
    }
}

impl<T> Iterator for MergedIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct Iter<I> {
    inner: I,
}

impl<I> From<I> for Iter<I> {
    fn from(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> KvIterator for Iter<I>
where
    I: Iterator,
{
    fn prefetch(&mut self, n: usize) {}
}

impl<I> Iterator for Iter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

