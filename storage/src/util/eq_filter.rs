pub struct EqualFilter<I, EQ>
where
    I: Iterator,
{
    iter: I,
    last: Option<I::Item>,
    eq: EQ,
}

impl<I, EQ> EqualFilter<I, EQ>
where
    I: Iterator,
{
    pub(crate) fn new(iter: I, eq: EQ) -> Self {
        Self {
            iter,
            last: None,
            eq,
        }
    }
}

impl<I, EQ> Iterator for EqualFilter<I, EQ>
where
    I: Iterator,
    I::Item: PartialEq<I::Item> + Clone,
    EQ: FnMut(&I::Item, &I::Item) -> bool,
{
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let a = self.iter.next()?;
            if let Some(last) = &self.last {
                if !(self.eq)(last, &a) {
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
