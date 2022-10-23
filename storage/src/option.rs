use crate::snapshot::Snapshot;

#[derive(Debug, Default)]
pub struct GetOption {
    must_fetch_value: bool,
    snapshot: Option<Snapshot>,
    fetch_delete: bool,
}

impl GetOption {
    pub fn set_snapshot(mut self, snapshot: Snapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    pub fn set_must_fetch_value(mut self, fetch: bool) -> Self {
        self.must_fetch_value = fetch;
        self
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub fn set_fetch_delete(mut self, fetch: bool) -> Self {
        self.fetch_delete = fetch;
        self
    }
}

impl GetOption {
    pub fn with_snapshot<S: Into<Snapshot>>(snapshot: S) -> Self {
        Self {
            must_fetch_value: false,
            snapshot: Some(snapshot.into()),
            fetch_delete: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct WriteOption {
    fsync: bool,
}

impl WriteOption {
    pub fn fsync(&self) -> bool {
        self.fsync
    }
}
