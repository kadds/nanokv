use crate::snapshot::Snapshot;

#[derive(Debug, Default)]
pub struct GetOption {
    must_fetch_value: bool,
    snapshot: Option<Snapshot>,
    fetch_delete: bool,
    debug: bool,
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
    pub fn debug(&self) -> bool {
        self.debug
    }
}

impl GetOption {
    pub fn with_snapshot<S: Into<Snapshot>>(snapshot: S) -> Self {
        Self {
            must_fetch_value: false,
            snapshot: Some(snapshot.into()),
            fetch_delete: false,
            debug: false,
        }
    }
    pub fn with_debug() -> Self {
        Self {
            debug: true,
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub struct WriteOption {
    fsync: bool,
    debug: bool,
}

impl WriteOption {
    pub fn fsync(&self) -> bool {
        self.fsync
    }
    pub fn debug(&self) -> bool {
        self.debug
    }
}

impl WriteOption {
    pub fn with_debug() -> Self {
        Self {
            debug: true,
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub struct ScanOption {}

impl ScanOption {}
