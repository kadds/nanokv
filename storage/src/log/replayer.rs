use std::{
    io::{BufReader, Read},
    marker::PhantomData,
};

use byteorder::{ByteOrder, LE};

use super::{LogEntrySerializer, LogFile};

pub struct LogReplayer<E, S> {
    current: Option<LogFile>,
    path: String,
    seq: u64,

    serializer: S,
    _pd: PhantomData<E>,
}

impl<E, S> LogReplayer<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new<P: Into<String>>(serializer: S, seq: u64, path: P) -> Self {
        let path = path.into();
        let log_file = LogFile::open(seq, &path);
        Self {
            seq,
            path,
            current: log_file,

            serializer,
            _pd: PhantomData::default(),
        }
    }
    pub fn execute<F, ST>(&mut self, mut init_state: ST, mut f: F) -> ST
    where
        F: FnMut(&mut ST, E),
    {
        let file = match &mut self.current {
            Some(file) => file.file(),
            None => return init_state,
        };
        let mut r = BufReader::new(file);
        loop {
            if let Some(e) = self.serializer.read(&mut r) {
                f(&mut init_state, e);
            } else {
                break;
            }
        }
        init_state
    }
}

pub struct LogScanner {
    path: String,
    begin_seq: u64,
}

impl LogScanner {}
