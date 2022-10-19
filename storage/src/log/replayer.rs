use std::{
    io::{BufReader, Read},
    marker::PhantomData,
};

use byteorder::{ReadBytesExt, LE};
use bytes::Buf;

use crate::util::crc_unmask;

use super::{LogEntrySerializer, LogFile, LogSegmentFlags, SEGMENT_CONTENT_SIZE};

pub struct LogReplayer<E, S> {
    current: Option<LogFile>,

    serializer: S,
    _pd: PhantomData<E>,
}

impl<E, S> LogReplayer<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new<P: Into<String>>(serializer: S, seq: u64, path: P) -> Self {
        let path: String = path.into();
        let log_file = LogFile::open(seq, &path);
        Self {
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
        let mut file_reader = BufReader::new(file);
        let mut buf = Vec::with_capacity(SEGMENT_CONTENT_SIZE);
        let mut need_end_flag = false;
        let mut done = false;

        loop {
            let crc_value = crc_unmask(file_reader.read_u32::<LE>().unwrap());
            let length = file_reader.read_u16::<LE>().unwrap();
            let flags = file_reader.read_u8().unwrap();
            let pos = buf.len();
            if flags == 0 {
                break;
            }
            if flags == LogSegmentFlags::FULL_FRAGMENT.bits {
                if need_end_flag {
                    panic!("flags check fail");
                }
                buf.resize(length as usize, 0);
                file_reader.read_exact(&mut buf).unwrap();
                done = true;
            } else {
                if flags == LogSegmentFlags::BEGIN_FRAGMENT.bits {
                    if need_end_flag {
                        panic!("flags check fail");
                    }
                    need_end_flag = true;
                    buf.resize(length as usize, 0);
                    file_reader.read_exact(&mut buf).unwrap();
                } else {
                    if !need_end_flag {
                        panic!("flags check fail");
                    }
                    if flags == LogSegmentFlags::MIDDLE_FRAGMENT.bits {
                        buf.resize(pos + length as usize, 0);
                        file_reader.read_exact(&mut buf[pos..]).unwrap();
                    } else if flags == LogSegmentFlags::LAST_FRAGMENT.bits {
                        done = true;
                        need_end_flag = false;

                        buf.resize(pos + length as usize, 0);
                        file_reader.read_exact(&mut buf[pos..]).unwrap();
                    } else {
                        break;
                    }
                }
            }
            // crc check

            let mut crc_builder = crc32fast::Hasher::new();
            crc_builder.update(&buf[pos..]);
            let calc_crc_value = crc_builder.finalize();
            if crc_value != calc_crc_value {
                panic!("crc check fail {} != {}", crc_value, calc_crc_value);
            }

            if done {
                let mut buf_reader = BufReader::new(buf.reader());
                done = false;

                if let Some(e) = self.serializer.read(&mut buf_reader) {
                    f(&mut init_state, e);
                } else {
                    break;
                }
                buf.clear();
            }
        }
        init_state
    }
}
