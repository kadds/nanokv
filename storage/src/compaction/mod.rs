pub mod major;
pub mod minor;

pub trait CompactSerializer {
    fn stop(&self);
}
