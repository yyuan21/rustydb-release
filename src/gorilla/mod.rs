pub mod bitstream;
pub mod error;
pub mod reader;
pub mod reader_mv;
pub mod writer;
pub mod writer_mv;
pub mod api;

pub use serde::{Serialize, Deserialize};
pub use bitstream::{BitReader, BitStream, BitWriter};
pub use error::Error;
pub use reader::GorillaReader;
pub use reader_mv::GorillaReaderMV;
pub use writer::GorillaWriter;
pub use writer_mv::GorillaWriterMV;

pub type GorillaDateTime = chrono::DateTime<chrono::Utc>;

lazy_static! {
  static ref BLOCK_DURATION: chrono::Duration = chrono::Duration::hours(2);
  static ref EPOCH: GorillaDateTime = {
    chrono::DateTime::<chrono::Utc>::from_utc(
      chrono::NaiveDateTime::from_timestamp(0, 0),
      chrono::Utc,
    )
  };
}

pub fn new_gorilla_date_time(n: chrono::NaiveDateTime) -> GorillaDateTime {
  chrono::DateTime::<chrono::Utc>::from_utc(n, chrono::Utc)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GorillaBlock {
  data: BitStream,
}

impl GorillaBlock {
    pub fn new(datastr: &str) -> Self {
        Self {
            data: BitStream::new(datastr),
        }
    }
    
    pub fn to_string(&self) -> String {
        self.data.to_string()
    }
}

const BLOCK_SIZE: usize = 4096;

#[derive(Clone, Copy, Debug)]
pub struct Entry {
  time: GorillaDateTime,
  value: f64,
}

#[derive(Clone, Debug)]
pub struct MVEntry {
  time: GorillaDateTime,
  values: Vec<f64>,
}

impl Entry {
  pub fn new(time: GorillaDateTime, value: f64) -> Self {
    Entry { time, value }
  }
}

impl MVEntry {
  pub fn new(time: GorillaDateTime, values: Vec<f64>) -> Self {
    MVEntry { time, values }
  }

  pub fn time(&self) -> GorillaDateTime {
      self.time
  }

  pub fn values(&self) -> Vec<f64> {
      self.values.clone()
  }

}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Zeros {
  leading: u8,
  trailing: u8,
}
