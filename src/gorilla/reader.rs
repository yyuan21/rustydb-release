use crate::gorilla::*;
use chrono::{Duration, TimeZone};

pub struct GorillaReader {
  entry: Entry,
  prev_entry: Entry,
  prev_diff: Duration,
  prev_zeros: Zeros,
  reader: BitReader,
}

impl GorillaReader {
  fn from_writer(writer: GorillaWriter) -> Self {
    let block = writer.close();
    let mut reader = BitReader::new(block.data);

    let header = {
      let ts = Duration::seconds(reader.read(64).unwrap() as i64);
      chrono::Utc.ymd(1970, 1, 1).and_hms(0, 0, 0) + ts
    };

    let time = {
      // always positive diff so should be OK to cast to i64 w/o masking
      let diff = Duration::seconds(reader.read(14).unwrap() as i64);
      header + diff
    };

    let value = f64::from_le_bytes(reader.read(64).unwrap().to_le_bytes());

    let prev_entry = Entry {
      time: header,
      value: 0.0,
    };

    GorillaReader {
      entry: Entry { time, value },
      prev_entry,
      prev_diff: Duration::seconds(0),
      prev_zeros: Zeros {
        leading: 32,
        trailing: 32,
      },
      reader,
    }
  }

  pub fn next(&mut self) -> Entry {
    let entry = self.entry;
    self.prev_diff = entry.time - self.prev_entry.time;
    self.prev_entry = entry;
    entry
  }

  pub fn get_next_value(&mut self) -> f64 {
    let to_f64 = |x: u64| -> f64 { f64::from_le_bytes(x.to_le_bytes()) };
    let to_u64 = |x: f64| -> u64 { u64::from_le_bytes(x.to_le_bytes()) };

    // 0b0
    if !self.reader.read_bit().unwrap() {
      self.prev_entry.value
    }
    // 0b10
    else if !self.reader.read_bit().unwrap() {
      let Zeros { leading, trailing } = self.prev_zeros;
      let nbits = 64 - leading - trailing;
      let xored = self.reader.read(nbits as usize).unwrap() << trailing;
      let val = to_f64(to_u64(self.prev_entry.value) ^ xored);
      self.prev_entry.value = val;
      val
    }
    // 0b11
    else {
      let leading = self.reader.read(5).unwrap() as u8;
      let nbits = self.reader.read(6).unwrap() as u8;
      let trailing = 64 - leading - nbits;
      self.prev_zeros = Zeros { leading, trailing };
      let xored = self.reader.read(nbits as usize).unwrap() << trailing;
      let val = to_f64(to_u64(self.prev_entry.value) ^ xored);
      self.prev_entry.value = val;
      val
    }
  }

  pub fn get_next_time(&mut self) -> GorillaDateTime {
    let to_dod = |x: u64, shift: u32, max: u64| -> Duration {
      let d = {
        if x > max {
          (x | std::u64::MAX << shift) as i64
        } else {
          x as i64
        }
      };
      Duration::seconds(d)
    };

    let (bits, max) = {
      if !self.reader.read_bit().unwrap() {
        return self.prev_entry.time + self.prev_diff;
      } else if !self.reader.read_bit().unwrap() {
        (7, 64)
      } else if !self.reader.read_bit().unwrap() {
        (9, 256)
      } else if !self.reader.read_bit().unwrap() {
        (12, 2048)
      } else {
        (32, std::i32::MAX as u64)
      }
    };

    let x = self.reader.read(bits).unwrap();
    let dod = to_dod(x, bits as u32, max);
    let diff = dod + self.prev_diff;
    let time = self.prev_entry.time + diff;
    self.prev_entry.time = time;
    self.prev_diff = diff;
    time
  }
}

fn compress_values(mv_entries: Vec<MVEntry>, header: GorillaDateTime, dim: usize) -> GorillaBlock {
    let mut writer = GorillaWriterMV::with_vec(header, dim);
    for i in 0..mv_entries.len() {
        assert!(writer.append_entry(mv_entries[i].clone()).is_ok());
    }
    writer.close()
}

fn retrieve_values(block: GorillaBlock, dim: usize) -> Vec<MVEntry> {
    let mut reader = GorillaReaderMV::from_block(block, dim);
    let mut result = Vec::new();
    while reader.get_reader().cursor() <= reader.get_reader().length() {
        let ts = reader.get_next_time();
        let values = reader.get_next_values();
        result.push(MVEntry{time: ts, values: values.clone()});
    }
    result
}

#[cfg(test)]
mod test {
  use super::*;

  fn setup_writer() -> GorillaWriter {
    let mut block = GorillaWriter::with_vec(*EPOCH);

    // make first delta 50 minutes (delta of 3000 seconds)
    let ts = *EPOCH + Duration::minutes(50);
    let value: f64 = 12.0;
    let entry = Entry::new(ts, value);

    // append first entry
    assert!(block.append_first(entry).is_ok());
    block
  }

  #[test]
  pub fn get_first() {
    let mut reader = GorillaReader::from_writer(setup_writer());
    let exp = Entry {
      time: *EPOCH + Duration::minutes(50),
      value: 12.0,
    };

    let res = reader.next();

    assert!(exp.time == res.time);
    assert!(exp.value == res.value);
  }

  // Tests when delta of delta is zero
  #[test]
  pub fn get_time_zero() {
    let mut writer = setup_writer();
    let exp = Entry {
      time: *EPOCH + Duration::minutes(100),
      value: 12.0,
    };
    assert!(writer.append_entry(exp).is_ok());
    let mut reader = GorillaReader::from_writer(writer);
    assert!(reader.next().time == *EPOCH + Duration::minutes(50));
    assert!(reader.get_next_time() == *EPOCH + Duration::minutes(100));
  }

  #[test]
  pub fn get_time() {
    let setup = |dur: i64| -> GorillaReader {
      let mut writer = setup_writer();
      let exp = Entry {
        time: *EPOCH + Duration::minutes(50) + Duration::seconds(dur),
        value: 12.0,
      };
      assert!(writer.append_entry(exp).is_ok());
      let mut reader = GorillaReader::from_writer(writer);
      assert!(reader.next().time == *EPOCH + Duration::minutes(50));
      reader
    };

    let exp_dt =
      |x: i64| -> GorillaDateTime { *EPOCH + Duration::minutes(50) + Duration::seconds(x) };

    // delta of delta = 0
    let mut reader = setup(3000);
    assert!(reader.get_next_time() == exp_dt(3000));

    // 7 bits
    let mut reader = setup(2937);
    assert!(reader.get_next_time() == exp_dt(2937));

    let mut reader = setup(3064);
    assert!(reader.get_next_time() == exp_dt(3064));

    // 9 bits
    let mut reader = setup(2745);
    assert!(reader.get_next_time() == exp_dt(2745));

    let mut reader = setup(3256);
    assert!(reader.get_next_time() == exp_dt(3256));

    // 12 bits
    let mut reader = setup(953);
    assert!(reader.get_next_time() == exp_dt(953));

    let mut reader = setup(5048);
    assert!(reader.get_next_time() == exp_dt(5048));

    // 32 bits
    let mut reader = setup(952);
    assert!(reader.get_next_time() == exp_dt(952));

    let mut reader = setup(5049);
    assert!(reader.get_next_time() == exp_dt(5049));
  }

  #[test]
  pub fn get_value() {
    // when the value is the same
    {
      let mut writer = setup_writer();
      let exp = Entry {
        time: *EPOCH + Duration::minutes(50) + Duration::seconds(2937),
        value: 12.0,
      };
      assert!(writer.append_entry(exp).is_ok());
      let mut reader = GorillaReader::from_writer(writer);
      let first_entry = reader.next();
      assert!(first_entry.time == *EPOCH + Duration::minutes(50));
      assert!(first_entry.value == 12.0);

      // next time and next value are the thing we appended
      assert!(reader.get_next_time() == exp.time);
      assert!(reader.get_next_value() == 12.0);
    }

    // try when the values are all over the place
    {
      let mut writer = setup_writer();
      assert!(writer.append_value(24.0).is_ok());
      assert!(writer.append_value(15.0).is_ok());
      assert!(writer.append_value(12.0).is_ok());
      let mut reader = GorillaReader::from_writer(writer);
      let first_entry = reader.next();
      assert!(first_entry.time == *EPOCH + Duration::minutes(50));
      assert!(first_entry.value == 12.0);
      assert!(reader.get_next_value() == 24.0);
      assert!(reader.get_next_value() == 15.0);
      assert!(reader.get_next_value() == 12.0);
    }
  }
}
