use crate::gorilla::*;
use chrono::{Duration, TimeZone};

pub struct GorillaReaderMV {
  dim: usize,
  entry: MVEntry,
  prev_entry: MVEntry,
  prev_diff: Duration,
  prev_zeros: Vec<Zeros>,
  reader: BitReader,
}

impl GorillaReaderMV {
  pub fn from_writer(writer: GorillaWriterMV) -> Self {
    let dim = writer.dim();
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

    let mut values: Vec<f64> = Vec::new();
    for _i in 0..dim {
      values.push(f64::from_le_bytes(reader.read(64).unwrap().to_le_bytes()))
    }

    let prev_entry = MVEntry {
      time: header,
      values: vec![0.0; dim],
    };

    GorillaReaderMV {
      dim: dim,
      entry: MVEntry { time, values },
      prev_entry,
      prev_diff: Duration::seconds(0),
      prev_zeros: vec![
        Zeros {
          leading: 32,
          trailing: 32,
        };
        dim
      ],
      reader,
    }
  }

  pub fn from_block(block: GorillaBlock, dim: usize) -> Self {
      let mut reader = BitReader::new(block.data);

      let header = {
        let ts = Duration::seconds(reader.read(64).unwrap() as i64);
        chrono::Utc.ymd(1970, 1, 1).and_hms(0, 0, 0) + ts
      };

/*
      let time = {
        // always positive diff so should be OK to cast to i64 w/o masking
        let diff = Duration::seconds(reader.read(14).unwrap() as i64);
        header + diff
      };


      let mut values: Vec<f64> = Vec::new();
      for _i in 0..dim {
        values.push(f64::from_le_bytes(reader.read(64).unwrap().to_le_bytes()));
        println!("Read value");
      }
      */

      let prev_entry = MVEntry {
        time: header,
        values: vec![0.0; dim],
      };

      GorillaReaderMV {
        dim: dim,
        entry: MVEntry { time: header, values: vec![0.0; dim] },
        prev_entry,
        prev_diff: Duration::seconds(0),
        prev_zeros: vec![
          Zeros {
            leading: 32,
            trailing: 32,
          };
          dim
        ],
        reader,
      }
  }

  pub fn get_reader(&self) -> &BitReader {
      &self.reader
  }

  pub fn next(&mut self) -> MVEntry {
    let entry_time = self.entry.time;
    self.prev_diff = entry_time - self.prev_entry.time;
    self.prev_entry = MVEntry {
      time: entry_time,
      values: self.entry.values.clone(),
    };
    MVEntry {
      time: entry_time,
      values: self.entry.values.clone(),
    }
  }

  pub fn get_next_values(&mut self) -> Vec<f64> {
    let to_f64 = |x: u64| -> f64 { f64::from_le_bytes(x.to_le_bytes()) };
    let to_u64 = |x: f64| -> u64 { u64::from_le_bytes(x.to_le_bytes()) };

    let mut values: Vec<f64> = vec![0.0; self.dim];

    for i in 0..self.dim {
      // 0b0
      if !self.reader.read_bit().unwrap() {
        values[i] = self.prev_entry.values[i]
      }
      // 0b10
      else if !self.reader.read_bit().unwrap() {
        let Zeros { leading, trailing } = self.prev_zeros[i];
        let nbits = 64 - leading - trailing;
        let xored = self.reader.read(nbits as usize).unwrap() << trailing;
        let val = to_f64(to_u64(self.prev_entry.values[i]) ^ xored);
        self.prev_entry.values[i] = val;
        values[i] = val;
      }
      // 0b11
      else {
        let leading = self.reader.read(5).unwrap() as u8;
        let nbits = self.reader.read(6).unwrap() as u8;
        let trailing = 64 - leading - nbits;
        self.prev_zeros[i] = Zeros { leading, trailing };
        let xored = self.reader.read(nbits as usize).unwrap() << trailing;
        let val = to_f64(to_u64(self.prev_entry.values[i]) ^ xored);
        self.prev_entry.values[i] = val;
        values[i] = val
      }
    }
    values
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

  pub fn get_next_entry(&mut self) -> MVEntry {
    let time = self.get_next_time();
    let values = self.get_next_values();
    self.entry = MVEntry {
      time: time,
      values: values.clone(),
    };
    MVEntry {
      time: time,
      values: values.clone(),
    }
  }
}

#[cfg(test)]
mod test {
  use super::*;

  fn is_all_same(v1: &Vec<f64>, v2: &Vec<f64>) -> bool {
    if v1.len() != v2.len() {
      return false;
    }
    for i in 0..v1.len() {
      if v1[i] != v2[i] {
        return false;
      }
    }
    true
  }

  fn setup_writer() -> GorillaWriterMV {
    let mut block = GorillaWriterMV::with_vec(*EPOCH, 10);

    // make first delta 50 minutes (delta of 3000 seconds)
    let ts = *EPOCH + Duration::minutes(50);
    let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    let entry = MVEntry::new(ts, values);

    // append first entry
    assert!(block.append_first(entry).is_ok());
    block
  }

  #[test]
  pub fn get_first() {
    let mut reader = GorillaReaderMV::from_writer(setup_writer());
    let exp = MVEntry {
      time: *EPOCH + Duration::minutes(50),
      values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };

    let res = reader.next();

    assert!(exp.time == res.time);
    assert!(exp.values.len() == res.values.len());
    assert!(is_all_same(&exp.values, &res.values))
  }
  #[test]
  pub fn get_time_zero() {
    let mut writer = setup_writer();
    let exp = MVEntry {
      time: *EPOCH + Duration::minutes(100),
      values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };
    assert!(writer.append_entry(exp).is_ok());
    let mut reader = GorillaReaderMV::from_writer(writer);
    assert!(reader.next().time == *EPOCH + Duration::minutes(50));
    assert!(reader.get_next_time() == *EPOCH + Duration::minutes(100));
  }

  #[test]
  pub fn get_time() {
    let setup = |dur: i64| -> GorillaReaderMV {
      let mut writer = setup_writer();
      let exp = MVEntry {
        time: *EPOCH + Duration::minutes(50) + Duration::seconds(dur),
        values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
      };
      assert!(writer.append_entry(exp).is_ok());
      let mut reader = GorillaReaderMV::from_writer(writer);
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
      let exp = MVEntry {
        time: *EPOCH + Duration::minutes(50) + Duration::seconds(2937),
        values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
      };
      let time = exp.time;
      assert!(writer.append_entry(exp).is_ok());
      let mut reader = GorillaReaderMV::from_writer(writer);
      let first_entry = reader.next();
      assert!(first_entry.time == *EPOCH + Duration::minutes(50));
      assert!(is_all_same(
        &first_entry.values,
        &vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
      ));
      // next time and next value are the thing we appended
      assert!(reader.get_next_time() == time);
      assert!(is_all_same(
        &reader.get_next_values(),
        &vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
      ));
    }

    // try when the values are all over the place
    {
      let mut writer = setup_writer();
      assert!(writer
        .append_values(vec![
          24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0
        ])
        .is_ok());
      assert!(writer
        .append_values(vec![
          15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0
        ])
        .is_ok());
      assert!(writer
        .append_values(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        .is_ok());

      let mut reader = GorillaReaderMV::from_writer(writer);
      let first_entry = reader.next();
      assert!(first_entry.time == *EPOCH + Duration::minutes(50));
      assert!(is_all_same(
        &first_entry.values,
        &vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
      ));
      assert!(is_all_same(
        &reader.get_next_values(),
        &vec![24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0]
      ));
      assert!(is_all_same(
        &reader.get_next_values(),
        &vec![15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
      ));
      assert!(is_all_same(
        &first_entry.values,
        &vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
      ));
    }
  }

  #[test]
  pub fn get_entry() {
    let mut writer = setup_writer();
    let exp1 = MVEntry {
      time: *EPOCH + Duration::minutes(50) + Duration::seconds(2937),
      values: vec![4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0],
    };
    let time1 = exp1.time;
    let values1 = exp1.values.clone();
    let exp2 = MVEntry {
      time: *EPOCH + Duration::minutes(50) + Duration::seconds(2937),
      values: vec![14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0],
    };
    let time2 = exp2.time;
    let values2 = exp2.values.clone();
    writer.append_entry(exp1);
    writer.append_entry(exp2);
    let mut reader = GorillaReaderMV::from_writer(writer);
    let first_entry = reader.next();
    assert!(first_entry.time == *EPOCH + Duration::minutes(50));
    assert!(is_all_same(
      &first_entry.values,
      &vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    ));
    let entry1 = reader.get_next_entry();
    assert!(entry1.time == time1);
    assert!(is_all_same(&entry1.values, &values1));
    let entry2 = reader.get_next_entry();
    assert!(entry2.time == time2);
    assert!(is_all_same(&entry2.values, &values2));

  }
}
