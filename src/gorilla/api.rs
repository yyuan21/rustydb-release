use crate::gorilla::*;
use chrono::{Duration, TimeZone, NaiveDate};

pub fn compress_values(mv_entries: Vec<MVEntry>, header: GorillaDateTime, dim: usize) -> GorillaBlock {
    let mut writer = GorillaWriterMV::with_vec(header, dim);

    for i in 0..mv_entries.len() {
        assert!(writer.append_entry(mv_entries[i].clone()).is_ok());
    }
    writer.close()
}

pub fn retrieve_values(block: GorillaBlock, dim: usize, num_entries: usize) -> Vec<MVEntry> {
    let mut reader = GorillaReaderMV::from_block(block, dim);
    let mut result = Vec::new();
    for i in 0..num_entries {
        let ts = reader.get_next_time();
        let values = reader.get_next_values();
        result.push(MVEntry{time: ts, values: values.clone()});
    }
    result
}

#[cfg(test)]

mod test {
  use super::*;
  use std::collections::HashMap;
  use serde::{Serialize, Deserialize};

  fn dt(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> GorillaDateTime {
      let n = NaiveDate::from_ymd(y, m, d).and_hms(h, min, s);
      new_gorilla_date_time(n)
  }

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

  #[test]
  pub fn simple_compress_get() {
      let mut vec: Vec<MVEntry> = Vec::new();
      let vec1 = vec![1.0,2.0,3.0,4.0,5.0];
      vec.push(MVEntry::new(dt(1970, 1, 1, 0, 24, 0), vec1.clone()));
      let vec2 = vec![13.0,12.0,35.0,47.0,35.0];
      vec.push(MVEntry::new(dt(1970, 1, 1, 0, 52, 0), vec2.clone()));
      let block = compress_values(vec, dt(1970, 1, 1, 0, 0, 0), 5);
      let read_entry = retrieve_values(block, 5, 2);
      assert!(is_all_same(&vec1, &read_entry[0].values));
      assert!(is_all_same(&vec2, &read_entry[1].values));
  }

  #[test]
  pub fn complex_compress_get() {
      let mut vec: Vec<MVEntry> = Vec::new();
      let mut hash: HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
      let vec1 = vec![1.0,2.0,3.0,4.0,5.0, 6.0,7.0, 8.0,9.0, 10.0];
      vec.push(MVEntry::new(dt(1970, 1, 1, 0, 24, 0), vec1.clone()));
      let vec2 = vec![13.0,12.0,35.0,47.0,35.0, 42.6, 12.6, 42.3, 86.5,14.2];
      vec.push(MVEntry::new(dt(1970, 1, 1, 0, 52, 0), vec2.clone()));
      let vec3 = vec![132.0,121.0,335.0,347.0,375.0, 424.6, 172.6, 412.3, 836.5,184.2];
      vec.push(MVEntry::new(dt(1970, 1, 1, 1, 15, 0), vec3.clone()));
      let block = compress_values(vec, dt(1970, 1, 1, 0, 0, 0), 10);
      let ser_block = bincode::serialize(&block).unwrap();
      hash.insert(ser_block.clone(), 3);
      let read_entry = retrieve_values(block, 10, *hash.get(&ser_block).unwrap());
      assert!(is_all_same(&vec1, &read_entry[0].values));
      assert!(is_all_same(&vec2, &read_entry[1].values));
      assert!(is_all_same(&vec3, &read_entry[2].values));
  }
}
