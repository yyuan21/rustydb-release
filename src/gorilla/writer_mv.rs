use crate::gorilla::*;

pub struct GorillaWriterMV {
  dim: usize,
  header: GorillaDateTime,
  prev_ts: GorillaDateTime,
  prev_delta: u32,
  prev_value: Vec<f64>,
  prev_zeros: Vec<Zeros>,
  pub body: BitWriter,
}

impl GorillaWriterMV {
  pub fn with_vec(header: GorillaDateTime, dim: usize) -> Self {
    // initialize to have no leading or trailing zeros
    let prev_zeros = vec![
      Zeros {
        leading: 32u8,
        trailing: 32u8,
      };
      dim
    ];

    let mut block = GorillaWriterMV {
      dim,
      header,
      prev_ts: header,
      prev_delta: 0,
      prev_value: vec![0.0; dim],
      prev_zeros,
      body: BitWriter::new(),
    };

    let timestamp = header.timestamp();
    block.body.write(64, timestamp as u64).unwrap();
    block
  }

  pub fn dim(&self) -> usize {
    self.dim
  }

  pub fn close(self) -> GorillaBlock {
    GorillaBlock {
      data: self.body.close(),
    }
  }

  fn validate_values(&self, values: &Vec<f64>) -> Result<(), Error> {
    if values.len() != self.dim {
      Err(Error::BadDimensionError)
    } else {
      Ok(())
    }
  }

  fn validate_timestamp(&self, time: GorillaDateTime) -> Result<u32, Error> {
    let delta = (time - self.prev_ts).num_seconds();

    if delta < 0 {
      //Err(Error::AppendOrderError)
      Err(Error::AppendOrderError)
    }
    // Can't append more than 14 bits
    else if delta > 16384 {
      Err(Error::AppendDurationError)
    } else {
      Ok(delta as u32)
    }
  }

  pub fn append_entry(&mut self, entry: MVEntry) -> Result<(), Error> {
    // Arguably, this should be an atomic operation
    self.validate_values(&(entry.values))?;
    self.append_time(entry.time)?;
    self.append_values(entry.values)?;
    Ok(())
  }

  pub fn append_first(&mut self, entry: MVEntry) -> Result<(), Error> {
    let delta = self.validate_timestamp(entry.time)?;
    self.body.write(14, delta as u64)?;
    let mut val;
    for i in 0..self.dim {
      val = u64::from_le_bytes(entry.values[i].to_le_bytes());
      self.body.write(64, val)?;
    }
    self.prev_value = entry.values;
    self.prev_ts = entry.time;
    self.prev_delta = delta;
    Ok(())
  }

  pub fn append_values(&mut self, values: Vec<f64>) -> Result<(), Error> {
    self.validate_values(&values)?;
    let u64bytes = |v: f64| -> u64 { u64::from_le_bytes(v.to_le_bytes()) };

    let xor_f64 = |l: f64, r: f64| -> u64 { u64bytes(l) ^ u64bytes(r) };

    //let l = u64bytes(value);
    //let r = u64bytes(self.prev_value);

    for i in 0..self.dim {
      let xored = xor_f64(values[i], self.prev_value[i]);
      let (inside_block, leading, trailing) = {
        let mut leading = xored.leading_zeros() as u8;
        let mut trailing = xored.trailing_zeros() as u8;
        let inside =
          leading >= self.prev_zeros[i].leading && trailing >= self.prev_zeros[i].trailing;
        if inside {
          leading = self.prev_zeros[i].leading;
          trailing = self.prev_zeros[i].trailing;
        }

        (inside, leading, trailing)
      };

      let nbits = 64 - leading - trailing;
      let to_write = xored >> trailing;

      if xored == 0 {
        self.body.write_bit(false)?;
      } else if inside_block {
        self.body.write_bit(true)?;
        self.body.write_bit(false)?;
        self.body.write(nbits as u32, to_write)?;
      } else {
        self.body.write_bit(true)?;
        self.body.write_bit(true)?;
        self.body.write(5, leading as u64)?;
        self.body.write(6, nbits as u64)?;
        self.body.write(nbits as u32, to_write)?;
        self.prev_zeros[i] = Zeros { leading, trailing };
      }

      self.prev_value[i] = values[i];
    }

    Ok(())
  }

  pub fn append_time(&mut self, time: GorillaDateTime) -> Result<(), Error> {
    let delta = self.validate_timestamp(time)?;
    let delta_of_delta = delta as i32 - self.prev_delta as i32;
    self.prev_delta = delta;
    self.prev_ts = time;

    if delta_of_delta == 0 {
      self.body.write_bit(false)?;
    } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
      self.body.write_bit(true)?;
      self.body.write_bit(false)?;
      self.body.write(7, delta_of_delta as u64)?;
    } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
      self.body.write_bit(true)?;
      self.body.write_bit(true)?;
      self.body.write_bit(false)?;
      self.body.write(9, delta_of_delta as u64)?;
    } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
      self.body.write_bit(true)?;
      self.body.write_bit(true)?;
      self.body.write_bit(true)?;
      self.body.write_bit(false)?;
      self.body.write(12, delta_of_delta as u64)?;
    } else {
      self.body.write_bit(true)?;
      self.body.write_bit(true)?;
      self.body.write_bit(true)?;
      self.body.write_bit(true)?;
      self.body.write(32, delta_of_delta as u64)?;
    }

    Ok(())
  }
}
