use crate::gorilla::*;

pub struct GorillaWriter {
    header: GorillaDateTime,
    prev_ts: GorillaDateTime,
    prev_delta: u32,
    prev_value: f64,
    prev_zeros: Zeros,
    pub body: BitWriter,
}

impl GorillaWriter {

    pub fn with_vec(header: GorillaDateTime)-> Self {

        // initialize to have no leading or trailing zeros
        let prev_zeros = Zeros{ leading: 32u8, trailing: 32u8 };

        let mut block = GorillaWriter {
            header,
            prev_ts: header,
            prev_delta: 0,
            prev_value: 0.0,
            prev_zeros,
            body: BitWriter::new(),
        };

        let timestamp = header.timestamp();
        block.body.write(64, timestamp as u64).unwrap();
        block
    }

    pub fn close(self) -> GorillaBlock {
        GorillaBlock {
            data: self.body.close()
        }
    }

    fn validate_timestamp(&self, time: GorillaDateTime) -> Result<u32, Error> {

        let delta = (time - self.prev_ts).num_seconds();

        if delta < 0 {
            Err(Error::AppendOrderError)
        }

        // Can't append more than 14 bits
        else if delta > 16384 {
            Err(Error::AppendDurationError)
        }

        else {
            Ok(delta as u32)
        }

    }

    pub fn append_first(&mut self, entry: Entry) -> Result<(), Error> {
        let delta = self.validate_timestamp(entry.time)?;
        let val = u64::from_le_bytes(entry.value.to_le_bytes());
        self.body.write(14, delta as u64)?;
        self.body.write(64, val)?;
        self.prev_value = entry.value;
        self.prev_ts = entry.time;
        self.prev_delta = delta;
        Ok(())
    }

    pub fn append_entry(&mut self, entry: Entry) -> Result<(), Error> {
        // Arguably, this should be an atomic operation
        self.append_time(entry.time)?;
        self.append_value(entry.value)?;
        Ok(())
    }

    pub fn append_value(&mut self, value: f64) -> Result<(), Error> {

        let u64bytes = | v: f64 | -> u64 {
            u64::from_le_bytes(v.to_le_bytes())
        };

        let xor_f64 = | l: f64, r: f64 | -> u64 {
            u64bytes(l) ^ u64bytes(r)
        };

        //let l = u64bytes(value);
        //let r = u64bytes(self.prev_value);

        let xored = xor_f64(value, self.prev_value);

        let (inside_block, leading, trailing) = {
            let mut leading = xored.leading_zeros() as u8;
            let mut trailing = xored.trailing_zeros() as u8;
            let inside = leading >= self.prev_zeros.leading &&
                         trailing >= self.prev_zeros.trailing;
            if inside {
                leading = self.prev_zeros.leading;
                trailing = self.prev_zeros.trailing;
            }

            (inside, leading, trailing)
        };

        let nbits = 64 - leading - trailing;
        let to_write = xored >> trailing;

        if xored == 0 {
            self.body.write_bit(false)?;
        }

        else if inside_block {
            self.body.write_bit(true)?;
            self.body.write_bit(false)?;
            self.body.write(nbits as u32, to_write)?;
        }

        else {
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write(5, leading as u64)?;
            self.body.write(6, nbits as u64)?;
            self.body.write(nbits as u32, to_write)?;
            self.prev_zeros = Zeros {leading, trailing};
        }

        self.prev_value = value;

        Ok(())
    }

    pub fn append_time(&mut self, time: GorillaDateTime) -> Result<(), Error>{

        let delta = self.validate_timestamp(time)?;
        let delta_of_delta = delta as i32 - self.prev_delta as i32;
        self.prev_delta = delta;
        self.prev_ts = time;

        if delta_of_delta == 0 {
            self.body.write_bit(false)?;
        }

        else if delta_of_delta >= -63 && delta_of_delta <= 64 {
            self.body.write_bit(true)?;
            self.body.write_bit(false)?;
            self.body.write(7, delta_of_delta as u64)?;
        }

        else if delta_of_delta >= -255 && delta_of_delta <= 256 {
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write_bit(false)?;
            self.body.write(9, delta_of_delta as u64)?;
        }

        else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write_bit(false)?;
            self.body.write(12, delta_of_delta as u64)?;
        }

        else {
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write_bit(true)?;
            self.body.write(32, delta_of_delta as u64)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #![allow(unused_imports)]
    use super::*;
    use chrono::{DateTime, NaiveDate, Duration};
    use std::io::{Read, Cursor};

    fn dt(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> GorillaDateTime {
        let n = NaiveDate::from_ymd(y, m, d).and_hms(h, min, s);
        new_gorilla_date_time(n)
    }

    fn epoch() -> GorillaDateTime {
        dt(1970, 1, 1, 0, 0, 0)
    }

    fn reader(x: BitStream) -> BitReader {
        BitReader::new(x)
    }

    #[test]
    fn initialize() {
        let x = epoch() + Duration::days(1);
        let block = GorillaWriter::with_vec(x).close().data;
        let mut reader = BitReader::new(block);
        assert!(x.timestamp() == reader.read(64).unwrap() as i64);
    }

    #[test]
    fn append_first() {
        let x = epoch();
        let mut block = GorillaWriter::with_vec(x);
        let entry = {
            let ts = x - Duration::days(-1);
            let value = 1.01;
            Entry::new(ts, value)
        };
        assert!(block.append_first(entry).is_err());

        let entry = {
            let ts = x + Duration::hours(2) + Duration::seconds(1);
            let value = 1.01;
            Entry::new(ts, value)
        };
        assert!(block.append_first(entry).is_ok());
        let block = block.close().data;
        let mut reader = BitReader::new(block);
        assert!(reader.read(64).unwrap() as i64 == x.timestamp());
        let r = Duration::seconds(reader.read(14).unwrap() as i64);
        assert!(r == Duration::hours(2) + Duration::seconds(1));
        let x = f64::from_le_bytes(
            reader.read(64).unwrap().to_le_bytes()
        );
        assert!(x == 1.01);
    }

    #[test]
    fn append_valid_time() {

        let setup = |dur: Duration| -> BitReader {
            let x = epoch();
            let mut block = GorillaWriter::with_vec(x);

            // make first delta 50 minutes (delta of 3000 seconds)
            let ts = x + Duration::minutes(50);
            let value = 1.01;
            let entry = Entry::new(ts, value);

            // append first entry
            assert!(block.append_first(entry).is_ok());

            // append new time
            assert!(block.append_time(ts + dur).is_ok());

            // close and advance reader cursor
            let block = block.close().data;
            let mut reader = BitReader::new(block);
            reader.read(64).unwrap(); // read header
            reader.read(14).unwrap(); // read first timestamp
            reader.read(64).unwrap(); // read first value
            reader
        };

        // zero delta of delta
        {
            let mut reader = setup(Duration::minutes(50));
            assert!(reader.read(1).unwrap() == 0);
        }

        // -63 and 64 delta of delta
        {
            let post = |x: u8| -> i8 {
                if x > 64 {
                    (x | std::u8::MAX << 7) as i8
                } else {
                    x as i8
                }
            };

            // -63
            let mut reader = setup(Duration::seconds(2937));
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            let r = reader.read(7).unwrap() as u8;
            assert!(post(r) == -63);

            // -64
            let mut reader = setup(Duration::seconds(3064));
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            let r = reader.read(7).unwrap() as u8;
            assert!(post(r) == 64);
        }

        // -255 and 256 delta of delta
        {
            let post = |x: u16| -> i16 {
                if x > 256 {
                    (x | (std::u16::MAX << 9)) as i16
                } else {
                    x as i16
                }
            };

            // -255
            let mut reader = setup(Duration::seconds(2745));
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            let r = reader.read(9).unwrap() as u16;
            assert!(post(r) == -255);

            // 256
            let mut reader = setup(Duration::seconds(3256));
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            let r = reader.read(9).unwrap() as u16;
            assert!(post(r) == 256);
        }

        // -2047 and 2048 delta of delta
        {
            let post = |x: u16| -> i16 {
                if x > 2048 {
                    (x | (std::u16::MAX << 12)) as i16
                } else {
                    x as i16
                }
            };

            let mut reader = setup(Duration::seconds(953));
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            let r = reader.read(12).unwrap() as u16;
            assert!(post(r) == -2047);

            let mut reader = setup(Duration::seconds(5048));
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            let r = reader.read(12).unwrap() as u16;
            assert!(post(r) == 2048);
        }

        {
            let post = |x: u64| -> i32 {
                if x > 2048 {
                    (x | (std::u64::MAX << 32)) as i32
                } else {
                    x as i32
                }
            };

            let mut reader = setup(Duration::seconds(952));
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            let r = reader.read(32).unwrap();
            assert!(post(r) == -2048);

            let mut reader = setup(Duration::seconds(5049));
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            let r = reader.read(32).unwrap();
            assert!(post(r) == 2049);
        }
    }

    #[test]
    fn append_value() {

        let setup_writer = || -> GorillaWriter {
            let x = epoch();
            let mut block = GorillaWriter::with_vec(x);

            // make first delta 50 minutes (delta of 3000 seconds)
            let ts = x + Duration::minutes(50);
            let value: f64 = 12.0;
            let entry = Entry::new(ts, value);

            // append first entry
            assert!(block.append_first(entry).is_ok());
            block
        };

        let consume_first = |block: GorillaWriter| -> BitReader {
            let block = block.close().data;
            let mut reader = BitReader::new(block);
            reader.read(64).unwrap(); // read header
            reader.read(14).unwrap(); // read first timestamp
            reader.read(64).unwrap(); // read first value
            reader
        };

        let setup = |val: f64| -> BitReader {
            let mut block = setup_writer();
            // append new time
            assert!(block.append_value(val).is_ok());

            // close and advance reader cursor
            consume_first(block)
        };

        {
            let mut reader = setup(12.0);
            assert!(!reader.read_bit().unwrap());
        }

        {
            let mut writer = setup_writer();
            assert!(writer.append_value(24.0).is_ok());
            assert!(writer.append_value(15.0).is_ok());
            assert!(writer.append_value(12.0).is_ok());
            let mut reader = consume_first(writer);

            // validate write of 24
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read(5).unwrap() == 11);
            assert!(reader.read(6).unwrap() == 1);
            assert!(reader.read(1).unwrap() == 1);

            // validate write of 15
            assert!(reader.read_bit().unwrap());
            assert!(reader.read_bit().unwrap());
            assert!(reader.read(5).unwrap() == 11);
            assert!(reader.read(6).unwrap() == 4);
            assert!(reader.read(4).unwrap() == 11);

            // validate write of 12
            assert!(reader.read_bit().unwrap());
            assert!(!reader.read_bit().unwrap());
            assert!(reader.read(4).unwrap() == 3);
        }
    }
}
