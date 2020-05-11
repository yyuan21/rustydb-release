use bitstream_io as bit_io;
use std::io::Cursor;
use std::str;
use byteorder::*;
use crate::gorilla::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BitStream {
    n: usize,
    bitstream: Vec<u8>,
}

impl BitStream {
    pub fn to_string(&self) -> String {
        let mut buf = Vec::new();
        buf.write_u32::<LittleEndian>(self.n as u32);
        buf.extend(&self.bitstream);

        let stream = unsafe {
            str::from_utf8_unchecked(&buf)
        };
        String::from(stream)
    }

    pub fn new(block: &str) -> Self {
        // the first 4 bytes should be n
        let blockbytes = block.as_bytes();

        let (mut nbuf, streambuf) = blockbytes.split_at(4);
        let nval = nbuf.read_u32::<LittleEndian>().unwrap();

        Self {
            n: nval as usize,
            bitstream: streambuf.to_vec(),
        }
    }
}

pub struct BitWriter {
    n: usize,
    bitstream: bit_io::BitWriter<Vec<u8>, bit_io::LittleEndian>,
}

impl BitWriter {
    pub fn new() -> Self {
        BitWriter {
            n: 0,
            bitstream: bit_io::BitWriter::endian(Vec::new(), bit_io::LittleEndian),
        }
    }

    pub fn write_bit(&mut self, bit: bool) -> Result<(), Error> {
        self.bitstream.write_bit(bit)?;
        self.n += 1;
        Ok(())
    }

    pub fn write(&mut self, nbits: u32, val: u64) -> Result<(), Error> {
        let mask = {
            if nbits < 64 {
                (1 << nbits) - 1
            } else {
                std::u64::MAX
            }
        };

        self.bitstream.write(nbits, val & mask)?;
        self.n += nbits as usize;
        Ok(())
    }

    pub fn length(&self) -> usize {
        self.n
    }

    pub fn close(mut self) -> BitStream {
        let fill_bits: usize = {
            if self.n % 8 == 0 {
                0
            } else {
                ((1 + (self.n / 8)) * 8) - self.n
            }
        };
        self.bitstream.write(fill_bits as u32, 0).unwrap();
        let v = self.bitstream.into_writer();
        BitStream {
            n: self.n,
            bitstream: v,
        }
    }
}

pub struct BitReader {
    n: usize,
    c: usize,
    bitstream: bit_io::BitReader<Cursor<Vec<u8>>, bit_io::LittleEndian>,
}

impl BitReader {
    pub fn new(stream: BitStream) -> Self {
        BitReader {
            n: stream.n,
            c: 0,
            bitstream: bit_io::BitReader::endian(
                Cursor::new(stream.bitstream),
                bit_io::LittleEndian,
            ),
        }
    }

    pub fn length(&self) -> usize {
        self.n
    }

    pub fn cursor(&self) -> usize {
        self.c
    }

    pub fn read_bit(&mut self) -> Result<bool, Error> {
        if self.c <= self.n {
            let x = self.bitstream.read_bit()?;
            self.c += 1;
            Ok(x)
        } else {
            Err(Error::BitReaderError("Exceeds bitstream contents"))
        }
    }

    pub fn read(&mut self, n: usize) -> Result<u64, Error> {
        if self.c + n <= self.n {
            let x = self.bitstream.read::<u64>(n as u32)?;
            self.c += n as usize;
            Ok(x)
        } else {
            Err(Error::BitReaderError("Exceeds bitstream contents"))
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn read_write_bit() {
        let mut writer = BitWriter::new();
        assert!(writer.write_bit(true).is_ok());
        let mut reader = BitReader::new(writer.close());
        assert!(reader.read_bit().unwrap());
    }

    #[test]
    fn read_write() {
        let mut writer = BitWriter::new();
        println!("{:#b}", 0b101011i64 as u64);
        println!("{:#b}", (1 << 6) - 1);
        println!("{:#b}", 0b101011i64 as u64 & (1u64 << 6) - 1);
        assert!(writer.write(6, 0b101011).is_ok());
        let mut reader = BitReader::new(writer.close());
        assert!(reader.read(6).unwrap() == 0b101011);
    }

    #[test]
    fn read_write_mix() {
        let mut writer = BitWriter::new();
        assert!(writer.write(6, 0b101011).is_ok());
        assert!(writer.write_bit(true).is_ok());
        let mut reader = BitReader::new(writer.close());
        assert!(reader.read(6).unwrap() == 0b101011);
        assert!(reader.read_bit().unwrap());
    }

    #[test]
    fn read_write_read_bits_true() {
        let mut writer = BitWriter::new();
        assert!(writer.write(6, 0b101011).is_ok());
        assert!(writer.write_bit(true).is_ok());
        let mut reader = BitReader::new(writer.close());
        assert!(reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());

        let mut writer = BitWriter::new();
        assert!(writer.write(6, 0b101011).is_ok());
        assert!(writer.write_bit(false).is_ok());
        let mut reader = BitReader::new(writer.close());
        assert!(reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
    }

    #[test]
    fn read_write_i64() {
        let mut writer = BitWriter::new();
        let x: i64 = -60;
        assert!(writer.write(7, x as u64).is_ok());
        assert!(writer.write_bit(true).is_ok());
        let mut reader = BitReader::new(writer.close());
        assert!(!reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
    }
}
