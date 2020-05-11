// The Write Ahead Log
use std::io;
use std::fs;
use std::time::Duration;
use std::io::{Read, BufReader, Write, BufWriter};
use std::path::{Path, PathBuf};

use byteorder::*;

const WAL_FILENAME: &'static str = "rustydb.wal";

// Each WAL record has the following components:
// 1. DURATION: sec(u64) & nanos(u32)
// 2. KEY: keylen(u32) & key(bytes)
// 3. VALUE: vallen & value(bytes)

pub struct WALWriter {
    path: PathBuf,
    writer: BufWriter<fs::File>,
}

impl WALWriter {
    pub fn new(path: &Path) -> io::Result<WALWriter> {
        let walfile = fs::File::create(path.join(WAL_FILENAME))?;
        let mut writer = BufWriter::new(walfile);
        writer.flush()?;
        Ok(WALWriter {
            path: path.to_path_buf(),
            writer: writer,
        })
    }

    pub fn reset(&mut self) -> io::Result<()> {
        let walpath = &self.path.join(WAL_FILENAME);
        if Path::exists(walpath) {
            // remove the old wal file
            fs::remove_file(walpath)?;
        }
        self.writer = BufWriter::new(fs::File::create(self.path.join(WAL_FILENAME))?);
        self.writer.flush()?;
        Ok(())
    }

    pub fn add(&mut self, timestamp: &Duration, key: &str, val: &str) -> io::Result<()> {
        // write timestamp
        self.writer.write_u64::<LittleEndian>(timestamp.as_secs())?;
        self.writer.write_u32::<LittleEndian>(timestamp.subsec_nanos())?;

        // write key string
        self.writer.write_u32::<LittleEndian>(key.as_bytes().len() as u32)?;
        self.writer.write_all(key.as_bytes())?;

        // write val string
        self.writer.write_u32::<LittleEndian>(val.as_bytes().len() as u32)?;
        self.writer.write_all(val.as_bytes())?;

        // each insertion will be flushed to disk immediately
        self.writer.flush()?;
        Ok(())
    }
}

pub struct WALReader {
    reader: BufReader<fs::File>,
}

impl WALReader {
    pub fn new(root: &Path) -> io::Result<Self> {
        let walfpath = root.join(WAL_FILENAME);
        if !walfpath.exists() {
            // nothing to read
            println!("No WAL records found, proceed")
        }

        // open the WAL file for R/W and create it if it doesn't exist
        // Note: it will throw errors if "write(true)" is not specified
        // which is pretty weird
        let walfile = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(walfpath)?;
        Ok(WALReader { reader: BufReader::new(walfile) })
    }

    pub fn read_entry(&mut self) -> Result<(Duration, String, String), io::Error> {
        let secs = self.reader.read_u64::<LittleEndian>()?;
        let nsecs = self.reader.read_u32::<LittleEndian>()?;

        // read key
        let keylen = self.reader.read_u32::<LittleEndian>()?;
        let mut keybuf = vec![0 as u8; keylen as usize];
        self.reader.read_exact(&mut keybuf)?;
        let key = String::from_utf8(keybuf).unwrap();

        // read value
        let vallen = self.reader.read_u32::<LittleEndian>()?;
        let mut valbuf = vec![0 as u8; vallen as usize];
        self.reader.read_exact(&mut valbuf)?;
        let val = String::from_utf8(valbuf).unwrap();
        
        Ok((Duration::new(secs, nsecs), key, val))
    }
}

impl Iterator for WALReader {
    type Item = (Duration, String, String);

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_entry() {
            Ok((duration, key, val)) => {
                Some((duration, key, val))
            },
            Err(_e) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::wal::*;
    use std::time::SystemTime;
    use tempfile::Builder;
    use rand::prelude::*;

    #[test]
    fn wal_single_entry() {
        let walpath = Builder::new().prefix("rustydb_wal_test").tempdir().unwrap();
        let mut wal_writer = WALWriter::new(walpath.path()).unwrap();

        let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        wal_writer.add(&ts, "foo", "bar").unwrap();

        let mut wal_reader = WALReader::new(walpath.path()).unwrap();
        let entry = wal_reader.read_entry().unwrap();
        assert_eq!(entry, (ts, String::from("foo"), String::from("bar")));
    }

    #[test]
    fn wal_multiple_entries() {
        let walpath = Builder::new().prefix("rustydb_wal_test").tempdir().unwrap();
        let mut wal_writer = WALWriter::new(walpath.path()).unwrap();

        let mut timestamps: Vec<Duration> = Vec::new();
        let pairs = vec![("foo", "bar"), ("zoohoo", "keefuu"), ("meemu", "mauha"), ("be", "p")];

        for (key, val) in &pairs {
            let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
            timestamps.push(ts);
            wal_writer.add(&ts, key, val).unwrap();
        }
        
        for (entry, (timestamp, pair)) in WALReader::new(walpath.path()).unwrap()
            .zip(timestamps.iter().zip(pairs.iter()))
        {
            let (ts, key, val) = entry;
            assert_eq!((ts, (key.as_str(), val.as_str())), (*timestamp, *pair));
        }
    }

    #[test]
    fn wal_random_entries() {
        let num = 100;
        let mut rng = rand::thread_rng();
        let walpath = Builder::new().prefix("rustydb_wal_test").tempdir().unwrap();
        let mut wal_writer = WALWriter::new(walpath.path()).unwrap();

        let mut timestamps: Vec<Duration> = Vec::new();
        let mut rand_pairs: Vec<(String, String)> = Vec::new();
        for _ in 0..num {
            let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
            let rkey: [char; 32] = rng.gen();
            let key: String = rkey.into_iter().collect();
            
            let rval: [char; 32] = rng.gen();
            let val: String = rval.into_iter().collect();
            
            wal_writer.add(&ts, &key, &val).unwrap();
            timestamps.push(ts);
            rand_pairs.push((key, val));
        }

        // verify
        for (entry, (timestamp, pair)) in WALReader::new(walpath.path()).unwrap()
            .zip(timestamps.iter().zip(rand_pairs.iter()))
        {
            let (ts, key, val) = entry;
            let (pkey, pval) = pair;
            assert_eq!((ts, key.as_str(), val.as_str()), (*timestamp, pkey.as_str(), pval.as_str()));
        }
    }
}
