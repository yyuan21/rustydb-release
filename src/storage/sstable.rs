// SSTable: Sorted String Table
// files that contain a set of arbitrary, sorted key-value pairs
use std::io;
use std::fs;
use std::mem;
use std::str;
use std::io::{Read, Write, BufReader, BufWriter, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashMap;

use byteorder::*;

// There is a separate metadata file that keeps track of information of
// all SSTable files including the key range and 
// An SSTable file contains compressed data

// An SSTable has the following sections:
// 1) data: (key, val) pairs
// 2) index: (key, location_to_data: u32) pairs
// 3) footer: (num_entries: u32, location_to_index: u32)
// TODO: storing keys twice in both data and index seems redundant
// it's currently implemented to speed up iteration, but maybe compressed
// timeseries data can be optimized so we have both iteration speed and
// no key duplication

// -------------------- SSTableFileReader --------------------

pub struct SSTableFileReader {
    // the path to the sstable file
    path: PathBuf,
    num_entries: u32,
    index: HashMap<String, u32>,
}

// iterating over an existing SSTable file
pub struct SSTableFileIter<'a> {
    reader: BufReader<fs::File>,
    sstable: &'a SSTableFileReader,
    curr_entry: u32
}

impl<'a> SSTableFileIter<'a> {
    fn read_entry(&mut self) -> Result<(String, String), io::Error> {
        let keylen = self.reader.read_u32::<LittleEndian>()?;
        let mut keybuf = vec![0 as u8; keylen as usize];
        self.reader.read_exact(&mut keybuf)?;
        let keystr = String::from_utf8(keybuf).unwrap();

        // load the value from data section
        let vallen = self.reader.read_u32::<LittleEndian>()?;
        let mut valbuf = vec![0 as u8; vallen as usize];
        self.reader.read_exact(&mut valbuf)?;
        let valstr = String::from_utf8(valbuf).unwrap();
        Ok((keystr, valstr))
    }
}

impl<'a> Iterator for SSTableFileIter<'a> {
    type Item = (String, String);
    
    fn next(&mut self) -> Option<Self::Item> {
        // no more items
        if self.curr_entry >= self.sstable.num_entries {
            return None;
        }

        match self.read_entry() {
            Ok((key, val)) => {
                self.curr_entry += 1;
                Some((key, val))
            },
            Err(e) => None,
        }
    }
}

impl SSTableFileReader {
    pub fn open(path: &Path) -> Result<SSTableFileReader, io::Error> {
        // load the index
        let sstfile = fs::File::open(path)?;
        let mut sst_reader = BufReader::new(sstfile);

        // read the footer to locate the index section
        let footer_offset = -2 * mem::size_of::<u32>() as i64;
        sst_reader.seek(SeekFrom::End(footer_offset))?;

        let num_entries = sst_reader.read_u32::<LittleEndian>()?;
        let index_loc = sst_reader.read_u32::<LittleEndian>()?;

        // load the index section
        // note that we assume keys are distinct, but they don't necessary have to
        // we might as well just read the index section sequentially and do a binary
        // search when using "Get", then read the data section sequentially as well
        let mut sst_index = HashMap::new();
        sst_reader.seek(SeekFrom::Start(index_loc as u64))?;
        for _ in 0..num_entries {
            let keylen = sst_reader.read_u32::<LittleEndian>()? as usize;
            let mut keybuf = vec![0 as u8; keylen];
            sst_reader.read_exact(&mut keybuf)?;
            let key = String::from_utf8(keybuf).unwrap();

            let offset = sst_reader.read_u32::<LittleEndian>()?;
            sst_index.insert(key, offset);
        }

        Ok(SSTableFileReader {
            path: path.to_path_buf(),
            num_entries: num_entries,
            index: sst_index,
        })
    }
    
    pub fn iter<'a>(&'a self) -> SSTableFileIter {
        let sstfile = fs::File::open(&self.path).unwrap();
        
        SSTableFileIter::<'a> {
            reader: BufReader::new(sstfile),
            sstable: self,
            curr_entry: 0,
        }
    }

    // get an value based on a key string
    // for current design we put index inside the latter half of the SSTable file
    // consider change it to have a separate index load on LSMTree startup
    pub fn get(&mut self, key: &str) -> Result<Option<String>, io::Error> {
        // get the real offset from the index
        let val_loc = match self.index.get(key) {
            Some(loc) => *loc,
            None => return Ok(None),
        };

        // open the file and seek to the value location
        let mut sstfile = fs::File::open(&self.path)?;
        sstfile.seek(SeekFrom::Start(val_loc as u64))?;

        // skip the key
        let keylen = sstfile.read_u32::<LittleEndian>()?;
        sstfile.seek(SeekFrom::Current(keylen as i64))?;

        // load the value from data section
        let vallen = sstfile.read_u32::<LittleEndian>()?;
        let mut valbuf = vec![0 as u8; vallen as usize];
        sstfile.read_exact(&mut valbuf)?;

        let valstr = unsafe {
            str::from_utf8_unchecked(&valbuf)
        };
        
        Ok(Some(String::from(valstr)))
    }
}

// -------------------- SSTableIndexBuilder --------------------

pub struct SSTableIndexBuilder {
    writer: BufWriter<fs::File>,
    index: Vec<(String, u32)>,
    bytes_written: usize,
}

// -------------------- SSTableFileBuilder --------------------

pub struct SSTableFileBuilder {
    writer: BufWriter<fs::File>,
    index: Vec<(String, u32)>,
    bytes_written: usize,
}

impl SSTableFileBuilder {
    pub fn new(path: &Path) -> Result<SSTableFileBuilder, io::Error> {
        let sstfile = fs::File::create(path)?;

        Ok(SSTableFileBuilder {
            writer: BufWriter::new(sstfile),
            index: Vec::new(),
            bytes_written: 0,
        }) 
    }

    // call this function to write an entry to a SSTable file
    pub fn add(&mut self, key: &str, val: &str) -> Result<(), io::Error> {
        let keybytes = key.as_bytes();
        let valbytes = val.as_bytes();
        let keylen = keybytes.len();
        let vallen = valbytes.len();

        // record the tuple location (key locations)
        self.index.push((key.to_string(), self.bytes_written as u32));

        // write keylen and key
        self.writer.write_u32::<LittleEndian>(keylen as u32)?;
        self.writer.write_all(keybytes)?;
        self.bytes_written += mem::size_of::<u32>() + keylen;

        // write vallen and val
        self.writer.write_u32::<LittleEndian>(vallen as u32)?;
        self.writer.write_all(valbytes)?;
        self.bytes_written += mem::size_of::<u32>() + vallen;
        Ok(())
    }

    // this function merges another SSTable to the current file
    pub fn merge_file(&mut self, path: &Path) -> Result<(), io::Error> {
        let reader = SSTableFileReader::open(path)?;

        // insert all pairs into the current file
        for (key, val) in reader.iter() {
            self.add(key.as_str(), val.as_str())?;
        }
        Ok(())
    }

    // we finish building the SSTable file, close and commit it
    // after this, the SSTable becomes immutable
    pub fn commit(&mut self) -> Result<(), io::Error> {
        let index_loc = self.bytes_written as u32;
        for (k, v) in &self.index {
            let keybytes = k.as_bytes();
            self.writer.write_u32::<LittleEndian>(keybytes.len() as u32)?;
            self.writer.write_all(keybytes)?;
            self.writer.write_u32::<LittleEndian>(*v)?;
        }

        // write footer
        self.writer.write_u32::<LittleEndian>(self.index.len() as u32)?;
        self.writer.write_u32::<LittleEndian>(index_loc as u32)?;

        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::sstable::*;
    use tempfile::Builder;
    use rand::prelude::*;

    #[test]
    fn sstable_single_entry() {
        let mut rng = rand::thread_rng();
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let sstfname = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut writer = SSTableFileBuilder::new(&sstfname).unwrap();

        writer.add("foo", "bar").unwrap();
        writer.commit().unwrap();

        let mut reader = SSTableFileReader::open(&sstfname).unwrap();
        assert_eq!(reader.get("foo").unwrap(), Some("bar".to_string()));
    }

    #[test]
    fn sstable_multiple_entries() {
        let mut rng = rand::thread_rng();
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let sstfname = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut writer = SSTableFileBuilder::new(&sstfname).unwrap();

        writer.add("foo", "bar").unwrap();
        writer.add("zoohoo", "keefuu").unwrap();
        writer.add("meemu", "mauha").unwrap();
        writer.add("be", "p").unwrap();
        writer.commit().unwrap();

        let mut reader = SSTableFileReader::open(&sstfname).unwrap();
        assert_eq!(reader.get("foo").unwrap(), Some("bar".to_string()));
        assert_eq!(reader.get("zoohoo").unwrap(), Some("keefuu".to_string()));
        assert_eq!(reader.get("meemu").unwrap(), Some("mauha".to_string()));
        assert_eq!(reader.get("be").unwrap(), Some("p".to_string()));
    }

    #[test]
    fn sstable_random_entries() {
        let num = 100;
        let mut rng = rand::thread_rng();
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let sstfname = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut writer = SSTableFileBuilder::new(&sstfname).unwrap();

        // generate random keys and values
        let mut keys: Vec<String> = Vec::new();
        let mut vals: Vec<String> = Vec::new();

        writer.add("foo", "bar").unwrap();
        writer.add("zoohoo", "keefuu").unwrap();
        writer.add("meemu", "mauha").unwrap();
        writer.add("be", "p").unwrap();
        for _ in 0..num {
            let rkey: [char; 32] = rng.gen();
            let key: String = rkey.into_iter().collect();
            keys.push(key.clone());

            let rval: [char; 32] = rng.gen();
            let val: String = rval.into_iter().collect();
            vals.push(val.clone());
            writer.add(&key, &val).unwrap();
        }
        writer.commit().unwrap();

        // verify
        let mut reader = SSTableFileReader::open(&sstfname).unwrap();
        for i in 0..num {
            assert_eq!(reader.get(&keys[i]).unwrap(), Some(vals[i].clone()));
        }
    }

    #[test]
    fn sstable_simple_iter() {
        let mut rng = rand::thread_rng();
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let sstfname = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut writer = SSTableFileBuilder::new(&sstfname).unwrap();

        // list of pairs for testing
        let pairs = vec![("foo", "bar"), ("zoohoo", "keefuu"), ("meemu", "mauha"), ("be", "p")];
        
        for (key, val) in &pairs {
            writer.add(key, val).unwrap();
        }
        writer.commit().unwrap();

        // verify
        let reader = SSTableFileReader::open(&sstfname).unwrap();
        for (entry, record) in reader.iter().zip(pairs.iter()) {
            let (key, val) = entry;
            assert_eq!((key.as_str(), val.as_str()), *record);
        }
    }

    #[test]
    fn sstable_random_iter() {
        let num = 100;
        let mut rng = rand::thread_rng();
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let sstfname = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut writer = SSTableFileBuilder::new(&sstfname).unwrap();

        // generate random keys and values
        let mut rand_pairs: Vec<(String, String)> = Vec::new();
        for _ in 0..num {
            let rkey: [char; 32] = rng.gen();
            let key: String = rkey.into_iter().collect();
            
            let rval: [char; 32] = rng.gen();
            let val: String = rval.into_iter().collect();
            
            writer.add(&key, &val).unwrap();
            rand_pairs.push((key, val));
        }
        writer.commit().unwrap();

        // verify
        let reader = SSTableFileReader::open(&sstfname).unwrap();
        for (entry, record) in reader.iter().zip(rand_pairs.iter()) {
            assert_eq!(entry, *record);
        }
    }

    #[test]
    fn sstable_chain_two() {
        let mut rng = rand::thread_rng();

        // first SSTable file
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let sstfname1 = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut sst1 = SSTableFileBuilder::new(&sstfname1).unwrap();

        // second SSTable file
        let sstfname2 = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut sst2 = SSTableFileBuilder::new(&sstfname2).unwrap();
        
        // list of pairs for testing
        let pairs = vec![("foo", "bar"), ("zoohoo", "keefuu"), ("meemu", "mauha"), ("be", "p")];

        // sstable 1 takes the first 2
        for entry in pairs.iter().take(2) {
            let (key, val) = *entry;
            sst1.add(key, val).unwrap();
        }
        sst1.commit().unwrap();

        // sstable 2 takes the rest
        for entry in pairs.iter().skip(2) {
            let (key, val) = *entry;
            sst2.add(key, val).unwrap();
        }
        sst2.commit().unwrap();

        // merge sst1 and sst2 to a compacted new sst
        let newsstfpath = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut newsst = SSTableFileBuilder::new(&newsstfpath).unwrap();

        newsst.merge_file(&sstfname1).unwrap();
        newsst.merge_file(&sstfname2).unwrap();
        newsst.commit().unwrap();

        // verify the new sstable file is correct
        let reader = SSTableFileReader::open(&newsstfpath).unwrap();
        for (entry, record) in reader.iter().zip(pairs.iter()) {
            let (key, val) = entry;
            assert_eq!((key.as_str(), val.as_str()), *record);
        }
    }

    #[test]
    fn sstable_chain_random() {
        let num_pairs: i32 = 100;
        let num_ssts: i32 = 10;

        // how much pairs a single sstable should take
        let chunk_size = num_pairs / num_ssts;
        let mut rng = rand::thread_rng();

        // generate random keys and values
        let mut rand_pairs: Vec<(String, String)> = Vec::new();
        for _ in 0..num_pairs {
            let rkey: [char; 32] = rng.gen();
            let key: String = rkey.into_iter().collect();
            
            let rval: [char; 32] = rng.gen();
            let val: String = rval.into_iter().collect();
            rand_pairs.push((key, val));
        }

        // the final sstable file
        let sstfpath = Builder::new().prefix("rustydb_sstable_test").tempdir().unwrap();
        let newsstfpath = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
        let mut newsst = SSTableFileBuilder::new(&newsstfpath).unwrap();

        // make "num_ssts" sstable files, filled with chunks of data, then
        // merge into the final sstable file
        for chunk in rand_pairs.chunks(chunk_size as usize) {
            let sstfname = sstfpath.path().join(format!("test_{}.sst", rng.gen::<u32>()));
            let mut sst = SSTableFileBuilder::new(&sstfname).unwrap();

            // add these specific chunk of data to new sstable, then commit
            for entry in chunk {
                let (key, val) = &*entry;
                sst.add(&key, &val).unwrap();
            }
            sst.commit().unwrap();

            // merage the new sstable
            newsst.merge_file(&sstfname).unwrap();
        }
        newsst.commit().unwrap();

        // verify
        let reader = SSTableFileReader::open(&newsstfpath).unwrap();
        for (entry, record) in reader.iter().zip(rand_pairs.iter()) {
            assert_eq!(entry, *record);
        }
    }
}

