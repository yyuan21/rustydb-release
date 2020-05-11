use crate::storage::sstable::*;

use std::io;
use std::fs;
use std::mem;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;

use uuid::Uuid;
use byteorder::*;

// -------------------- Date-Tiered Compaction --------------------

// Reference: https://labs.spotify.com/2014/12/18/date-tiered-compaction
// The largest SSTable will be 4GB each

// proposed SSTable file size tier
// | Level | Size  |
// |     0 | 4MB   |
// |     1 | 16MB  |
// |     2 | 64MB  |
// |     3 | 256MB |
// |     4 | 1GB   |
// |     5 | 4GB   |

// when memtable reaches ~4MB, flush to disk as L0 SSTable, when the number of
// L0 sstables reaches fanout factor, a compaction thread is spawn to pack them into
// L1 sstables which is 16MB each, note that all sstables have disjoint time ranges
// 

// -------------------- Memtable flushing --------------------

// The following will trigger a 'Memtable -> SSTable file' flush
// 1. Memtable reaches certain threshold
// 2. WAL file reaches certain threshold
// 3. Database restart with a non-empty WAL

// -------------------- Settings --------------------

// the metadata filename
const META_FILENAME: &'static str = "rustydb.meta";

// memtable threshold in bytes (4MB)
const MEMTABLE_THRESHOLD: usize = 4 * 1024 * 1024;

const SSTABLE_FANOUT: usize = 4;

// -------------------- SSTableMeta --------------------

// contains the metainfo of a single SSTable file, the LSM Tree keeps track of
// all SSTable files using a vector of these structs.
// reconstructed on database initialization
struct SSTableMeta {
    filename: String,           // the filename of the SSTable file and index
    level: usize,               // the level of the SSTable
    min_key: String,            // the minimum key of the SSTable
    max_key: String,            // the maximum key of the SSTable
}

impl SSTableMeta {
    fn new(minkey: &str, maxkey: &str) -> Self {
        let ufname = Uuid::new_v4().to_hyphenated().to_string();
        SSTableMeta {
            filename: format!("{}.sst", ufname),
            level: 0,
            min_key: String::from(minkey),
            max_key: String::from(maxkey),
        }
    }

    fn in_range(&self, key: &str) -> bool {
        let keystr = String::from(key);
        self.min_key <= keystr && keystr <= self.max_key
    }
}

// -------------------- LSMTree --------------------

// a memtable stores both (key, val) pairs as well as the anticipated
// size if it get flushed to disk as sstable file
struct MemTable {
    map: BTreeMap<String, String>,
    flush_size: usize,
}

impl MemTable {
    fn new() -> Self {
        MemTable {
            map: BTreeMap::new(),
            flush_size: 0,
        }
    }

    fn insert(&mut self, key: &str, val: &str) {
        self.map.insert(key.to_string(), val.to_string());

        // if flushed to disk, we store the following format:
        // | keylen: u32 | key bytes | valuelen: u32 | value bytes |
        self.flush_size += 2 * mem::size_of::<u32>() + key.len() + val.len();
    }

    fn need_flush(&self, key: &str, val: &str) -> bool {
        let pairsz = 2 * mem::size_of::<u32>() + key.len() + val.len();
        self.flush_size + pairsz > MEMTABLE_THRESHOLD
    }

    fn get_minkey(&self) -> String {
        self.map.keys().next().unwrap().to_string()
    }

    fn get_maxkey(&self) -> String {
        self.map.keys().next_back().unwrap().to_string()
    }

    fn reset(&mut self) {
        self.map.clear();
        self.flush_size = 0;
    }

    fn write_entries_to_sstable(&self, sst: &mut SSTableFileBuilder) -> Result<(), io::Error> {
        for entry in &self.map {
            sst.add(&entry.0, &entry.1)?;
        }
        Ok(())
    }
}

pub struct LSMTree {
    // the base path of the lsmtree
    path: PathBuf,

    // read/write access this first, then periodically flushed
    // these can be accessed by both writer thread and compaction thread
    memtable: MemTable,

    // buffered memtable sections, use these when compaction is running
    // these will only be accessed by writer thread
    buffered_memtable: MemTable,

    // metainfo about all sstables this lsmtree is holding
    sstables: Vec<SSTableMeta>,

    total_flushed_size: usize,
}

impl LSMTree {
    // initialize a new LSMTree
    pub fn new(rootpath: &Path) -> Result<Self, io::Error> {
        let mut newtree = Self {
            path: rootpath.to_path_buf(),
            memtable: MemTable::new(),
            buffered_memtable: MemTable::new(),
            sstables: Vec::new(),
            total_flushed_size: 0,
        };

        newtree.tryload_meta()?;
        Ok(newtree)
    }

    // try to load the metadata file if exists
    fn tryload_meta(&mut self) -> Result<(), io::Error> {
        // try to reload the sstable metainfo from existing root path if any
        let metafpath = self.path.join(META_FILENAME);
        if !metafpath.exists() {
            return Ok(())
        }

        let mut metafile = fs::File::open(metafpath)?;

        // number of entries in the metadata file
        let num_sstables = metafile.read_u32::<LittleEndian>()?;

        // for each entry, allocate a new SSTableMeta struct and push to the tree
        for _ in 0..num_sstables {
            // read filename
            let sst_fname_len = metafile.read_u8()? as usize;
            let mut sst_fname_buf = vec![0 as u8; sst_fname_len];
            metafile.read_exact(&mut sst_fname_buf)?;
            let sst_fname = String::from_utf8(sst_fname_buf).unwrap();

            // read level
            let sst_level = metafile.read_u8()? as usize;

            // read min key
            let minkey_len = metafile.read_u32::<LittleEndian>()? as usize;
            let mut minkey_buf = vec![0 as u8; minkey_len];
            metafile.read_exact(&mut minkey_buf)?;
            let minkey = String::from_utf8(minkey_buf).unwrap();

            // read max key
            let maxkey_len = metafile.read_u32::<LittleEndian>()? as usize;
            let mut maxkey_buf = vec![0 as u8; maxkey_len];
            metafile.read_exact(&mut maxkey_buf)?;
            let maxkey = String::from_utf8(maxkey_buf).unwrap();

            // add to the newtree's sstable info list
            self.sstables.push(SSTableMeta {
                filename: sst_fname,
                level: sst_level,
                min_key: minkey,
                max_key: maxkey,
            });
        }
        Ok(())
    }

    // insert a (key, value) pair into the LSMTree
    // 
    // If the compaction thread is flushing memtable to sstable:
    // 1. 'memtable' is locked by compaction thread
    // 2. 'set' write the (key, val) to 'buffered_memtable' and return
    // 3. After compaction thread finish flushing, it replace the 'memtable'
    //    with 'buffered_memtable', then allocate a new 'buffered_memtable'
    // 4. If 'buffered_memtable' also reaches the threshold, then block
    pub fn set(&mut self, key: &str, val: &str) -> Result<(), io::Error> {
        // compact if this insertion causes an overflow
        if self.memtable.need_flush(key, val) {
            println!("Flushing Memtable to disk: {} bytes", self.memtable.flush_size);
            self.flush_memtable()?;
        }
        
        // all insertions go to the memtable first
        self.memtable.insert(key, val);

        // when memtable is flushed to disk as sstables, we will store:
        // 1. (key, val) pair --> len(key) + len(val)
        // 2. an index entry that locate this pair: len(key) + u32 location

        // if memtable overflows, then trigger a flush here
        // 1. pack memtable and write to a new sstable
        // 2. clear both memtable and WAL
        Ok(())
    }

    // retrieve a value by a specific key
    // try lock 'memtable' if it's locked then check 
    // 1. check the memtable first, retrieve it if present
    // 2. open each SSTable and check the min, max key range
    pub fn get(&self, key: &str) -> Result<Option<String>, io::Error> {
        // if the (k, v) is still in memory
        if let Some(s) = self.memtable.map.get(key) {
            return Ok(Some(s.to_string()));
        }

        // search SSTable files for value
        for sstable in &self.sstables {
            if sstable.in_range(key) {
                let path = self.path.join(&sstable.filename);
                let mut currsst = SSTableFileReader::open(&path)?;
                if let Some(val) = currsst.get(key)? {
                    return Ok(Some(val));
                }
            }
        }

        Ok(None)
    }

    // flush the current memtable to disk and store it as sstable files
    pub fn flush_memtable(&mut self) -> Result<(), io::Error> {
        let minkey = self.memtable.get_minkey();
        let maxkey = self.memtable.get_maxkey();
        let new_sstable = SSTableMeta::new(&minkey, &maxkey);

        let mut sst_builder = SSTableFileBuilder::new(&self.path.join(&new_sstable.filename))?;
        self.memtable.write_entries_to_sstable(&mut sst_builder)?;
        sst_builder.commit()?;

        self.sstables.push(new_sstable);
        self.flush_metadata()?;

        self.total_flushed_size += self.memtable.flush_size;
        
        // reset the current memtable
        self.memtable.reset();
        Ok(())
    }

    // write out the current LSMTree metadata to a metadata file
    pub fn flush_metadata(&mut self) -> Result<(), io::Error> {
        let mut metafile = fs::File::create(self.path.join(META_FILENAME))?;

        // record number of sstables
        metafile.write_u8(self.sstables.len() as u8)?;

        // record each SSTableMeta info
        for sstable in &self.sstables {
            // write filename
            metafile.write_u32::<LittleEndian>(sstable.filename.len() as u32)?;
            metafile.write_all(sstable.filename.as_bytes())?;

            // write level
            metafile.write_u8(sstable.level as u8)?;

            // write min key
            metafile.write_u32::<LittleEndian>(sstable.min_key.len() as u32)?;
            metafile.write_all(sstable.min_key.as_bytes())?;

            // write max key
            metafile.write_u32::<LittleEndian>(sstable.max_key.len() as u32)?;
            metafile.write_all(sstable.max_key.as_bytes())?;
        }

        // make sure all in-memory data reaches disk
        metafile.sync_all()?;
        Ok(())
    }

    pub fn total_bytes_flushed(&self) -> usize {
        self.total_flushed_size
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::lsmtree::*;
    use tempfile::Builder;
    use rand::prelude::*;

    #[test]
    fn lsmtree_single_entry() {
        let lsmpath = Builder::new().prefix("rustydb_lsmtree_test").tempdir().unwrap();
        let mut newtree = LSMTree::new(lsmpath.path()).unwrap();

        newtree.set("foo", "bar").unwrap();
        let val = newtree.get("foo").unwrap();
        assert_eq!(val, Some(String::from("bar")));
    }

    #[test]
    fn lsmtree_multiple_entries() {
        let lsmpath = Builder::new().prefix("rustydb_lsmtree_test").tempdir().unwrap();
        let mut newtree = LSMTree::new(lsmpath.path()).unwrap();

        newtree.set("foo", "bar").unwrap();
        newtree.set("zoohoo", "keefuu").unwrap();
        newtree.set("meemu", "mauha").unwrap();
        newtree.set("be", "p").unwrap();
        
        assert_eq!(newtree.get("foo").unwrap(), Some(String::from("bar")));
        assert_eq!(newtree.get("zoohoo").unwrap(), Some(String::from("keefuu")));
        assert_eq!(newtree.get("meemu").unwrap(), Some(String::from("mauha")));
        assert_eq!(newtree.get("be").unwrap(), Some(String::from("p")));
    }

    #[test]
    fn lsmtree_random_entries() {
        // number of pairs
        let num = 350;

        // value length (multiple of 32 bytes)
        let vallen = 100;
        
        let mut rng = rand::thread_rng();
        let lsmpath = Builder::new().prefix("rustydb_lsmtree_test").tempdir().unwrap();
        let mut newtree = LSMTree::new(lsmpath.path()).unwrap();

        let mut rand_pairs: Vec<(String, String)> = Vec::new();
        for i in 0..num {
            println!("Inserting entry {}", i);
            let rkey: [char; 32] = rng.gen();
            let key: String = rkey.into_iter().collect();

            let mut val: String = "".to_owned();
            for _ in 0..vallen {
                let currval: [char; 32] = rng.gen();
                let valstr: String = currval.into_iter().collect();
                val.push_str(&valstr);
            }
            
            newtree.set(&key, &val).unwrap();
            rand_pairs.push((key, val));
        }

        // verify
        for (key, val) in rand_pairs {
            assert_eq!(newtree.get(key.as_str()).unwrap(), Some(val));
        }
    }  
}
