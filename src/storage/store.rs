// RustyStore, the public structure exposed to users
// this is part of LSMTree implementation where threads are spawned to share
// the same structure with "lsmtree.rs" and do compaction

// Example:
// let mut store = RustyStore::init();
// store.add("a", "a_val");
// assert!("a_val", store.get("a"));

use crate::storage::lsmtree::*;
use crate::storage::sstable::*;
use crate::storage::wal::*;

use std::io;
use std::thread;
use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Condvar};

// -------------------- RustyStore --------------------

// the abstraction of the whole datastore
pub struct RustyStore {
    tree: Arc<Mutex<LSMTree>>,

    // write ahead log
    wal: WALWriter,
    num_wal_entries: usize,

    // ---------- coordinate threads -----------
    // The compaction thread will wait on this cond, and when an insertion causes
    // an overflow, the main thread set the bool to True to wake up compaction thread
    need_compact_cond: Arc<(Mutex<bool>, Condvar)>,

    // when the main thread wakes up compaction thread, we set this to False and wait
    // the compaction thread will set this to True once finished
    // TODO: change this non-concurrent behavior
    compact_finish_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl RustyStore {
    pub fn new(path: &Path) -> Result<Self, io::Error> {
        let mut lsmtree = LSMTree::new(path)?;

        // on start up, we search for WAL file under storage root
        // if a WAL file present, we do the following:
        // 1. read each entry from WAL file, and re-insert them into memtable
        // 2. flush the memtable to disk as a new L0 SSTable file
        // 3. reset the WAL file
        for (_, key, val) in WALReader::new(&path)? {
            lsmtree.set(&key, &val)?;
        }

        // flush the recovered WAL records to disk
        lsmtree.flush_memtable()?;

        // initially we don't start compact right away
        let newtree = Arc::new(Mutex::new(lsmtree));
        let need_compact = Arc::new((Mutex::new(false), Condvar::new()));
        let compact_finish = Arc::new((Mutex::new(true), Condvar::new()));

        Self::start_compaction_thread(newtree.clone(), need_compact.clone(), compact_finish.clone());

        Ok(Self {
            tree: newtree,
            wal: WALWriter::new(path)?,
            num_wal_entries: 0,
            need_compact_cond: need_compact,
            compact_finish_cond: compact_finish,
        })
    }

    fn start_compaction_thread(tree: Arc<Mutex<LSMTree>>,
                               need_compact_cond: Arc<(Mutex<bool>, Condvar)>,
                               compact_finish_cond: Arc<(Mutex<bool>, Condvar)>)
    {
        thread::spawn(move || {
            // wait condition
            let (need_compact_bool, cvar) = &*need_compact_cond;
            let mut need_compact = need_compact_bool.lock().unwrap();
            // wait until notified by the main thread
            while !*need_compact {
                need_compact = cvar.wait(need_compact).unwrap();
            }

            // flush current LSMTree's memtable to disk as SSTable files
            // lock the tree to prevent modifications
            // TODO: may only need to lock certain components of the tree
            
            println!("Compaction thread wakes up");
            let mut lsmtree = tree.lock().unwrap();
            
            // compaction finished
            println!("Compaction finished");
            let (compact_finish_bool, cvar) = &*compact_finish_cond;
            let mut compact_finished = compact_finish_bool.lock().unwrap();
            *compact_finished = true;
            // We notify the condvar that the value has changed.
            cvar.notify_one();
        });
    }

    // get a value by key
    pub fn get(&self, key: &str) -> Result<Option<String>, io::Error> {
        // TODO: the idea is to not block even if compaction is going
        self.tree.lock().unwrap().get(key)
    }

    // add a kv pair to the database
    pub fn set(&mut self, key: &str, val: &str) -> Result<(), io::Error> {
        // if inserting the pair will cause the current memtable size reaches its limit
        // then we need to:
        // 1. if the compaction thread is running, we block and wait on a condvar
        //    when the condvar is notified, we wake up and insert the pair
        //    TODO: if the compaction thread takes a long time to run, would this make
        //    "set" very slow? A possible better approach may be temporaily store the pair
        //    in a "buffered" area and return directly, then let the compaction thread
        //    to look after this buffer area as well. Subject to further thoughts.
        // 2. if the compaction thread is sleeping, we notify a condvar to wake it up
        //    then hand over the pair and let it compact altogether, this does not block.
        
        // wait compaction to finish
        let (compact_finish_bool, cvar) = &*self.compact_finish_cond;
        let mut compact_finish = compact_finish_bool.lock().unwrap();
        // wait until notified by the main thread
        while !*compact_finish {
            compact_finish = cvar.wait(compact_finish).unwrap();
        }

        // commit to to WAL
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        self.wal.add(&timestamp, key, val)?;

        // lock the tree and insert the pair
        let mut lsmtree = self.tree.lock().unwrap();
        (*lsmtree).set(key, val);
        Ok(())
    }    
}

// #[cfg(test)]
// mod tests {
//     use crate::storage::store::*;
//     use tempfile::Builder;
//     use rand::prelude::*;

//     fn start_db() -> Result<RustyStore, io::Error> {
//         let test_root = Builder::new().prefix("rustydb_temp_test").tempdir()?;
//         let store = RustyStore::new(test_root.path())?;
//         return Ok(store);
//     }

//     #[test]
//     fn simple_get_put() {
//         let mut store = start_db().unwrap();
//         store.set("foo", "bar").unwrap();
//         assert_eq!(store.get("foo").unwrap(), Some("bar".to_string()));
//     }

//     #[test]
//     fn multiple_get_put() {
//         let mut store = start_db().unwrap();

//         // multiple insertions
//         store.set("foo", "bar").unwrap();
//         store.set("zoo", "kee").unwrap();
//         store.set("hoo", "fuu").unwrap();
//         store.set("mee", "mau").unwrap();
//         store.set("bee", "puu").unwrap();

//         // multiple queries
//         assert_eq!(store.get("foo").unwrap(), Some("bar".to_string()));
//         assert_eq!(store.get("zoo").unwrap(), Some("kee".to_string()));
//         assert_eq!(store.get("hoo").unwrap(), Some("fuu".to_string()));
//         assert_eq!(store.get("mee").unwrap(), Some("mau".to_string()));
//         assert_eq!(store.get("bee").unwrap(), Some("puu".to_string()));
//     }

//     #[test]
//     fn random_get_put() {
//         let num = 100;
//         let mut store = start_db().unwrap();

//         // generate random keys and values
//         let mut rng = thread_rng();
//         let mut keys: Vec<String> = Vec::new();
//         let mut vals: Vec<String> = Vec::new();
//         for _ in 0..num {
//             let rkey: [char; 32] = rng.gen();
//             let key: String = rkey.into_iter().collect();
//             keys.push(key.clone());

//             let rval: [char; 32] = rng.gen();
//             let val: String = rval.into_iter().collect();
//             vals.push(val.clone());
//             store.set(&key, &val).unwrap();
//         }

//         // verify
//         for i in 0..num {
//             assert_eq!(store.get(&keys[i]).unwrap(), Some(vals[i].clone()));
//         }
//     }
// }
