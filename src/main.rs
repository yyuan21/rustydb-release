#[macro_use] extern crate lazy_static;
extern crate bincode;

pub mod storage;
pub mod gorilla;

use std::io;
use std::fs;
use std::env;
use std::str;
use std::fs::File;
use std::io::BufRead;
use std::path::Path;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::collections::HashSet;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chrono::{Utc, TimeZone};
use byteorder::*;

use storage::lsmtree::*;
use gorilla::*;
use gorilla::api::*;

const NUM_DATALINES: usize = 500;
const STORAGE_ROOT: &'static str = "rustystore_root";

#[derive(Hash)]
struct ConstructKey {
    tagstr: String,
    metric: String,
}

#[derive(Hash)]
struct ImportKey {
    tagstr: String,
    metric: String,
    start_dt: String,
}

// compute a key's hash
fn compute_key_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

fn main() {
    let datafile = "./sample-data/data-cpu-200-host-7-day.txt";
    let rootdir = env::home_dir().unwrap().join(STORAGE_ROOT);
    fs::create_dir_all(&rootdir).unwrap();

    if Path::exists(&rootdir) {
        println!("The root directory exists");
    }

    let mut tree = LSMTree::new(&rootdir).unwrap();

    // for accumulating data points
    let mut key_entry_table: HashMap<u64, Vec<MVEntry>> = HashMap::new();

    // read the datafile line by line and parse
    let mut counter = 0;
    let mut entries: Vec<MVEntry> = Vec::new();

    let mut keyset: Vec<String> = Vec::new();

    println!("Reading data from {}", datafile);
    let start = Instant::now();

    let mut firstkey: bool = false;
    let mut firsthash = 0;
    let mut initts = 0;
    let mut finalts = 0;
    
    if let Ok(lines) = read_lines(datafile) {
        let mut prev_tag: String = "".to_string();
        for line in lines {
            if let Ok(ip) = line {
                let tokens: Vec<&str> = ip.split(',').collect();

                match tokens[0] {
                    "tags" => {
                        // tag line, save the line and continue
                        prev_tag = ip.to_owned();
                    },
                    "cpu" => {
                        // compute the ConstructKey, and check against the key table
                        let ckey = ConstructKey {
                            tagstr: prev_tag.clone(),
                            metric: tokens[0].to_string(),
                        };

                        let ckeyhash = compute_key_hash(&ckey);

                        if key_entry_table.contains_key(&ckeyhash) {
                            // add the current data points to the MVEntry array
                            let mut curr_mventries = key_entry_table.get_mut(&ckeyhash).unwrap();
                            curr_mventries.push(parse_dataline(&tokens));
                            if curr_mventries.len() >= NUM_DATALINES {
                                // have accumulated enough data
                                // 1. construct an import key {tags, metric, start_dt}
                                // 2. Use MVEntry vector to construct a GorillaBlock
                                // 3. insert {importkey, GorillaBlock} to LSMTree
                                // 4. reset the MVEntry array
                                let tagstr = prev_tag.clone();
                                let start_dt = curr_mventries[0].time();
                                let start_dt_nanots = start_dt.timestamp_nanos();

                                if firstkey {
                                    firsthash = ckeyhash;
                                    initts = start_dt_nanots;
                                    firstkey = false;
                                }

                                // convert ckeyhash to string format
                                let mut ckeybuf = Vec::new();
                                ckeybuf.write_u64::<LittleEndian>(ckeyhash);
                                let ckeybytes = unsafe {
                                    str::from_utf8_unchecked(&ckeybuf)
                                };
                                let mut ckeystr = String::from(ckeybytes);

                                // convert start timestamp to string format
                                let mut start_tsbuf = Vec::new();
                                start_tsbuf.write_u64::<LittleEndian>(start_dt_nanots as u64);
                                let start_tsbytes = unsafe {
                                    str::from_utf8_unchecked(&start_tsbuf)
                                };
                                let start_tsstr = String::from(start_tsbytes);

                                // combine construct key {tag & metric} with init timestamp
                                ckeystr.push_str(&start_tsstr);

                                let entryblk = compress_values(curr_mventries.to_vec(),
                                                               start_dt,
                                                               curr_mventries[0].values().len());
                                let entryblkstr = entryblk.to_string();
                                tree.set(&ckeystr, &entryblkstr);

                                // reset MVEntry vector for current {tags, metric}
                                key_entry_table.remove(&ckeyhash);
                            }
                        } else {
                            key_entry_table.insert(ckeyhash, Vec::new());
                            let mut curr_mventries = key_entry_table.get_mut(&ckeyhash).unwrap();
                            curr_mventries.push(parse_dataline(&tokens));
                        }
                    },
                    _ => println!("Parse error"),
                }
            }
        }
    }

    tree.flush_memtable();
    let compressed_size: f64 = tree.total_bytes_flushed() as f64;
    let duration = start.elapsed();
    println!("Data imported: {:?}", duration);
    println!("Compressed Data size: {:.2} MB", compressed_size / (1024f64 * 1024f64));

    // for key in keyset {
    //     let val = tree.get(&key).unwrap();

    //     match val {
    //         None => println!("Nothing"),
    //         Some(v) => {
    //             let gblk = GorillaBlock::new(&v);
    //             let entries = retrieve_values(gblk, 10, NUM_DATALINES);
    //             println!("Entries: {:?}", entries);
    //         },
    //     }   
    // }
}



fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn parse_dataline(tokens: &Vec<&str>) -> MVEntry {
    let dt = Utc.timestamp_nanos(tokens[1].parse::<i64>().unwrap());

    let mut values: Vec<f64> = Vec::new();
    for i in 2..tokens.len() {
        values.push(tokens[i].parse::<f64>().unwrap());
    }
    MVEntry::new(dt, values)
}
