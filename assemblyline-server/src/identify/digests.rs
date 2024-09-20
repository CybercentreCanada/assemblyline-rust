use anyhow::Result;
use log::debug;

use std::io::Read;
use std::path::{Path, PathBuf};

use md5::Digest;

use crate::IBool;

use super::entropy;

pub const DEFAULT_BLOCKSIZE: usize = 65536;


pub struct Digests {

    pub md5: String,
    pub sha1: String,
    pub sha256: String,
    pub ssdeep: Option<String>,
    pub tlsh: Option<String>,
    pub size: u64,
    pub entropy: Option<f64>,
    pub first_block: Vec<u8>,
}

pub fn get_digests_for_file_blocking(path: &Path, blocksize: Option<usize>, calculate_entropy: impl IBool, skip_fuzzy_hashes: impl IBool) -> Result<Digests> {
    let blocksize = blocksize.unwrap_or(DEFAULT_BLOCKSIZE);
    let calculate_entropy: bool = calculate_entropy.into().unwrap_or(true);
    let skip_fuzzy_hashes: bool = skip_fuzzy_hashes.into().unwrap_or(false);
    let path = path.to_owned();
    debug!("get_digests {path:?}; calculate_entropy: {calculate_entropy}; skip_fuzzy_hashes: {skip_fuzzy_hashes}");

    let mut first_block = vec![];
    let mut md5 = md5::Md5::new();
    let mut sha1 = sha1::Sha1::new();
    let mut sha256 = sha2::Sha256::new();
    let mut entropy = if calculate_entropy {
        Some(entropy::BufferedCalculator::new())
    } else {
        None
    };
    let mut fuzzy_hashes = if !skip_fuzzy_hashes {
        let mut ssdeep = ssdeep::Generator::new();
        ssdeep.set_fixed_input_size(std::fs::metadata(&path)?.len())?;
        Some((tlsh2::TlshDefaultBuilder::new(), ssdeep))
    } else {
        None
    };

    let mut size = 0;
    let mut buffer = vec![0u8; blocksize];
    let mut file = std::fs::File::open(&path)?;

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break
        }

        let bytes = &buffer[0..bytes_read];

        if first_block.len() < blocksize {
            let additional = blocksize.saturating_sub(first_block.len()).min(bytes_read);
            first_block.extend_from_slice(&bytes[0..additional]);
        }
        md5.update(bytes);
        sha1.update(bytes);
        sha256.update(bytes);
        if let Some(entropy) = &mut entropy {
            entropy.update(bytes);
        }
        if let Some((tlsh, ssdeep)) = &mut fuzzy_hashes {
            tlsh.update(bytes);
            ssdeep.update(bytes);
        }
        size += bytes_read as u64;

    }

    // // invoke ssdeep on its own, could import a library that does it by chunk as well but AFAIK this
    // // library is more used
    // let ssdeep = if !skip_fuzzy_hashes {
    //     match ssdeep::hash_from_file(path) {
    //         Ok(hash) => Some(hash),
    //         Err(err) => {
    //             error!("Could not evaluate SSDEEP hash: {err}");
    //             None
    //         }
    //     }
    // } else {
    //     None
    // };

    let (tlsh, ssdeep) = match fuzzy_hashes {
        Some((tlsh, ssdeep)) => {
            let tlsh = tlsh.build().map(|hash| hex::encode(hash.hash().as_slice()));
            let ssdeep = ssdeep.finalize()?.to_string();
            (tlsh, Some(ssdeep))
        },
        None => (None, None),
    };

    debug!("entropy {:?}", entropy.as_ref().map(|calculator| calculator.entropy()));
    
    Ok(Digests {
        md5: hex::encode(md5.finalize().as_slice()),
        sha1: hex::encode(sha1.finalize().as_slice()),
        sha256: hex::encode(sha256.finalize().as_slice()),
        ssdeep,
        tlsh,
        size,
        entropy: entropy.map(|calculator| calculator.entropy()),
        first_block,
    })
}

/// Generate digests for file reading only 'blocksize bytes at a time.
pub async fn get_digests_for_file(path: PathBuf, blocksize: Option<usize>, calculate_entropy: impl IBool, skip_fuzzy_hashes: impl IBool) -> Result<Digests> {
    let calculate_entropy: bool = calculate_entropy.into().unwrap_or(true);
    let skip_fuzzy_hashes: bool = skip_fuzzy_hashes.into().unwrap_or(false);

    tokio::task::spawn_blocking(move || {
        get_digests_for_file_blocking(&path, blocksize, calculate_entropy, skip_fuzzy_hashes)
    }).await?
}

// def get_md5_for_file(path: str, blocksize: int = DEFAULT_BLOCKSIZE) -> str:
//     md5 = hashlib.md5()
//     with open(path, 'rb') as f:
//         data = f.read(blocksize)
//         length = len(data)

//         while length > 0:
//             md5.update(data)
//             data = f.read(blocksize)
//             length = len(data)

//         return md5.hexdigest()


// def get_sha256_for_file(path: str, blocksize: int = DEFAULT_BLOCKSIZE) -> str:
//     sha256 = hashlib.sha256()
//     with open(path, 'rb') as f:
//         data = f.read(blocksize)
//         length = len(data)

//         while length > 0:
//             sha256.update(data)
//             data = f.read(blocksize)
//             length = len(data)

//         return sha256.hexdigest()
