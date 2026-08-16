//! The DuckLake catalog bloom filter, ported to Rust so the read path can probe
//! it in-process.
//!
//! DuckLake (the forked extension) writes an opt-in bloom per file per column
//! into `ducklake_file_column_blooms` — an SBBF (split-block bloom filter, the
//! Apache Parquet block layout) hashed with `DuckLakeMurmur3` and stored Base64.
//! It is *not* the Parquet on-disk bloom, and it is not xxhash64, so the read
//! path cannot reuse `parquet`'s `Sbbf`. To probe it, this reproduces the writer
//! exactly: the same murmur3, the same block/mask math, the same word layout.
//!
//! Why in-process: the blob is opaque bits. Postgres has it but not the
//! algorithm; DuckDB has the algorithm but its I/O is invisible to the latency
//! shim. Probing here keeps file dismissal a free in-memory check — no per-file
//! bloom fetch, which is the whole point (see DECISIONS.md, catalog blooms).
//!
//! Ported from `ducklake/src/common/ducklake_bloom_filter.cpp` and
//! `ducklake_murmur3.hpp`. `probes_match_a_blob_the_fork_wrote` pins it against a
//! real Base64 blob the extension produced, so a drift is caught.

use base64::Engine;

/// Salt constants from the Apache Parquet Split-Block Bloom Filter spec.
const SBBF_SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424c, 0x9efc4947, 0x5c6bfb31,
];

const WORDS_PER_BLOCK: usize = 8;
const BYTES_PER_BLOCK: usize = WORDS_PER_BLOCK * 4;

/// A parsed catalog bloom for one file+column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBloom {
    /// `num_blocks * WORDS_PER_BLOCK` little-endian words.
    words: Vec<u32>,
    num_blocks: usize,
}

impl CatalogBloom {
    /// Parse the Base64 blob stored in `ducklake_file_column_blooms.bloom`.
    ///
    /// Returns `None` if the blob is malformed — the caller treats an unparseable
    /// bloom as "cannot prune" (scan the file), which is safe, never wrong.
    pub fn from_base64(b64: &str) -> Option<CatalogBloom> {
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
        if bytes.is_empty() || bytes.len() % BYTES_PER_BLOCK != 0 {
            return None;
        }
        let words = bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        Some(CatalogBloom {
            words,
            num_blocks: bytes.len() / BYTES_PER_BLOCK,
        })
    }

    /// Might this file contain `key`? `false` means definitely not (skip the
    /// file); `true` means maybe (open it).
    pub fn might_contain(&self, key: &[u8]) -> bool {
        let hash = hash64(key);
        let block = block_index(hash, self.num_blocks);
        let mask = compute_mask(hash as u32);
        for i in 0..WORDS_PER_BLOCK {
            let w = self.words[block * WORDS_PER_BLOCK + i];
            if w & mask[i] != mask[i] {
                return false;
            }
        }
        true
    }
}

/// The 64-bit hash the filter is keyed on: two murmur3 x86-32 hashes (different
/// seeds) combined, matching `DuckLakeBloomFilter::Hash`.
fn hash64(key: &[u8]) -> u64 {
    let hi = murmur3_x86_32(key, 0);
    let lo = murmur3_x86_32(key, 0x9747b28c);
    ((hi as u64) << 32) | (lo as u64)
}

/// Multiply-shift on the upper 32 bits maps the hash uniformly onto a block.
fn block_index(hash: u64, num_blocks: usize) -> usize {
    (((hash >> 32) * num_blocks as u64) >> 32) as usize
}

/// The per-block bit mask for a key (the lower 32 bits of the hash).
fn compute_mask(key: u32) -> [u32; WORDS_PER_BLOCK] {
    let mut mask = [0u32; WORDS_PER_BLOCK];
    for i in 0..WORDS_PER_BLOCK {
        let y = key.wrapping_mul(SBBF_SALT[i]);
        mask[i] = 1u32 << (y >> 27);
    }
    mask
}

/// Murmur3 x86 32-bit, matching `DuckLakeMurmur3::Hash` (Iceberg-spec
/// compatible). Returned as the `u32` bit pattern the writer combines.
fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let nblocks = data.len() / 4;
    let mut h1 = seed;

    for i in 0..nblocks {
        let mut k1 = u32::from_le_bytes([
            data[i * 4],
            data[i * 4 + 1],
            data[i * 4 + 2],
            data[i * 4 + 3],
        ]);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let tail = &data[nblocks * 4..];
    let mut k1 = 0u32;
    let rem = data.len() & 3;
    if rem == 3 {
        k1 ^= (tail[2] as u32) << 16;
    }
    if rem >= 2 {
        k1 ^= (tail[1] as u32) << 8;
    }
    if rem >= 1 {
        k1 ^= tail[0] as u32;
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u32;
    // fmix32
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85ebca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2ae35);
    h1 ^= h1 >> 16;
    h1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real Base64 blob the forked extension wrote for a `pk` bloom over the
    /// BLOB keys `k1`..`k100` (ndv 100 → 4 blocks). If the port drifts from the
    /// writer, the present keys stop being found.
    const FORK_BLOB: &str = "Nnn6cNXytKRuV6bgWYZPLIshG5s5hjmq3LyWWSp/syXHTLa/r4Y72DeHHRsUtcXOlHN+8trY8uc/prMqtLPI+71l3dz4vf1sa357MjIX1Xw4xdbev/khnOmmlunb2MPLRnbGb925l03TboOqozd8Ng9nN0/5b5KTNTZvKUwrnmc=";

    #[test]
    fn blob_parses_to_whole_blocks() {
        let b = CatalogBloom::from_base64(FORK_BLOB).unwrap();
        assert_eq!(b.num_blocks, 4);
        assert_eq!(b.words.len(), 4 * WORDS_PER_BLOCK);
    }

    #[test]
    fn probes_match_a_blob_the_fork_wrote() {
        let b = CatalogBloom::from_base64(FORK_BLOB).unwrap();

        // Every key the fork inserted must probe present — a false negative here
        // would mean the port disagrees with the writer, which would silently
        // lose data on the read path.
        for i in 1..=100 {
            let key = format!("k{i}");
            assert!(
                b.might_contain(key.as_bytes()),
                "k{i} must be present in a bloom built over k1..k100"
            );
        }
    }

    #[test]
    fn absent_keys_are_mostly_rejected() {
        let b = CatalogBloom::from_base64(FORK_BLOB).unwrap();
        // At fpp 0.01 a handful of false positives are allowed, but the filter
        // must reject the vast majority of keys it never saw. Probe 1000 clearly
        // absent keys and require most to be rejected.
        let rejected = (100_000..101_000)
            .filter(|i| !b.might_contain(format!("k{i}").as_bytes()))
            .count();
        assert!(rejected > 950, "bloom rejected only {rejected}/1000 absent keys");
    }

    #[test]
    fn a_malformed_blob_is_none_not_a_panic() {
        assert!(CatalogBloom::from_base64("not valid base64 !!!").is_none());
        assert!(CatalogBloom::from_base64("YWJj").is_none()); // 3 bytes, not a block
    }
}
