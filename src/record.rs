//! The record model: `(pk, lsn, op, value)`.
//!
//! This is the unit that flows through every layer — WAL frame, memtable entry,
//! and Parquet row. Deletes are tombstones, never removals: a `Delete` record
//! must be stored, or a point read falls through to an older Parquet file and
//! resurrects the value it was supposed to erase.

use bytes::Bytes;

use crate::error::{Error, Result};

pub type Lsn = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Put = 0,
    Delete = 1,
}

impl Op {
    fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Op::Put),
            1 => Some(Op::Delete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Bytes,
    pub lsn: Lsn,
    pub op: Op,
    pub value: Bytes,
}

impl Record {
    pub fn put(key: impl Into<Bytes>, lsn: Lsn, value: impl Into<Bytes>) -> Self {
        Record {
            key: key.into(),
            lsn,
            op: Op::Put,
            value: value.into(),
        }
    }

    pub fn delete(key: impl Into<Bytes>, lsn: Lsn) -> Self {
        Record {
            key: key.into(),
            lsn,
            op: Op::Delete,
            value: Bytes::new(),
        }
    }

    /// Bytes this record occupies in a memtable, counting the entry overhead.
    /// Deliberately an overestimate — see `Memtable::approx_bytes`.
    pub fn heap_size(&self) -> usize {
        self.key.len() + self.value.len() + ENTRY_OVERHEAD
    }
}

/// Rough per-entry cost of the skiplist node plus `Entry` fields. Not exact,
/// and not meant to be.
const ENTRY_OVERHEAD: usize = 64;

// ---- WAL framing ----------------------------------------------------------
//
// | crc32 (u32 LE) | len (u32 LE) | payload[len] |
//
// payload:
// | lsn (u64 LE) | op (u8) | key_len (u32 LE) | key | val_len (u32 LE) | value |
//
// The crc covers the payload only. A torn tail — short read, bad length, or bad
// crc — ends replay at that offset rather than failing: a crash mid-append is
// expected, and the records before the tear are still good.

pub const FRAME_HEADER_LEN: usize = 8;

pub fn encode(rec: &Record, out: &mut Vec<u8>) {
    let payload_start = out.len() + FRAME_HEADER_LEN;
    out.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);

    out.extend_from_slice(&rec.lsn.to_le_bytes());
    out.push(rec.op as u8);
    out.extend_from_slice(&(rec.key.len() as u32).to_le_bytes());
    out.extend_from_slice(&rec.key);
    out.extend_from_slice(&(rec.value.len() as u32).to_le_bytes());
    out.extend_from_slice(&rec.value);

    let payload_len = out.len() - payload_start;
    let crc = crc32fast::hash(&out[payload_start..]);
    out[payload_start - FRAME_HEADER_LEN..payload_start - 4].copy_from_slice(&crc.to_le_bytes());
    out[payload_start - 4..payload_start].copy_from_slice(&(payload_len as u32).to_le_bytes());
}

/// Decode one frame from `buf`. Returns the record and the number of bytes
/// consumed, or `None` if `buf` holds a torn tail.
pub fn decode(buf: &[u8], segment: &str, offset: u64) -> Result<Option<(Record, usize)>> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;

    let end = FRAME_HEADER_LEN + len;
    if buf.len() < end {
        return Ok(None);
    }
    let payload = &buf[FRAME_HEADER_LEN..end];
    if crc32fast::hash(payload) != crc {
        return Ok(None);
    }

    let corrupt = |reason| Error::CorruptWal {
        segment: segment.to_string(),
        offset,
        reason,
    };

    // Past the crc check the payload is intact, so anything malformed from here
    // is a bug or a deliberately mangled file, not a torn write.
    if payload.len() < 13 {
        return Err(corrupt("payload shorter than fixed header"));
    }
    let lsn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let op = Op::from_u8(payload[8]).ok_or_else(|| corrupt("unknown op"))?;

    let key_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;
    let key_end = 13 + key_len;
    if payload.len() < key_end + 4 {
        return Err(corrupt("key length overruns payload"));
    }
    let key = Bytes::copy_from_slice(&payload[13..key_end]);

    let val_len =
        u32::from_le_bytes(payload[key_end..key_end + 4].try_into().unwrap()) as usize;
    let val_end = key_end + 4 + val_len;
    if payload.len() < val_end {
        return Err(corrupt("value length overruns payload"));
    }
    let value = Bytes::copy_from_slice(&payload[key_end + 4..val_end]);

    Ok(Some((
        Record {
            key,
            lsn,
            op,
            value,
        },
        end,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let recs = vec![
            Record::put(&b"alpha"[..], 1, &b"one"[..]),
            Record::delete(&b"beta"[..], 2),
            Record::put(&b""[..], 3, &b""[..]),
        ];
        let mut buf = Vec::new();
        for r in &recs {
            encode(r, &mut buf);
        }

        let mut out = Vec::new();
        let mut off = 0;
        while let Some((rec, n)) = decode(&buf[off..], "test", off as u64).unwrap() {
            out.push(rec);
            off += n;
        }
        assert_eq!(off, buf.len());
        assert_eq!(recs, out);
    }

    #[test]
    fn torn_tail_is_not_an_error() {
        let mut buf = Vec::new();
        encode(&Record::put(&b"k"[..], 1, &b"v"[..]), &mut buf);
        buf.truncate(buf.len() - 1);
        assert!(decode(&buf, "test", 0).unwrap().is_none());
    }

    #[test]
    fn bit_flip_in_payload_is_a_torn_tail_not_a_bad_record() {
        let mut buf = Vec::new();
        encode(&Record::put(&b"k"[..], 1, &b"v"[..]), &mut buf);
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        assert!(decode(&buf, "test", 0).unwrap().is_none());
    }
}
