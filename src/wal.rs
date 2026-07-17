//! Segmented write-ahead log.
//!
//! The WAL holds the LSN suffix past the published Iceberg watermark — the only
//! state in this engine that is not rebuildable from the columnar level. Two
//! properties matter:
//!
//! * **fsync before ack.** `append` returns only after `sync_data`. The engine
//!   must not insert into the memtable until this returns, or a reader can
//!   observe a value that a crash then destroys.
//! * **Batch-oriented.** `append` takes a slice and pays one fsync for the whole
//!   batch. This is the seam group commit will hang off — but nothing collects
//!   batches yet: the engine calls `append` with one record, so today the write
//!   path pays one fsync per write. Batching would change the cost, not the
//!   guarantee; every writer in a batch still waits for the shared fsync.
//!
//! Segments are named by the LSN they start at. Truncation deletes whole sealed
//! segments whose highest LSN is at or below the flush watermark, which is why
//! it is safe to run right after the Iceberg commit lands: everything in those
//! segments is already in the columnar level.
//!
//! I/O here is deliberately synchronous `std::fs`. fsync blocks regardless, and
//! sync code keeps the crash-injection points in step 5 easy to reason about.
//! Callers on an async runtime should wrap this in `spawn_blocking`.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::record::{self, Lsn, Record};

const SEGMENT_SUFFIX: &str = ".wal";

#[derive(Debug, Clone)]
struct Segment {
    path: PathBuf,
    start_lsn: Lsn,
    max_lsn: Lsn,
}

pub struct Wal {
    dir: PathBuf,
    segment_max_bytes: u64,

    writer: BufWriter<File>,
    active: Segment,
    active_bytes: u64,

    sealed: Vec<Segment>,
    scratch: Vec<u8>,
}

/// What a replay found on disk.
pub struct Replay {
    pub records: Vec<Record>,
    /// Highest LSN durably on disk, or 0 if the log is empty.
    pub max_lsn: Lsn,
}

impl Wal {
    /// Open (or create) the log in `dir`, replaying whatever is there.
    ///
    /// `from_lsn` is the flush watermark: records at or below it are already in
    /// the columnar level and are skipped. Pass 0 to replay everything.
    pub fn open(dir: impl AsRef<Path>, segment_max_bytes: u64, from_lsn: Lsn) -> Result<(Self, Replay)> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut segments = Self::list_segments(&dir)?;
        let mut records = Vec::new();
        let mut max_lsn = 0;

        for seg in &mut segments {
            let (recs, seg_max, good_bytes) = Self::replay_segment(&seg.path)?;

            // A torn tail can only be in the last segment — anything earlier was
            // sealed after a successful fsync. Truncating it back to the last
            // good frame keeps the file append-clean for the next write.
            let len = fs::metadata(&seg.path)?.len();
            if good_bytes < len {
                let f = OpenOptions::new().write(true).open(&seg.path)?;
                f.set_len(good_bytes)?;
                f.sync_all()?;
            }

            seg.max_lsn = seg_max;
            max_lsn = max_lsn.max(seg_max);
            records.extend(recs.into_iter().filter(|r| r.lsn > from_lsn));
        }

        // Reuse the newest segment as the active one so a restart appends rather
        // than orphaning a partly-filled file.
        let (active, active_bytes, sealed) = match segments.pop() {
            Some(last) => {
                let bytes = fs::metadata(&last.path)?.len();
                (last, bytes, segments)
            }
            None => {
                let seg = Segment {
                    path: dir.join(Self::segment_name(0)),
                    start_lsn: 0,
                    max_lsn: 0,
                };
                (seg, 0, Vec::new())
            }
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active.path)?;

        let wal = Wal {
            dir,
            segment_max_bytes,
            writer: BufWriter::new(file),
            active,
            active_bytes,
            sealed,
            scratch: Vec::with_capacity(64 * 1024),
        };

        Ok((wal, Replay { records, max_lsn }))
    }

    /// Durably append a batch. One fsync for the whole batch — this is the
    /// group-commit boundary. Returns only once the data is on disk.
    pub fn append(&mut self, records: &[Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        if self.active_bytes >= self.segment_max_bytes {
            self.rotate(records[0].lsn)?;
        }

        self.scratch.clear();
        for rec in records {
            record::encode(rec, &mut self.scratch);
        }

        self.writer.write_all(&self.scratch)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;

        self.active_bytes += self.scratch.len() as u64;
        self.active.max_lsn = self.active.max_lsn.max(
            records
                .iter()
                .map(|r| r.lsn)
                .max()
                .expect("batch is non-empty"),
        );

        Ok(())
    }

    /// Drop every sealed segment fully covered by `watermark`.
    ///
    /// Safe only *after* the Iceberg commit carrying `watermark` has landed —
    /// publish-then-truncate. Reversing the order loses data on a crash in
    /// between. The active segment is never deleted, even if fully covered:
    /// truncating the file being appended to is not worth the complexity, and
    /// replay skips its covered records anyway.
    pub fn truncate_through(&mut self, watermark: Lsn) -> Result<()> {
        let (covered, keep): (Vec<_>, Vec<_>) = self
            .sealed
            .drain(..)
            .partition(|s| s.max_lsn <= watermark && s.max_lsn > 0);

        self.sealed = keep;
        for seg in covered {
            fs::remove_file(&seg.path)?;
        }
        Ok(())
    }

    /// Bytes currently held across all live segments.
    pub fn size_bytes(&self) -> Result<u64> {
        let mut total = self.active_bytes;
        for seg in &self.sealed {
            total += fs::metadata(&seg.path)?.len();
        }
        Ok(total)
    }

    fn rotate(&mut self, next_lsn: Lsn) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        let path = self.dir.join(Self::segment_name(next_lsn));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let sealed = std::mem::replace(
            &mut self.active,
            Segment {
                path,
                start_lsn: next_lsn,
                max_lsn: 0,
            },
        );
        self.sealed.push(sealed);
        self.writer = BufWriter::new(file);
        self.active_bytes = 0;

        Ok(())
    }

    fn segment_name(start_lsn: Lsn) -> String {
        format!("{start_lsn:020}{SEGMENT_SUFFIX}")
    }

    fn list_segments(dir: &Path) -> Result<Vec<Segment>> {
        let mut segs = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(SEGMENT_SUFFIX) else {
                continue;
            };
            let Ok(start_lsn) = stem.parse::<Lsn>() else {
                continue;
            };
            segs.push(Segment {
                path,
                start_lsn,
                max_lsn: 0,
            });
        }
        segs.sort_by_key(|s| s.start_lsn);
        Ok(segs)
    }

    /// Read one segment. Returns its records, its highest LSN, and the byte
    /// offset of the end of the last intact frame.
    fn replay_segment(path: &Path) -> Result<(Vec<Record>, Lsn, u64)> {
        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;

        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let mut records = Vec::new();
        let mut max_lsn = 0;
        let mut off = 0usize;

        while let Some((rec, n)) = record::decode(&buf[off..], &name, off as u64)? {
            max_lsn = max_lsn.max(rec.lsn);
            records.push(rec);
            off += n;
        }

        Ok((records, max_lsn, off as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const SEG: u64 = 4096;

    fn recs(range: std::ops::Range<u64>) -> Vec<Record> {
        range
            .map(|i| Record::put(format!("key{i:04}").into_bytes(), i, vec![b'v'; 32]))
            .collect()
    }

    #[test]
    fn append_then_replay() {
        let dir = TempDir::new().unwrap();
        let (mut wal, replay) = Wal::open(dir.path(), SEG, 0).unwrap();
        assert!(replay.records.is_empty());

        wal.append(&recs(1..50)).unwrap();
        drop(wal);

        let (_, replay) = Wal::open(dir.path(), SEG, 0).unwrap();
        assert_eq!(replay.records.len(), 49);
        assert_eq!(replay.max_lsn, 49);
        assert_eq!(replay.records[0].lsn, 1);
    }

    #[test]
    fn replay_skips_records_below_watermark() {
        let dir = TempDir::new().unwrap();
        let (mut wal, _) = Wal::open(dir.path(), SEG, 0).unwrap();
        wal.append(&recs(1..101)).unwrap();
        drop(wal);

        let (_, replay) = Wal::open(dir.path(), SEG, 60).unwrap();
        assert_eq!(replay.records.len(), 40);
        assert_eq!(replay.records[0].lsn, 61);
        // max_lsn reports what is on disk, not what was replayed.
        assert_eq!(replay.max_lsn, 100);
    }

    #[test]
    fn a_torn_tail_loses_only_the_torn_frame() {
        let dir = TempDir::new().unwrap();
        let (mut wal, _) = Wal::open(dir.path(), SEG, 0).unwrap();
        wal.append(&recs(1..21)).unwrap();
        let path = wal.active.path.clone();
        drop(wal);

        // Simulate a crash partway through the last append.
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 5)
            .unwrap();

        let (mut wal, replay) = Wal::open(dir.path(), SEG, 0).unwrap();
        assert_eq!(replay.records.len(), 19);
        assert_eq!(replay.max_lsn, 19);

        // And the truncated file is clean enough to keep appending to.
        wal.append(&recs(20..25)).unwrap();
        drop(wal);
        let (_, replay) = Wal::open(dir.path(), SEG, 0).unwrap();
        assert_eq!(replay.records.len(), 24);
        assert_eq!(replay.max_lsn, 24);
    }

    #[test]
    fn rotation_and_truncation() {
        let dir = TempDir::new().unwrap();
        let (mut wal, _) = Wal::open(dir.path(), 1024, 0).unwrap();

        // Each append is a batch; rotation happens on batch boundaries.
        for i in 0..20u64 {
            wal.append(&recs(i * 10 + 1..i * 10 + 11)).unwrap();
        }
        assert!(!wal.sealed.is_empty(), "expected the log to have rotated");

        let watermark = wal.sealed[0].max_lsn;
        wal.truncate_through(watermark).unwrap();
        drop(wal);

        let (_, replay) = Wal::open(dir.path(), 1024, watermark).unwrap();
        assert_eq!(replay.max_lsn, 200);
        assert!(replay.records.iter().all(|r| r.lsn > watermark));
    }

    #[test]
    fn truncation_never_drops_the_active_segment() {
        let dir = TempDir::new().unwrap();
        let (mut wal, _) = Wal::open(dir.path(), SEG, 0).unwrap();
        wal.append(&recs(1..10)).unwrap();

        wal.truncate_through(1_000_000).unwrap();
        assert!(wal.active.path.exists());
    }
}
