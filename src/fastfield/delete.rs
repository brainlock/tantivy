use crate::common::HasLen;
use crate::directory::ReadOnlySource;
use crate::directory::WritePtr;
use crate::space_usage::ByteCount;
use crate::DocId;
use bit_set::BitSet;
use std::io;
use std::io::Write;

/// Write a delete `BitSet`
///
/// where `delete_bitset` is the set of deleted `DocId`.
pub fn write_delete_bitset(delete_bitset: &BitSet, writer: &mut WritePtr) -> io::Result<()> {
    let max_doc = delete_bitset.capacity();
    let mut byte = 0u8;
    let mut shift = 0u8;
    for doc in 0..max_doc {
        if delete_bitset.contains(doc) {
            byte |= 1 << shift;
        }
        if shift == 7 {
            writer.write_all(&[byte])?;
            shift = 0;
            byte = 0;
        } else {
            shift += 1;
        }
    }
    if max_doc % 8 > 0 {
        writer.write_all(&[byte])?;
    }
    writer.flush()
}

/// Set of deleted `DocId`s.
#[derive(Clone)]
pub struct DeleteBitSet {
    data: ReadOnlySource,
    len: usize,
}

impl DeleteBitSet {
    /// Opens a delete bitset given its data source.
    pub fn open(data: ReadOnlySource) -> DeleteBitSet {
        let num_deleted: usize = data
            .as_slice()
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();
        DeleteBitSet {
            data,
            len: num_deleted,
        }
    }

    /// Returns an iterator over the `DocId`s in the `DeleteBitSet`
    pub fn doc_ids(&self) -> impl Iterator<Item = DocId> + '_ {
        self.data
            .as_slice()
            .iter()
            .enumerate()
            .flat_map(|(byte_offset, byte)| {
                let mut doc_ids = Vec::with_capacity(8);
                let mut byte = *byte;
                let mut shift = 0;
                while byte > 0 {
                    if byte & 1u8 == 1 {
                        doc_ids.push((byte_offset * 8 + shift) as DocId)
                    }

                    // let's skip as many zero-bits as we can at once
                    let next_byte = byte >> 1;
                    let skip = if next_byte > 0 {
                        next_byte.trailing_zeros()
                    }else{
                        0
                    };

                    byte = byte >> (1 + skip);
                    shift += 1 + skip as usize;
                }
                doc_ids
            })
    }

    /// Returns true iff the document is still "alive". In other words, if it has not been deleted.
    pub fn is_alive(&self, doc: DocId) -> bool {
        !self.is_deleted(doc)
    }

    /// Returns true iff the document has been marked as deleted.
    #[inline(always)]
    pub fn is_deleted(&self, doc: DocId) -> bool {
        let byte_offset = (doc / 8u32) as usize;

        self.data
            .as_slice()
            .get(byte_offset)
            .map(|b: &u8| {
                let shift = (doc & 7u32) as u8;
                b & (1u8 << shift) != 0
            })
            .unwrap_or(false)
    }

    /// Summarize total space usage of this bitset.
    pub fn space_usage(&self) -> ByteCount {
        self.data.len()
    }
}

impl HasLen for DeleteBitSet {
    fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::*;
    use bit_set::BitSet;
    use std::path::PathBuf;

    fn test_delete_bitset_helper(bitset: &BitSet) {
        let test_path = PathBuf::from("test");
        let mut directory = RAMDirectory::create();
        {
            let mut writer = directory.open_write(&*test_path).unwrap();
            write_delete_bitset(bitset, &mut writer).unwrap();
        }
        {
            let source = directory.open_read(&test_path).unwrap();
            let delete_bitset = DeleteBitSet::open(source);
            let n = bitset.capacity();
            for doc in 0..n {
                assert_eq!(bitset.contains(doc), delete_bitset.is_deleted(doc as DocId));
            }
            assert_eq!(delete_bitset.len(), bitset.len());
        }
    }

    #[test]
    fn test_delete_bitset() {
        {
            let mut bitset = BitSet::with_capacity(10);
            bitset.insert(1);
            bitset.insert(9);
            test_delete_bitset_helper(&bitset);
        }
        {
            let mut bitset = BitSet::with_capacity(8);
            bitset.insert(1);
            bitset.insert(2);
            bitset.insert(3);
            bitset.insert(5);
            bitset.insert(7);
            test_delete_bitset_helper(&bitset);
        }
    }

    #[test]
    fn test_delete_bitset_is_deleted_does_not_crash_out_of_bounds() {
        let mut directory = RAMDirectory::create();
        let test_path = PathBuf::from("test");

        let bitset = BitSet::with_capacity(8);

        let mut writer = directory.open_write(&*test_path).unwrap();
        write_delete_bitset(&bitset, &mut writer).unwrap();

        let source = directory.open_read(&test_path).unwrap();
        let delete_bitset = DeleteBitSet::open(source);

        assert_eq!(
            false,
            delete_bitset.is_deleted((bitset.capacity() + 1) as DocId)
        )
    }

    fn data_for_test_doc_ids_iterator() -> Vec<Vec<DocId>> {
        vec![
            vec![],
            vec![0],
            vec![1],
            vec![2],
            vec![0, 2],
            vec![0u32, 7, 8, 11, 31, 32, 33, 51],
            vec![31],
            vec![32],
            vec![33],
            vec![33, 150],
        ]
    }

    #[test]
    fn test_doc_ids_iterator() {
        for test_deleted_ids in data_for_test_doc_ids_iterator() {
            let mut directory = RAMDirectory::create();
            let test_path = PathBuf::from("test");

            let mut bitset = BitSet::with_capacity(64);

            for i in &test_deleted_ids {
                bitset.insert(*i as usize);
            }

            let mut writer = directory.open_write(&*test_path).unwrap();
            write_delete_bitset(&bitset, &mut writer).unwrap();

            let source = directory.open_read(&test_path).unwrap();
            let delete_bitset = DeleteBitSet::open(source);

            let doc_ids: Vec<DocId> = delete_bitset.doc_ids().collect();

            assert_eq!(test_deleted_ids, doc_ids);
        }
    }
}
