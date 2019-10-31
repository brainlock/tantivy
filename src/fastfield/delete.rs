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
        DeleteBitSetIterator::new(&self)
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

pub struct DeleteBitSetIterator<'a> {
    delete_bitset_iter: std::iter::Enumerate<std::slice::Iter<'a, u8>>,
    byte_offset: usize,
    shift: u8,
    current_byte: Option<u8>,
    current_docid: Option<DocId>,
}

impl<'a> DeleteBitSetIterator<'a> {
    pub fn new(delete_bitset: &'a DeleteBitSet) -> DeleteBitSetIterator<'a> {
        DeleteBitSetIterator {
            delete_bitset_iter: delete_bitset.data.iter().enumerate(),
            byte_offset: 0,
            current_byte: None,
            shift: 0,
            current_docid: None,
        }
    }

    fn next_byte(&mut self) {
        if self.current_byte.is_none() {
            self.consume_byte();

            while self.current_byte.is_some() && self.current_byte.unwrap() == 0 {
                self.consume_byte();
            }

            self.shift = 0;
            self.current_docid = None;
        }
    }

    fn consume_byte(&mut self) {
        let (byte_offset, current_byte) = match self.delete_bitset_iter.next() {
            Some((byte_offset, byte)) => (byte_offset, Some(*byte)),
            None => (0, None)
        };
        self.current_byte = current_byte;
        self.byte_offset = byte_offset;
    }

    fn next_bit(&mut self) {
        if let Some(byte) = self.current_byte {
            let (current_byte, skipped) = self.skip_zeros(byte);
            self.shift += skipped;

            self.current_docid = Some((self.byte_offset * 8 + self.shift as usize) as DocId);

            self.shift += 1;

            let next_byte = current_byte >> 1;
            if next_byte > 0 {
                self.current_byte = Some(next_byte);
            }else{
                self.current_byte = None;
            }
        }else{
            self.current_docid = None;
        }
    }

    fn skip_zeros(&mut self, byte: u8) -> (u8, u8) {
        assert!(byte > 0);

        let skip = byte.trailing_zeros() as u8; // byte is u8, cannot have more than 8 trailing 0s
        let new_byte = byte >> skip;

        (new_byte, skip)
    }
}

impl<'a> Iterator for DeleteBitSetIterator<'a> {
    type Item = DocId;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_byte();
        self.next_bit();
        self.current_docid
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
