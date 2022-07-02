// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;
//MemTable是在内存中的数据结构，用于保存最近更新的数据，会按照Key有序地组织这些数据，
class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator);

 // 因为 memtable是用来保存数据的，所以禁止拷贝构造和赋值操作
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

  // TODO begin
  uint64_t GetTailSequence(){ return tail_sequence_; }
  uint64_t GetLogFileNumber(){ return log_file_number_; }
  uint64_t SetLogFileNumber(uint64_t fid){  log_file_number_ = fid; }
  // TODO end

 private:
  // TODO ： 友元函数，后续需要注意这里可能会破坏封装？
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef SkipList<const char*, KeyComparator> Table;

  ~MemTable();  // Private since only Unref() should be used to delete it
  // TODO begin memtable 中最大的sequence，用来记录最大的imm 转到 sst的时候 应该记录到 manifest 中的imm_last_sequence这个值
  // TODO 用于db再次打开的时候恢复的作用。下面两个保持一起更新
  uint64_t tail_sequence_;
  uint64_t log_file_number_;
  // TODO end
  
  KeyComparator comparator_;
  // TODO : 为什么不用原子操作呢
  int refs_;
  Arena arena_;
  Table table_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
