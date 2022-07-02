// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"
#include "db/dbformat.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT WriteBatch {
 public:
    // TODO begin 增加了 ValueType type = kTypeValue
 // 基类 定义操作 比如 MemTableInserter: 对memtable表的操作
  class LEVELDB_EXPORT Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value, ValueType type = kTypeValue  ) = 0;
    virtual void Delete(const Slice& key) = 0;
  };
 // TODO end
  WriteBatch();
  // TODO begin
  WriteBatch(size_t separate_threshold): separate_threshold_(separate_threshold){ Clear();}
  // TODO end
  // Intentionally copyable.
  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

  // The size of the database changes caused by this batch.
  //
  // This number is tied to implementation details, and may change across
  // releases. It is intended for LevelDB usage metrics.
  size_t ApproximateSize() const;

  // Copies the operations in "source" to this batch.
  //
  // This runs in O(source size) time. However, the constant factor is better
  // than calling Iterate() over the source batch with a Handler that replicates
  // the operations into this batch.
  void Append(const WriteBatch& source);

  // Support for iterating over the contents of a batch.
  // 遍历batch结构，为了方便扩展，参数使用的是handler基类，对应的是抽象工厂模式
  // 采用抽象工厂模式：要使用就使用一整套东西
  Status Iterate(Handler* handler) const;

  Status Iterate(Handler* handler, uint64_t fid, uint64_t offset) const;

  bool IsGarbageColletion(){ return belong_to_gc; }

  void setGarbageColletion( bool is_gc ) { belong_to_gc = is_gc; }

 private:
  // TODO begin 如果是true 说明是gc过程的，不可以进行合并操作。
  bool belong_to_gc;
  // TODO end
  // WriteBatchInternal 是WriteBatch的友元函数，通过WriteBatchInternal 实现WriteBatch中的函数功能
  friend class WriteBatchInternal;
  size_t separate_threshold_;
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
