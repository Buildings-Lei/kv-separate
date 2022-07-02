// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

// blockContens 中的数据放进 block中，方便操作和迭代
// 解析出来的data block 不好进行查找 所以用一个block 进行代理的功能。里面内涵迭代器
class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  //非常重要,对Block内部结构的查找遍历都是依赖该迭代器
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;//内存
  size_t size_;//内存大小
  uint32_t restart_offset_;  // Offset in data_ of restart array  restart offset‘s offset
  bool owned_;               // Block owns data_[];内存是否分配到heap区
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
