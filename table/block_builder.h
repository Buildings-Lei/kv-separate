// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;


// BlockBuilder接受一组key-value，将其序列化到buffer中，buffer的数据就是要写入文件中的数据。
// BlockBuilder主要是用于index_block/data_block/meta_index_block的构建。
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  //Add每次插入一个key-value
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  // 注意返回的 slice 的寿命是和这个build是一样长的，因为里面装的就是 buffer_
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  //LevelDB的Options。
  const Options* options_;
  //序列化数据。
  std::string buffer_;              // Destination buffer
  //重启点位置列表。
  std::vector<uint32_t> restarts_;  // Restart points
  //当前重启点关联的key数量。计数，用来触发当达到一定的数量的时候 就要重启了
  int counter_;                     // Number of entries emitted since restart
  //Finish()是否已经被调用过了。
  bool finished_;                   // Has Finish() been called?
  //上一次插入的key。用来共享压缩的
  std::string last_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
