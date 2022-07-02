// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// StartBlock的任务是根据block_offset创建filter。
// 默认每2K block_offset就创建一个filter。默认值2K是由常量kFilterBase指定的。
// 每个data block写入文件后，都会调用StartBlock,探测是否需要更新filterblock信息
// block_offset 表示 data block的写入数据量

// TODO : 有点问题，假设，block_offset == 4k ，filter_offsets_.size() == 0
// 那么 filter_index == 2, 这个时候 进入while循环 GenerateFilter（）一次就把所有的数据都布隆映射掉了，
// 怎么体现 每 2k 进行一次转换
void FilterBlockBuilder:: StartBlock(uint64_t block_offset) {
  //计算需要的filter数量
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  //比如block_offset是5K，那么filter_index就等于2，需要两个filter。
  //如果下次调用的block_offset是5.5K，那么计算出来的filter_index还是2，不会新建filter。
  while (filter_index > filter_offsets_.size()) {
    //GenerateFilter用keys_和start_中保存的key列表创建一个filter，创建完成后清空keys_和start_。
    GenerateFilter();
  }
}

//AddKey把key保存到key列表中。更新keys_,start_
void FilterBlockBuilder:: AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

//format  filter data | filter data offset | filter offset's offset | footer
//用Finish完成filter block的构建，Finish会先为剩余的key创建一个新的filter，
//然后把filter offset列表按照顺序编码到result_中，把offset列表的offset编码到result_最后面。
//最后把常量kFilterBaseLg添加到最后，默认为11，2^11=2048=2KB，是为了表明每多少block_offset创建一个filter。
Slice FilterBlockBuilder::Finish() {
  //剩余key构建filter data
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // append filter data offset
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  //append filter data offset‘s offset
  PutFixed32(&result_, array_offset);
  // append footer
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

//GenerateFilter用keys_和start_中保存的key列表创建一个filter，创建完成后清空keys_和start_。
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  // 当前新添加的key的个数，如果没有，则记录上一次的result的大小
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    // 如果key数量为0，把下一个filter的offset放入filter_offsets_中，即让filter offset指向下一个filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  //把keys_中的key解析出来放到tmp_keys_中;BloomFilterPolicy参数为数组
  start_.push_back(keys_.size());  // Simplify length computation; 简化计算,无逻辑意义
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 调用CreateFilter创建filter，新建的filter会被append到result_中
  filter_offsets_.push_back(result_.size());
  // 针对当前已有的数据 构建布隆过滤器，注意每次是新加的
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);
  // 清空 等待下一次
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  //filter data offset‘s offset
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  //filter data offset
  offset_ = data_ + last_word;
  // filter data offset 数组中每个offset的大小为4个字节,所以num_就是offset数组的大小,也就是filter data的数量
  num_ = (n - 5 - last_word) / 4;
}
// 查看 key是否位于 这个data block里面，因为一个data block 对应一个过滤器
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  ////位于哪个filter data ，每个过滤器为2kb，所以有可能多个data block 对应一个过滤器
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      //获取第index个filter data
      Slice filter = Slice(data_ + start, limit - start);
      //查询key是否在filter中
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
