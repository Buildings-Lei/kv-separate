// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
  // 按照字典序进行比较的比较器，默认比较器
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    // a > b 返回 1 
    return a.compare(b);
  }
  // 找到最短的区分部分
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    // 去除公共的部分
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      // 取 8位 刚刚好是一个字符char类型的大小，其实刚刚好取出一个字符。
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        // 为了可以 Tack on the earliest possible number to the shortened user key.
        // 这里之所以要加1 后，后面的sequence 和 type 会拉到最大值（key中 这两个值 值小的 key大，是反的）
        // 比如说 aaac -》 aaad（后面的sequence 和 type 拉到最大，时期变成aaad* 中最小的值）
        (*start)[diff_index]++;
        // 从0开始的，所以要加上 1 ,后面就都不要了。
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }
  // 将 *key 变成  string a ，a >= *key.
  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }

};

}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
