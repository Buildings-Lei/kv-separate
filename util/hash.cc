// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/hash.h"

#include <cstring>

#include "util/coding.h"

// The FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels. The real definition should be provided externally.
// This one is a fallback version for unsupported compilers.
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif

namespace leveldb {

// n 表示data的长度
// Murmur哈希的算法确实比较简单，它的计算过程其实就是它的名字，MUltiply and Rotate，
// 因为它在哈希的过程要经过多次MUltiply and Rotate，所以就叫 MurMur 了。
//MurMurHash3 比 MD5 快。
//MurMurHash3 128 位版本哈希值是 128 位的，跟 MD5 一样。128 位的哈希值，在数据量只有千万级别的情况下，基本不用担心碰撞。
// 32 位哈希值发生碰撞的可能性就比 128 位的要高得多。当数据量达到十万时，就很有可能发生碰撞。
//散列值比较“均匀”，如果用于哈希表，布隆过滤器等, 元素就会均匀分布。
uint32_t Hash(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  // 0的任意次方都是0
  // n为数据长度,m是固定值;也就是说数据长度不同,那么hash值也会有区别
  uint32_t h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    // 4个字节通过无整数编码的值
    uint32_t w = DecodeFixed32(data);
    data += 4;
    // h = (h + w)*m
    h += w;
    h *= m;
    // rotate ??
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  // h = (h + data[0,1,2])*m
  switch (limit - data) {
    case 3:
      h += static_cast<uint8_t>(data[2]) << 16;
      FALLTHROUGH_INTENDED;
    case 2:
      h += static_cast<uint8_t>(data[1]) << 8;
      FALLTHROUGH_INTENDED;
    case 1:
      h += static_cast<uint8_t>(data[0]);
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

}  // namespace leveldb
