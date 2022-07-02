// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
// 磁盘上最大的level个数，默认为7
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
// 第0层sstable个数到达这个阈值时触发压缩，默认值为4
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
// 第0层sstable到达这个阈值时，延迟写入1ms，将CPU尽可能的移交给压缩线程，默认值为8
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
// 第0层sstable到达这个阈值时将会停止写，等到压缩结束，默认值为12
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
// 新压缩产生的sstable允许最多推送至几层(目标层不允许重叠)，默认为2
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
// 在数据迭代的过程中，也会检查是否满足压缩条件，该参数控制读取的最大字节数
static const int kReadBytesPeriod = 1048576;
// TODO begin
// gc后台回收的时候进行 batch合并之后再写入的大小。
static const uint64_t gWriteBatchSize = 4*1024*1024;
// TODO end
}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
// 记录当前操作类型，后续压缩会根据该类型进行对应操作。
// ValueType是为了区分一个键是插入还是删除，删除其实也是一条数据的插入，但是是一条特殊的插入，
// 通过在User Key后面附上kTypeDeletion来说明要删除这个键，而kTypeValue说明是插入这个键。
// TODO begin kTypeSeparate 表示插入的key-value，需要进行kv分离。
enum ValueType { kTypeDeletion = 0x0,kTypeSeparate = 0x01, kTypeValue = 0x2};
// TODO end
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

// https://zhuanlan.zhihu.com/p/272468157
// 一个递增的uint64整数，相同key则按照其降序，最大值为 ((0x1ull << 56) - 1);
//SequenceNumber是一个版本号，是全局的，每次有一个键写入时，都会加一，
//每一个Internal Key里面都包含了不同的SequenceNumber。SequenceNumber是单调递增的，
//SequenceNumber越大，表示这键越新，如果User Key相同，就会覆盖旧的键。
// 所以就算User Key相同，对应的Internal Key也是不同的，Internal Key是全局唯一的。
// 当我们更新一个User Key多次时，数据库里面可能保存了多个User Key，
//但是它们所在的Internal Key是不同的，并且SequenceNumber可以决定写入的顺序。
// 当用户写入时，将User Key封装成Internal Key，保留版本信息，存储到SSTable里，当需要读取
// 时，将User Key从Internal Key里提取出来，所有User Key相同的Internal Key里面
// SequenceNumber最大的Internal Key就是当前的键，它对应的值就是当前值。
// 当Internal Key里面包含的User Key不同时，直接用User Key的比较方式即可。
// 否则，根据SequenceNumber比较，按SequenceNumber倒序排序。
// 这样的好处就是，在全局有序的Map里，根据User Key排序，User Key相同的会排在一起，SequenceNumber大的Internal Key排在前面。
// 当Seek一个User Key时，会定位到第一个符合条件的Internal Key，也就是具有最大的SequenceNumber的Internal Key。
// 除了比较器，布隆过滤器也会被改造，以适应User Key到Internal Key的转换。
// Internal Key里的SequenceNumber主要是为了支持Snapshot的功能。
// 当生成一个DB Iterator或者给一个Option显示设置一个Snapshot，只会读取那个时刻的数据。
// 实现方式就是当读取的时候，如果Snapshot对应的SequenceNumber小于Internal Key的SequenceNumber，
// 那么这个键就是不可见的，找到可见的SequenceNumber最大的Internal Key就是需要读取的键的值。
typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
// 这里取56 位 是因为还要留下 8 位给type
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

// 从 InternalKey 中解压出来的 ，方便使用
struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {}
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
// 8 个字节存放的是type 和 sequence
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
// 在 user_key 后面加上 type 和 sequence的信息。
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
// On error, returns false, leaves "*result" in an undefined state.
// InternalKey 转 parseInternalKey
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// Returns the user key portion of an internal key.
// 把slice 的长度改成真正的data的长度，而不是加了type和 sequence的长度。
inline Slice ExtractUserKey(const Slice& internal_key) {
  // 至少要有 8 个字节 因为 type 和sequence 编码后就是8个字节
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;

 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  const char* Name() const override;
  int Compare(const Slice& a, const Slice& b) const override;
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override;
  void FindShortSuccessor(std::string* key) const override;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;

 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override;
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;

 public:
  InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
  // slice 的data 部分是不进行编码的 
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }
  // 把s.data的数据直接分配给 rep_.
  bool DecodeFrom(const Slice& s) {
    rep_.assign(s.data(), s.size());
    return !rep_.empty();
  }

  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }
  // 重新 改变 InternalKey 中的值
  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};
// InternalKeyComparator 是 InternalKey的比较器
// 比较两个 InternalKey 中的rep 的字典序。
inline int InternalKeyComparator::Compare(const InternalKey& a,
                                          const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

// InternalKey 转 parseInternalKey，方便一些
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  //反解码出来的 一共是64字节，第一个字节存的是 type信息。第二个字节存的是 sequence信息。
  uint8_t c = num & 0xff; 
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  // 验证一下
  // TODO begin
  return (c <= static_cast<uint8_t>(kTypeValue));
  // TODO end
}

// A helper class useful for DBImpl::Get()
// 包含了 key_length ,user_key ,type ,sequence
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  // 为了能够让程序员显式的禁用某个函数，C++11 标准引入了一个新特性："=delete"函数。程序员只需在函数声明后上“=delete;”，就可将该函数禁用。
  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];  // Avoid allocation for short keys，为了加快速度，避免一直要从堆中申请空间
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
