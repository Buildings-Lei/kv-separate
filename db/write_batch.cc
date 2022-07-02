// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
// sequence number(8 bytes), put/del count(4 bytes)
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  belong_to_gc = false;
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

// 对WriteBatch 中存储的所有的record 进行操作，该插入memtable的插入memtable，该删除的删除
// handler 为 MemTableInserter inserter;
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  // 12个字节，8个字节用来表示sequence，4个字节用来表示 count，移除头
  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    // record 记录为  1 个字节 是 kTypeValue ,剩下的字节是 key value
    // record 记录为  1 个字节 是 kTypeDeletion， 剩下的字节是key 
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

// TODO begin

// 对WriteBatch 中存储的所有的record 进行操作，该插入memtable的插入memtable，该删除的删除
// handler 为 MemTableInserter inserter;
    Status WriteBatch::Iterate(Handler* handler,uint64_t fid, uint64_t offset) const {
        Slice input(rep_);
        // 整个writebatch 的起始地址
        const char* begin = input.data();

        if (input.size() < kHeader ) {
            return Status::Corruption("malformed WriteBatch (too small)");
        }
        // 12个字节，8个字节用来表示sequence，4个字节用来表示 count，移除头
        input.remove_prefix(kHeader);
        Slice key, value;
        int found = 0;
        while (!input.empty()) {
            found++;
            const uint64_t kv_offset = input.data() - begin + offset;
            assert( kv_offset > 0);

            // record 记录为  1 个字节 是 kTypeValue ,剩下的字节是 key value
            // record 记录为  1 个字节 是 kTypeDeletion， 剩下的字节是key
            char tag = input[0];
            input.remove_prefix(1);
            switch (tag) {
                case kTypeValue:
                    if (GetLengthPrefixedSlice(&input, &key) &&
                        GetLengthPrefixedSlice(&input, &value)) {
                        handler->Put(key, value);
                    } else {
                        return Status::Corruption("bad WriteBatch Put");
                    }
                    break;
                case kTypeDeletion:
                    if (GetLengthPrefixedSlice(&input, &key)) {
                        handler->Delete(key);
                    } else {
                        return Status::Corruption("bad WriteBatch Delete");
                    }
                    break;
                case kTypeSeparate:
                    if (GetLengthPrefixedSlice(&input, &key) &&
                        GetLengthPrefixedSlice(&input, &(value)) ){
                        // value = fileNumber + offset + valuesize 采用变长编码的方式
                        std::string dest;
                        PutVarint64(&dest,fid);
                        PutVarint64(&dest,kv_offset);
                        PutVarint64(&dest,value.size());
                        Slice value_offset(dest);
                        handler->Put(key, value_offset,kTypeSeparate);
                    } else {
                        return Status::Corruption("bad WriteBatch Put");
                    }
                    break;
                default:
                    return Status::Corruption("unknown WriteBatch tag");
            }
        }
        if (found != WriteBatchInternal::Count(this)) {
            return Status::Corruption("WriteBatch has wrong count");
        } else {
            return Status::OK();
        }
    }
// TODO end

// 返回key的对数，
int WriteBatchInternal::Count(const WriteBatch* b) {
  // 因为 count 是固定的 4个字节 所以用 fix32
  return DecodeFixed32(b->rep_.data() + 8);
}
// 设置 key的个数 前 12个字节 ，8个为sequence，4个为key的个数
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  // 因为 count 是固定的 4个字节所以用 fix32
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}
 // 在rep_ 后面不断的增加 record的记录 kTypeValue key value
void WriteBatch::Put(const Slice& key, const Slice& value) {
  // WriteBatchInternal 是WriteBatch的友元函数，通过WriteBatchInternal 实现WriteBatch中的函数功能
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // TODO begin
  if( value.size() >= separate_threshold_ ){
      rep_.push_back(static_cast<char>(kTypeSeparate));
  }else{
      rep_.push_back(static_cast<char>(kTypeValue));
  }
  // TODO end
  PutLengthPrefixedSlice(&rep_,  key);
  PutLengthPrefixedSlice(&rep_, value);
}
// 在rep_ 后面不断的增加 record的记录 kTypeDeletion key 由于是删除所以没有value
void WriteBatch::Delete(const Slice& key) {
  // WriteBatchInternal 是WriteBatch的友元函数，通过WriteBatchInternal 实现WriteBatch中的函数功能
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}
// 将多个WriteBatch 进行合并，底层还是在调用WriteBatchInternal 中的实现
void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
  // 用来插入 memtable 中的 skiplist 表的
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;
  // TODO begin 增加了 type 的选择
  // 往memtable中添加数据
  void Put(const Slice& key, const Slice& value, ValueType type = kTypeValue) override {
    mem_->Add(sequence_, type, key, value);
    sequence_++;
  }
  // TODO end
  // 往memtable中删除数据
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};

}  // namespace

// 将WriteBatch 中的record的值 插入到memtable中
Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  // 一批 writeBatch中只有一个sequence，公用的，后续会自加。
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}
// TODO begin
// 将WriteBatch 中的record的值 插入到memtable中,offset 表示WriteBatch 在log中的偏移
    Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable,uint64_t fid, size_t offset) {
        MemTableInserter inserter;
        // 一批 writeBatch中只有一个sequence，公用的，后续会自加。
        inserter.sequence_ = WriteBatchInternal::Sequence(b);
        inserter.mem_ = memtable;
        return b->Iterate(&inserter, fid, offset);
    }
// TODO end

// 给 writeBatch 中的rep进行内容赋值
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

// 多个 WriteBatch 进行合并，但是不考虑空间的释放的问题
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
