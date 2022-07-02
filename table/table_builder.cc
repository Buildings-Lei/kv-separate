// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    // 由于 index的key的压缩效果不大，所以选择1
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;  // 要写入文件的封装
  uint64_t offset;  // 要写入文件中的偏移量
  Status status;   
  BlockBuilder data_block;  
  BlockBuilder index_block;
  std::string last_key; // 上一个key
  int64_t num_entries; //num_entries表示当前data block中有多少个key-value，
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;    // 用来看是否要写入 index block中
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  //其实这里啥也没有做;需要一个data block都写完或者大于2KB数据才会真实写
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  //检查是否存在错误，以及检查本次Add的key是否大于last_key，
  //TableBuilder只接受升序添加key，因为这个是为Memtable服务的，所以key必然是升序的。
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  //r->pending_index_entry is true only if data_block is empty.
  //如果本次key/value是data block的第一个插入;
  //则更新上一个data block的索引信息(保存offset和size),Flush函数计算得到
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 找到最短的区分部分，减少索引块等内部数据结构的空间需求。
    // last_key 会保持最必要的部分
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // 在flush中把data_block写入文件，并在pending_handle中保存offset和size
    r->pending_handle.EncodeTo(&handle_encoding);
    // 增加索引
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  //如果包含filter block,则将key加入到filter block中
  // 后续如果 filter_block 生成了一个过滤器后 会自动的将之前保存的key 信息删除掉
  // 这就相当于 一个data block 和一个过滤器对应起来。
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  //更新last_key，对于下次Add来说，last_key就是本次Add的key
  r->last_key.assign(key.data(), key.size());
  //num_entries表示当前data block中有多少个key-value，这里加1
  r->num_entries++;
  //把本次Add的key-value加入到data block中。data block会把数据序列化后放到自己的buffer中
  r->data_block.Add(key, value);

  //如果data_block的size达到阈值（默认为4K），就调用Flush把data block的buffer数据写入文件，这个data block也就完成了，
  //同时pending_index_entry被置为true，表示有一个index entry（即这个data block的index entry）等待写入。
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}
// 将数据刷到磁盘中
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  //把data_block写入文件，并在pending_handle中保存offset和size
  // 加上压缩type 和 crc 校验码刷入磁盘中
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    //把pending_index_entry置为true，并调用WritableFile::Flush把WritableFile内部的buffer写入磁盘
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  //创建filter block，FilterBlockBuilder每2KB创建一个filter block，所以会出现多个data block共用一个filter block的情况。
  //但是一个data block最多对应一个block filter。 这种策略的好处是，当data block很小时，避免创建太多的filter。
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

//WriteBlock把一个block写入文件中，这里有别于WriteRawBlock，WriteBlock需要先对block做一下处理，然后调用WriteRawBlock完成写入。
// 将 block 中的数据写入到 磁盘中，并且将这个数据的偏移地址和size存入到 handle.
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  //调用BlockBuilder的Finish完成block数据的最终构建，Finish会在尾部写入key的重启点列表，重启点是帮助多个key共用前缀的，以减少空间占用。
  Slice raw = block->Finish();

  //根据是否压缩来决定是否对block的数据进行压缩，如果需要压缩就调用压缩算法压缩后放到block_contents中，
  //反之直接把原始数据赋给block_contents，这里使用Slice作为载体，所以不会拷贝完整数据，开销不大。
  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  //调用WriteRawBlock把block数据写入文件，然后清空compressed_output和block。
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}


//WriteRawBlock把一段数据(raw block)写入文件中，并返回block在文件中的offset和size。
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  //把raw block的offset和size存入handle，给下一次的index使用，在ADD中if (r->pending_index_entry) {}进行使用
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());

  //把raw block数据写入文件，WritableFile是一个文件操作的抽象类，用于兼容不用操作系统的文件IO接口。
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    //生成trailer并写入文件，trailer总共5个字节，trailer[0]存储CompressionType，用于表示数据是否压缩及压缩类型。
    //trailer[1-4]存储crc校验和，crc校验和是基于raw block的数据域和trailer域计算出来的。
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      //把rep_的offset加上数据域size和trailer域size之和，表示文件当前已写入的总size。
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  //5部分磁盘顺序
  //data block list
  //filter block
  //meta index block
  //index block
  //footer

  Rep* r = rep_;
  //调用Flush，把当前的data block落盘，并添加对应的index entry和filter
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 前面说过filter block复用了BlockBuilder，这里完成filter block的构建，并写入文件，逻辑和前面介绍的WriteBlock类似。
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  //创建meta_index_block，同样复用BlockBuilder，里面只有一个key-value，key是filter.$Name，Name就是filter policy的名字，比如BloomFilter。
  //value是filter block的offset和size的编码结果。随后把meta_index_block写入文件。
  //metaindex_block_handle中存储了meta_index_block的offset和size。
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    // 将 meta_index_block 中的数据写入到 磁盘中，并且将这个数据的偏移地址和size存入到metaindex_block_handle
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  //如果有pending_index_entry就加入index block，逻辑和Add函数里添加index_entry类似，主要区别是找最短的最大key调用的是FindShortSuccessor，
  //把last_key置为最短的比last_key大的string，也是为了节省空间。
  //因为这是整个Memtable最后一个key了，所以新产生的key只需要大于last_key，不需要小于下一个key。
  //最后调用WriteBlock把index block写入文件。index_block_handle中存储了index block的offset和size。
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      // 前面的 Flush(); 最后把data block 落盘后还是 要更新index的数据
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  //生成footer，并写入文件; 48个字节
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
