// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {
// 里面是不包含有 data block 的数据的，用file进行读取文件
// 里面有 filter 和 index_block ，可以用来定位文件中的位置
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  //sstable文件对应的句柄
  RandomAccessFile* file;
  //唯一表示一个sstable对应的cache
  //与data block的offset构成定位data block的依据
  uint64_t cache_id;
  //filter block对应的读对象FilterBlockReader
  FilterBlockReader* filter;
  //filter block对应的原始数据
  const char* filter_data;
  //metaindex 对应的handle
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  //index block对应的Block,可以通过该对象来解析获取对应的Data block内容
  Block* index_block;
};

// 打开文件，然后生成table返回， size : 要打开文件的末尾位置。
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  //从sstable文件中读取footer
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  //解码,初始化Footer结构
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  // 从sstable文件中读取index block内容到index_block_contents
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  // 将 index-data中的内容读到 index_block_contents 当中.
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // 将index block对应内容存储到Table::Rep结构中
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    // 过滤器要进行单独的操作，需要FilterBlockReader 方便操作。
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

//读取metaindex,并且根据metaindex读取filter block完成,完成初始化FilterBlockReader对象
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //读取metaindex内容
  //从sstable文件中读取metaindex block内容到contents;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);
  
  //metaindex只存储一对key/value;而且key是固定值,value为“filter block”对应的offset以及size
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  // 可能会有多种的过滤器进行进行过滤
  // iter 是遍历 meta_index_block  key -value 为 key ： 过滤器名字，value: 对应的 偏移量 和size
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    // 找到合适的 过滤器就开始 读偏移量对应的内容 就是过滤器保存下来的值
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

//读取filter block,并且初始化FilterBlockReader对象
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //读取filter block对应的内容到block中
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  //初始化filter block对应的对象FilterBlockReader
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}
// 
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// arg ：table 在这个table中读取 datablock，index_value : data block的索引
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  // 一个 option 只有一个缓存，默认8M
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    // 如果开启了缓存的情况下：
    if (block_cache != nullptr) {
      //每个Table对应一个唯一的 cache_id;handle.offset()对应一个唯一的Data Block数据块
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      // key : cache_id + index_offset ,value : index_offset 对应的 data_block 的地址（包装起来的 Block*）
      cache_handle = block_cache->Lookup(key);
      // 如果 该data block是在缓存中，那么直接 返回找到的 Block* 的值就行
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {// 没有在缓存当中，所以直接去文件中读取，并将其放入到缓存当中
        //读取的是sstable中的一个data block块 ，
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            //所以block_cache缓存的基本单位是一个Data block块对应的Block对象，value 就是一个 Block*
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else { // 没有开启缓存的情况下，直接从文件中读取
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  //生成data block对应的迭代器;用于完成data block内部的查找逻辑
  // 注册删除函数：
  // 当迭代器的寿命使用完的时候，如果没有缓存的话将会释放 block 的空间
  // 如果有缓存的话，因为之前 lookup 或者是 insert 过一次，所以这里要release 一次调用unref
  // iter 析构后并不会把 block 给释放掉了，而是继续在缓存当中。
  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}
// 调用 index_block 的 NewIterator 访问索引block
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}
// 在本 table中查询是否 key
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  //在index block中查找对应的datablock
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    //如果包含filter block,在filter block中查找是否包含key
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      //在data block中查找具体的key/value;注意通过Iterator实现
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        // 找到了 key - value 调用回调函数直接进行处理
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      // 要释放 iter 同时也给指向的block 进行了一次unref
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
