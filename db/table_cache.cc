// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  //抽象io句柄;可以是文件句柄;mmap句柄;内存句柄
  RandomAccessFile* file;
  //sstable对应的内存结构体;注意不包含具体的data block
  Table* table;
};


static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

//查询缓存系统中是否包含指定sstable文件,如果包含直接返回对应Cache::Handle
//如果不包含,则打开对应文件并且完成初始化以及添加到缓存系统中
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  //查找key位于缓存系统中的Cache::Handle;该结构中包含sstable文件相关信息(TableAndFile类等)
  *handle = cache_->Lookup(key);
  //在缓冲系统中未找到对应缓存
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    //抽象类,可以对应文件句柄,mmap句柄,内存句柄等不同io
    RandomAccessFile* file = nullptr;
    //缓存系统中对应的sstable文件结构
    Table* table = nullptr;
    //读取文件句柄;可能是文件fd,也可能是mmap对应fd;
    //mmap限制最多是1000个
    //这里只是打开句柄,并没有将内容全部读取到内存中
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    //打开sstable文件;并且读取到Table中
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      //将打开的sstable缓存到缓存系统中
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}
// 返回的迭代器 指向的是table（里面的是 指向table中的 index_block 的迭代器）
// file_number 和 file_size 确定的是具体的一个文件
// 生成这个文件的 二级迭代器，一级指向的是index_block , 二级指向的是 data_block。
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  // 前面FindTable里面有插入或者寻找过一次了，所以迭代器析构后要进行unref一次
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
// 从table缓存中，查询指定的 k，file_number : 文件名中的数字的部分，获取 key = k 对应的缓存。
// 查询指定文件 中的 key 对应的value值 
// arg ： handle_result 函数调用时候的参数 ，
// handle_result: 回调函数，当找到 key的时候处理的函数
// file_size : 文件的大小
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  // 查找指定的SSTable，优先去缓存找，找不到则去磁盘读取，读完之后在插入到缓存中
  // handle 其实是一个 LRUHandle 类型。里面的value 缓存的sstalbe具体内容;TableAndFile 对象或者Block等
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    // 这里的value缓存的就是 TableAndFile 对象
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    // 查找指定的key，内部先bf判断，然后缓存获取，最后在读取文件
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

//将对应sstable文件从缓存系统中删除
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
