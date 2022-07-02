// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

//定义了日志的block大小，记录类型，记录的header大小等相关常量信息。
//每条记录的header由checksum (4 bytes), length (2 bytes), type (1 byte)组成。
enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

//32KB
static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
    static const int kHeaderSize = 4 + 2 + 1;

// TODO begin
//4M 每次读取value log 的大小，gc 和 恢复会用到
static const int vBlockSize = 4*1024*1024;

// Header is checksum (4 bytes), length (4 bytes), type (1 byte).
static const int vHeaderSize = 4 + 4 ;
//write_batch 头部
static const int wHeaderSize = 8 + 4 ;
// TODO end




}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
