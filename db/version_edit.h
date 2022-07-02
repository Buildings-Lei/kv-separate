// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// sstable文件元数据
struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;            // 引用计数
  // Seeks allowed until compaction; 当该值为0时,意味着需要进行compaction操作了; 变量allowed_seeks的值在sstable文件加入到version时确定
  int allowed_seeks;  
  uint64_t number;       //文件名相关;sstable文件的名字是 number.ldb
  uint64_t file_size;    // File size in bytes  文件大小
  InternalKey smallest;  // Smallest internal key served by table 最小的key
  InternalKey largest;   // Largest internal key served by table 最大的key
};

// TODO begin
// 保存
    struct LogMetaData {
        LogMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

        int refs;            // 引用计数
        // Seeks allowed until compaction; 当该值为0时,意味着需要进行compaction操作了; 变量allowed_seeks的值在sstable文件加入到version时确定
        int allowed_seeks;
        uint64_t number;       //文件名相关;sstable文件的名字是 number.ldb
        uint64_t file_size;    // File size in bytes  文件大小
        InternalKey smallest;  // Smallest internal key served by table 最小的key
        InternalKey largest;   // Largest internal key served by table 最大的key
    };
// TODO end

//Manifest 文件保存了整个 LevelDB 实例的元数据，比如：每一层有哪些 SSTable。 
//格式上，Manifest 文件其实就是一个 log 文件，一个 log record 就是一个 VersionEdit
//(注意:落盘的格式全部都是VersionEdit,Version需要转换为VersionEdit才能够落盘)。
//LevelDB 用 VersionEdit 来表示一次元数据的变更。Manifest 文件保存 VersionEdit 序列化后的数据。
//version_n + version_eidt = version_n+1
//Version 是 VersionEdit 进行 apply 之后得到的数据库状态——当前版本包含哪些 SSTable，并通过引用计数保证多线程并发访问的安全性。
//读操作要读取 SSTable 之前需要调用 Version::Ref 增加引用计数，不使用时需要调用 Version::UnRef 减少引用计数。

//MANIFEST文件和LOG文件一样，只要DB不关闭，这个文件一直在增长。
//事实上，随着时间的流逝，早期的版本是没有意义的，我们没必要还原所有的版本的情况，我们只需要还原Alive的版本的信息。
//MANIFEST只有一个机会变小，抛弃早期过时的VersionEdit，给当前的VersionSet来个快照，然后从新的起点开始累加VerisonEdit。这个机会就是重新开启DB。
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  
  //设置最大序列号last_sequence_(内部key的编码号)
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  // TODO begin
  //设置序列号 imm_last_sequence_ imm 转 sst的时候用
  void SetImmLastSequence(SequenceNumber seq,uint64_t fid) {
      has_imm_last_sequence_ = true;
      imm_last_sequence_ = seq;
      imm_log_file_number_ = fid;
  }

  // TODO end
  //增加指定level进行下一次 compaction 的起始 key
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  // 添加指定的文件到lsm中的指定层中
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  // 记录要删除sstable文件的信息
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  //VersionEdit 通过成员函数 EncodeTo 和 DecodeFrom 进行序列化和反序列化
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  //pair: (level-no,file-no)
  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  //比较器的名称，这个在创建 LevelDB 的时候就确定了，以后都不能修改。(必须包含)
  //配置文件ptions.comparator定义
  std::string comparator_;
  //最小的有效 log number。小于 log_numbers_ 的 log 文件都可以删除。
  uint64_t log_number_;
  //已经废弃，代码保留是为了兼容旧版本的 LevelDB。
  uint64_t prev_log_number_;
  //下一个文件的编号(最小值为2) 。
  uint64_t next_file_number_;
  //SSTable 中的最大的 sequence number。
  SequenceNumber last_sequence_;

  //是否包含comparator_
  bool has_comparator_;
  //是否包含log_number_
  bool has_log_number_;
  //是否包含prev_log_number_
  bool has_prev_log_number_;
  //是否包含next_file_number_
  bool has_next_file_number_;
  //是否包含last_sequence_
  bool has_last_sequence_;
  // TODO begin
  // 是否包含 imm_last_sequence_
  bool has_imm_last_sequence_;
  // 恢复log的时候 用来定位memtable 和 immemtabl中的位置
  SequenceNumber imm_last_sequence_;
  // imm_last_sequence 所处在的log文件
  uint64_t imm_log_file_number_;
  // TODO end

  //记录每一层要进行下一次 compaction 的起始 key。
  // pair: (level-no,internal_key)
  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  //可以删除的 SSTable（level-no -> file-no）。
  DeletedFileSet deleted_files_;
  //新增的 SSTable。
  //pair : (level-no,FileMetaData)
  std::vector<std::pair<int, FileMetaData>> new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
