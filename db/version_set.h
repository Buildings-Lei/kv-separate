// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
////利用二分查找,找到key存在的文件下标编号
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
//检查是否和指定level的文件有重合
//disjoint_sorted_files=true，表明文件集合是互不相交、有序的
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);


//Version是管理某个版本的所有sstable的类，就其导出接口而言，无非是遍历sstable，查找k/v。以及为compaction做些事情，给定range，检查重叠情况。
//而它不会修改它管理的sstable这些文件，对这些文件而言它是只读操作接口。
//可见一个Version就是一个sstable文件集合，以及它管理的compact状态。
class Version {
 public:
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // 追加一系列iterator到 @*iters中，将在merge到一起时生成该Version的内容
  // 要求: Version已经保存了(见VersionSet::SaveTo)
  //函数功能是为该Version中的所有sstable都创建一个Two Level Iterator，以遍历sstable的内容。
  //对于level=0级别的sstable文件，直接通过TableCache::NewIterator()接口创建，这会直接载入sstable文件到内存cache中。
  //对于level>0级别的sstable文件，通过函数NewTwoLevelIterator()创建一个TwoLevelIterator，这就使用了lazy open的机制。
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // 在指定version中,给定@key查找value，如果找到保存在@*val并返回OK。
  // 否则返回non-OK，设置@ *stats.
  // 要求：没有hold lock
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  // 把@stats加入到当前状态中，如果需要触发新的compaction返回true
  // 要求：hold lock
  //Stat表明在指定key range查找key时，都要先seek此文件，才能在后续的sstable文件中找到key。
  //该函数是将stat记录的sstable文件的allowed_seeks减1，减到0就执行compaction。
  //也就是说如果文件被seek的次数超过了限制，表明读取效率已经很低，需要执行compaction了。
  //所以说allowed_seeks是对compaction流程的有一个优化。
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  //它在指定level中找出和[begin, end]有重合的sstable文件
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);


  // 如果指定level中的某些文件和[*smallest_user_key,*largest_user_key]有重合就返回true。
  // @smallest_user_key==NULL表示比DB中所有key都小的key.
  // @largest_user_key==NULL表示比DB中所有key都大的key.
  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // 决定一个新的memtalble应该放置在lsm中的那一层
  // result that covers the range [smallest_user_key,largest_user_key].
  //初始level为0，最高放到kMaxMemCompactLevel层。
  //能升到第level+1层需要满足一下要求：
  //level小于kMaxMemCompactLevel。
  //Key范围和level+1层的文件没有重叠。
  //Key范围和level+2层的文件可以有重叠，但是有重叠的文件的总size不超过10倍默认文件最大值
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  //返回level层对应的sstable文件个数
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  // 在lsm-tree中查找指定key的具体实现逻辑
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  // 每个 level 的所有 sstable 元信息。
  // files_[i]中的 FileMetaData 按照 FileMetaData::smallest 排序， 
  // 这是在每次更新都保证的。(参见 VersionSet::Builder::Save())
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  //下一个要compact的文件; seek compaction决定
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  // 根据size compaction决定的下一个应该compact的level和compaction分数.
  // 分数 < 1 说明compaction并不紧迫. 这些字段在Finalize()中初始化
  double compaction_score_;
  // 保存需要合并的层
  int compaction_level_;
};


/*
MVCC是一个数据库常用的概念。Multiversion concurrency control多版本并发控制。每一个执行操作的用户，
        看到的都是数据库特定时刻的的快照(snapshot), writer的任何未完成的修改都不会被其他的用户所看到;
        当对数据进行更新的时候并是不直接覆盖，而是先进行标记, 然后在其他地方添加新的数据，从而形成一个新版本, 
        此时再来读取的reader看到的就是最新的版本了。所以这种处理策略是维护了多个版本的数据的,但只有一个是最新的。
*/

//VersionSet 是一个 Version 的集合。
//随着数据库状态的变化，LevelDB 内部会不停地生成 VersionEdit——进而产生新的 Version。
//此时，旧的 Version 可能还在被正在执行的请求使用。所以，同一时刻可能存在多个 Version。
//VersionSet 用一个链表将这些 Version 维护起来，包含当前Alive的所有Version信息，列表中第一个代表数据库的当前版本。
//只有一个current version，持有最新的sstable集合。
//VersionSet类只有一个实例，在DBImpl(数据库实现类)类中，维护所有活动的Version对象，来看VersionSet的所有语境。
//数据库启动时,通过Current文件加载Manifset文件，读取Manifest文件完成版本信息恢复。
//Compaction时
//读取数据时,LevelDB通过VersionSet中的TableCache对象完成数据读取。
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  // 在current version上应用指定的VersionEdit，都会将VersionEdit作为一笔记录，追加写入到MANIFEST文件
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  //数据库启动时,通过Current文件加载Manifset文件，读取Manifest文件完成版本信息恢复。
  //Recover通过Manifest恢复VersionSet及Current Version信息，恢复完毕后Alive的Version列表中仅包含当Current Version对象。
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  //// 当前的MANIFEST文件号
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  //// 分配并返回新的文件编号
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  //// 返回指定level的文件个数
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  //// 返回指定level中所有sstable文件大小的和
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }



  // Set the last sequence number to s.
  //// 获取、设置last sequence，set时不能后退
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  //标记指定的文件编号已经被使用了
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  //// 返回当前log文件编号
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  //就是选取一层需要compact的文件列表，及相关的下层文件列表，记录在Compaction*返回。
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  //人工触发;选取一层需要compact的文件列表，及相关的下层文件列表，记录在Compaction*返回。
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  // 对于所有level>0，遍历文件，找到和下一层文件的重叠数据的最大值(in bytes)
  // 这个就是Version:: GetOverlappingInputs()函数的简单应用
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  // 通过调用NeedCompaction判定是否需要执行Compaction
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  // 获取函数，把所有version的所有level的文件加入到@live中
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  //在指定的version中查找指定key的大概位置。
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

  // TODO begin
  bool SaveImmLastSequence(){ return save_imm_last_sequence_; }
  bool StartImmLastSequence(bool save ){ save_imm_last_sequence_ = save; }
  void SetImmLastSequence( uint64_t seq ){ imm_last_sequence_ = seq; }
  uint64_t ImmLastSequence() const { return imm_last_sequence_; }
  uint64_t ImmLogFileNumber() const { return imm_log_file_number_; }
  void SetImmLogFileNumber( uint64_t fid ){ imm_log_file_number_ = fid; }
  // TODO end

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  //传入的参数是还没有删除的manifest文件
  //当老的manifest文件大于2K的时候,返回false,表示需要生成新的manifest文件;(生成新的文件统一在LogAndApply中完成)
  //如果小于2K的时候,返回true,表示复用老的manifest文件,不生成新的manifest文件;
  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  //该函数依照规则为下次的compaction计算出最适用的level，对于level 0和>0需要分别对待
  void Finalize(Version* v);

  //获取inputs层中所有文件的smallest,largest key
  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  ////PickCompaction就是选取一层需要compact的文件列表，及相关的下层文件列表，记录在Compaction*返回。
  //本函数负责“及相关的下层文件列表”逻辑
  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  //把currentversion保存到*log中，信息包括comparator名字、compaction点和各级sstable文件，函数逻辑很直观。
  //写入到MANIFEST时，先将current version的db元信息保存到一个VersionEdit中，然后在组织成一个log record写入文件；
  Status WriteSnapshot(log::Writer* log);

  //把v加入到versionset中，并设置为current version。并对老的current version执行Uref()。
  //在双向循环链表中的位置在dummy_versions_之前
  void AppendVersion(Version* v);

  Env* const env_; // 操作系统封装
  const std::string dbname_; //构造函数设置;表示的是leveldb的目录
  const Options* const options_;
  TableCache* const table_cache_; // table cache
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;  // manifest log文件编号
  uint64_t manifest_file_number_; // manifest文件编号
  uint64_t last_sequence_;
  uint64_t log_number_;// log编号
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted
  // TODO begin

  uint64_t imm_last_sequence_;
  // 是否保存 imm 转 sst时候的sequence，主要用在 LogAndApply 这个函数当中，用于区分是mior compact 还是 major compact的过程。
  bool save_imm_last_sequence_;
  // imm_last_sequence 所处在的log文件
  uint64_t imm_log_file_number_;
  // TODO end

  // Opened lazily
  WritableFile* descriptor_file_; //menifest文件相关
  log::Writer* descriptor_log_;  //menifest文件相关
  //头节点;不表示具体的Version;为了环形链表操作容易
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  //当前版本
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  // level下一次compaction的开始key，空字符串或者合法的InternalKey
  // 为了尽量均匀 compact 每个 level，所以会将这一次 compact 的 end-key 作为 下一次 compact 的 start-key。
  // compactor_pointer_就保存着每个 level 下一次 compact 的 start-key.
  // 除了 current_外的 Version，并不会做 compact，所以这个值并不保存在 Version 中。
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  // 需要压缩的level
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  // 返回对应层次参与压缩的文件
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  // 获取某一层第i个文件的sst元数据
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  // 本次压缩产生的最大文件大小
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  //是否可以直接把文件从level标记到level+1层;
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  // 判断当前user_key在>=(level+2)层中是否已经存在。主要是用于key的type=deletion时可不可以将该key删除掉。
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;  // 要 compact 的 level
  // 压缩之后最大的文件大小，等于options->max_file_size
  uint64_t max_output_file_size_; // 生成 sstable 的最大 size (kTargetFileSize)
  // 当前需要操作的版本
  Version* input_version_;  // compact 时当前的 Version
  // 版本变化
  VersionEdit edit_;  // 记录 compact 过程中的操作

  // Each compaction reads inputs from "level_" and "level_+1"
  // 记录了参与 compact 的两层文件，是最重要的两个变量
  // inputs_[0]为 level-n 的 sstable 文件信息，
  // inputs_[1]为 level-n+1 的 sstable 文件信息
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // 位于 level-n+2，并且与 compact 的 key-range 有 overlap 的 sstable。
  // 保存 grandparents_是因为 compact 最终会生成一系列 level-n+1 的 sstable， 
  // 而如果生成的 sstable 与 level-n+2 中有过多的 overlap 的话，当 compact
  // level-n+1 时，会产生过多的 merge，为了尽量避免这种情况，compact 过程中 
  // 需要检查与 level-n+2 中产生 overlap 的 size 并与
  // 阈值 kMaxGrandParentOverlapBytes 做比较，
  // 以便提前中止 compact。
  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  // 记录 compact 时 grandparents_中已经 overlap 的 index
  // grandparent下表索引
  size_t grandparent_index_;  // Index in grandparent_starts_
  // 记录是否已经有 key 检查 overlap
  // 如果是第一次检查，发现有 overlap，也不会增加 overlapped_bytes_.
  // (没有看到这样做的意义)
  bool seen_key_;             // Some output key has been seen
  // 记录已经 overlap 的累计 size
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  // level_ptrs_[i]中就记录了 input_version_->levels_[i]中，上一次比较结束的sstable 的容器下标
  // 用于记录某个user_key与>=level+2中每一层不重叠的文件个数
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
