// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/value_log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "db/kv_separate_management.h"
#include "db/garbage_collection.h"


namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class SeparateManagement;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  // 新创建一个快照。
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  //触发人工compaction调用接口;会遍历所有层的sstable
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  //触发人工compaction调用接口;只处理level层的compaction操作
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  // 触发minor compaction操作
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);
  // TODO begin
  Status OutLineGarbageCollection();
  Status GetAllValueLog(std::string dir,std::vector<uint64_t>& logs);
  // TODO end

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  //是人工触发的Compaction，由外部接口调用产生，例如在ceph调用的Compaction都是Manual Compaction
  //实际其内部触发调用的接口是： void DBImpl::CompactRange(const Slice begin, const Slice end)
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence,bool& found_sequence_pos)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  // TODO begin
  static void GarbageCollectionBGWork(void* db);
  void GarbageCollectionBackgroundCall();
  void MaybeScheduleGarbageCollection() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void BackGroundGarbageCollection();
  Status CollectionValueLog(uint64_t fid, uint64_t& last_sequence);
  // TODO end

  static void BGWork(void* db);
  void BackgroundCall();
  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // TODO begin
  bool ParsedValue( Slice kvalue,  Slice key, std::string& value, uint64_t val_size);
  Status GetLsm( const Slice& key,std::string* value);
  // TODO end

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  // 表示是否启用了info_log打印日志，如果启用了后期需要释放内存
  const bool owns_info_log_;
  // 表示是否启用block缓存
  const bool owns_cache_;
  // 数据库名字
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  // table缓存，用于提高性能，缓存了SST的元数据信息
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  // 文件所保护只有一个进程打开db
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;

  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);

  // TODO begin 用于gc回收的过程
  port::CondVar garbage_collection_work_signal_ GUARDED_BY(mutex_);
  // TODO end

  // 表示db是否关闭
  std::atomic<bool> shutting_down_;
  

  // 活跃memtable
  MemTable* mem_;
  // 被压缩的memtabl 也就是不活跃的memtable
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  // 表示是否已经有一个imm_，因为只需要一个线程压缩，也只会有一个imm
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  // wal log句柄
  WritableFile* logfile_;
  // 当前日志编号
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  // TODO begin 换成了下面的 vlog了
   //log::Writer* log_;
  // TODO end
  // 用于采样
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  // 用于批量写，多个写操作
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);
  // 快照，leveldb支持从某个快照读取数据
  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  // 保护某些SST文件不被删除，主要是在CompactMemTable中，因为mem还没有生成sst还没有真正的处理完
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  // 表示后台压缩线程是否已经被调度或者在运行
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  // TODO begin
  // 表示后台gc线程是否已经被调度或者在运行
  bool background_GarbageCollection_scheduled_ GUARDED_BY(mutex_);
  // TODO end
  // 手动压缩句柄
  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);
  // 版本管理
  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);
  // 记录压缩状态信息，用于打印
  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

  // gc 管理
  // TODO begin
  // 判断是否结束gc后台回收线程，在进行快照的时候会对这个值改变为true。
  bool finish_back_garbage_collection_;
  SeparateManagement* garbage_colletion_management_;
  // 当前写入的vlog 的kv对的数量。
  int log_kv_numbers_;
  // wal log句柄
  log::VlogWriter* vlog_;

  // TODO end
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
