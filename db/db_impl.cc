// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>


#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"

#include "db/value_log_reader.h"
#include "db/value_log_writer.h"

#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "db/log_format.h"
#include <iostream>


namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  // 记录压缩产生的数据
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  // 记录最小的seq_num,如果小于等于smallest_snapshot，表示可以删除
  SequenceNumber smallest_snapshot;

  // TODO 记录压缩后将要输出的文件,不一定是有真实存在的文件，有可能是还没来的及创建的文件
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  // 构建sst
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    //8M
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      garbage_collection_work_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      vlog_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      background_GarbageCollection_scheduled_(false),
      finish_back_garbage_collection_(false),
      log_kv_numbers_(0),
      manual_compaction_(nullptr),
      garbage_colletion_management_(new SeparateManagement(raw_options.garbage_collection_threshold) ),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  while(background_GarbageCollection_scheduled_){
      garbage_collection_work_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete vlog_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}
// 新建一个数据库，同时创建一个manifest文件和CURRENT文件，CURRENT文件中存的是manifest文件名
Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  // 设置为 2 ，是因为 下面的manifest就是从 1 开始的。
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);
  // 新建一个manifest 文件
  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    // 将 log_number sequence nextfile 写进去到record。
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

//删除非活跃manifest文件;删除无用sstable文件;等所有无用的文件
void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  // 获取dbname_目录下的所有的文件，并将其放入到 filenames 当中
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      // 筛选应该要删除的文件，主要想要删除 log 和 manifest 文件
      switch (type) {
        case kLogFile: // .log 文件：是之前的文件，并且不是将要用于被压缩的文件
          // TODO begin 所有的log文件都保留下来，等到gc的时候才删除。
          // keep = ((number >= versions_->LogNumber()) ||
          // (number == versions_->PrevLogNumber()));
          // TODO end
          break;
        case kDescriptorFile: // MANIFEST 小于当前的 MANIFEST 编号的可以被删除了
        // manifest 文件创建的时候先会进行一个快照，记录所有的版本信息，后续的版本信息以新增的方式进行迭代
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile: // .ldb 在 vset中的所有的 table文件都不应该删除
          keep = (live.find(number) != live.end());
          break;
        case kTempFile: 
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        // 如果是ldb文件那么有可能在内存中也有，所以要先从内存中删除。
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  // 真正的删除文件
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      //如果不存在Current文件
      //生成全新的manifest和current文件
      // TODO : 新建全新的manifest 前面的数据都没有读进来,都不要了
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  //// 恢复当前version信息
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  // 获取文件列表
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  //返回存在所有Version中的sstable文件名
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  uint64_t max_number = 0;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
        if(number > max_number) max_number = number;
      //删除存在的文件
      expected.erase(number);
      //存储当前已有的日志文件
      //TODO begin
      if (type == kLogFile)
        logs.push_back(number);
      //TODO end
    }
  }
  if (!expected.empty()) {
    //存在文件缺失情况
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  //排序日志文件
  std::sort( logs.begin(), logs.end() );

  assert( logs.size() == 0 || logs[logs.size() - 1] >= versions_->ImmLogFileNumber() );

  //TODO begin
  bool found_sequence_pos = false;
  //int log_numbers = (logs.size() > 2) ? ( logs.size() - 2 ) : ( 0 );
  for(int i = 0; i < logs.size(); ++i){
      if( logs[i] < versions_->ImmLogFileNumber() ) {
          continue;
      }
      Log(options_.info_log, "RecoverLogFile old log: %06llu \n", static_cast<unsigned long long>(logs[i]));
      //重做日志操作
      s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                         &max_sequence, found_sequence_pos);
      if (!s.ok()) {
          return s;
      }
  }


    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(max_number);
  //TODO end

  //使用内存中的max_sequence更新version中的last_sequence_
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

// 恢复内存中的数据，重做日志操作即是将日志中记录的操作读取出来，然后再将读取到的操作重新写入到leveldb中memtable
// logfile 保存的memtable的信息，如果memtable保存为sst之后，会进行删除。
// 压缩的时候会根据这个sequence来进行判断的,在DoCompactionWork会有判断，小于 smallest_sequence 是会被删除的
// max_sequence ： 所有日志访问后，最大的sequence
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence,bool& found_sequence_pos) {
  struct LogReporter : public log::VlogReader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::VlogReader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  uint64_t record_offset = 0;
  int compactions = 0;
  MemTable* mem = nullptr;

  // TODO begin
  uint64_t imm_last_sequence = versions_->ImmLastSequence();
  // TODO end

  while (reader.ReadRecord(&record, &scratch) && status.ok()) { 
    if (record.size() < 20) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    // TODO begin 如果 imm_last_sequence == 0 的话，那么整个说明没有进行一次imm 转sst的情况，
    //  所有的log文件都需要进行回收，因为也有可能是manifest出问题了。
    // 回收编号最大的log文件即可。note
      if( !found_sequence_pos  && imm_last_sequence != 0 ){
        Slice tmp = record;
        tmp.remove_prefix(8);
        uint64_t seq = DecodeFixed64(tmp.data());
        tmp.remove_prefix(8);
        uint64_t kv_numbers = DecodeFixed32(tmp.data());

        // 解析出来的seq不符合要求跳过。恢复时定位seq位置时候一定是要大于或者等于 versions_->LastSequence()
        if( ( seq + kv_numbers - 1 ) < imm_last_sequence  ) {
            record_offset += record.size();
            continue;
        }else if( ( seq + kv_numbers - 1 ) == imm_last_sequence ){
            // open db 落盘过sst，再一次打开db就是这个情况。
            found_sequence_pos = true;
            record_offset += record.size();
            continue;
        }else { // open db 之后没有落盘过sst，然后数据库就关闭了，第二次又恢复的时候就是这个情况
            found_sequence_pos = true;
        }
    }
    // 去除头部信息 crc 和length
    record.remove_prefix(log::vHeaderSize);
    // TODO end
    // 将 读到的 record 放入到 batch中
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem,log_number,record_offset + 8);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }
    // 当memtable 存入的够多的时候，放入到 sst中
    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;

      // TODO begin mem 落盘修改 imm_last_sequence，版本恢复
      versions_->SetImmLastSequence(mem->GetTailSequence());
      versions_->SetImmLogFileNumber(log_number);
      // TODO end

      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
    // 前面已经移除了一个头部了，所以偏移位置要个头部
    record_offset += record.size() + log::vHeaderSize ;
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(vlog_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      vlog_ = new log::VlogWriter(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {

      // TODO begin mem 落盘修改 imm_last_sequence，版本恢复
      versions_->SetImmLastSequence(mem->GetTailSequence());
      versions_->SetImmLogFileNumber(log_number);
      // TODO end

      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}
  // 将 mem 转换成 sst 并写入零层，同时在 edit中新增这个文件信息
  //调用PickLevelForMemTableOutput为新的SST文件挑选level，最低放到level 0，最高放到level 2
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);

  //遍历Memtable的迭代器;保证key的顺序性
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    //调用BuildTable完成Memtable到SST文件的转换，并将该sst加入到缓存当中。
    //meta.number是文件要使用的number，最终生成的sst文件名为$number.ldb，比如000012.ldb。
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      //调用PickLevelForMemTableOutput为新的SST文件挑选level，最低放到level 0，最高放到level 2
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    //这里还会修改 VersionEdit，添加一个文件记录。包含文件的number、size、最大和最小key。
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  //更新第level层的统计信息，level是Step2选出的level
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}
// CompactMemTable会把imm_转为一个SST文件，放到L0~L2中的一层
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  // 调用WriteLevel0Table把Memtable落盘，并在VersionEdit存储新增的文件信息。
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  // 将imm_ 转换成sst 并把其加入到 table_cache_ 的缓存中，
  // 调用PickLevelForMemTableOutput为新的SST文件挑选level，
  // 同时在 edit 中新增这个文件信息
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  //调用LogAndApply把VersionEdit的更新应用到LevelDB的版本库。
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    // TODO begin
    //构建新版本，并将其加入到 version_当中
    versions_->StartImmLastSequence(true);
    versions_->SetImmLastSequence(imm_->GetTailSequence());
    versions_->SetImmLogFileNumber(imm_->GetLogFileNumber());
    s = versions_->LogAndApply(&edit, &mutex_);
    versions_->StartImmLastSequence(false);
    // TODO end
  }

  //调用RemoveObsoleteFiles删除废弃的文件(比如Memtable的预写日志文件）
  //RemoveObsoleteFiles会检查数据库下所有的文件，删除废弃的文件。
  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
    // TODO begin
    // garbage_collection_work_signal_.SignalAll();
    // TODO end
  }
}
// 可能调度后台线程进行压缩
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
    // 先检查线程是否已经被调度了，如果已经被调度了，就直接退出。
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    // 如果DB已经被关闭，那么就不调度了。
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
    // 如果后台线程出错，也不调度。
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
    //如果imm_不为空，调度
    //如果设置了手动合并，调度
    //如果版本系统认为需要合并，调度。
  } else {
    //设置调度变量,通过detach线程调度;detach线程即使主线程退出,依然可以正常执行完成
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

// TODO begin 调度垃圾回收的相关的函数 不需要锁，仅仅是追加的方式。
// 获得db库中所有的log文件，将其放入到vector中
    Status DBImpl::GetAllValueLog(std::string dir,std::vector<uint64_t>& logs){
        logs.clear();
        std::vector<std::string> filenames;
        // 获取文件列表
        Status s = env_->GetChildren(dir, &filenames);
        if (!s.ok()) {
            return s;
        }
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                //存储当前已有的日志文件
                if (type == kLogFile)
                    logs.push_back(number);
            }
        }
        return s;
    }

// 手动进行离线回收、
// 1. 如果管理类 separate_management中有的话，那么就按照这个类中的map的信息进行回收，主要用于删除快照后的一个回收
// 2. 对db目录下的文件所有的文件进行回收，主要针对于open的时候。主线程中使用。
// 返回的 status 如果是不ok的说明回收的时候出现一个log文件是有问题的。
Status DBImpl::OutLineGarbageCollection(){
     MutexLock l(&mutex_);
     Status s;
    // map 中保存了文件的信息，那么就采用map来指导回收，否则对db下所有的log文件进行回收
    if( !garbage_colletion_management_->EmptyMap() ){
        garbage_colletion_management_->ColletionMap();
        uint64_t last_sequence = versions_->LastSequence();
        garbage_colletion_management_->ConvertQueue(last_sequence);
        versions_->SetLastSequence(last_sequence);
        MaybeScheduleGarbageCollection();
        return Status();
    }
    return s;
}
// 读取回收一个log文件，不加锁
// next_sequence : 只有在open的时候才会返回需要修改的值，在线gc是不需要的。
// next_sequence 指的是第一个没有用到的sequence
Status DBImpl::CollectionValueLog(uint64_t fid, uint64_t& next_sequence){

    struct LogReporter : public log::VlogReader::Reporter {
        Status* status;
        void Corruption(size_t bytes, const Status& s) override {
            if (this->status->ok()) *this->status = s;
        }
    };
    LogReporter report;
    std::string logName = LogFileName(dbname_, fid);
    SequentialFile* lfile;
    Status status = env_->NewSequentialFile(logName, &lfile);
    if (!status.ok()) {
        Log(options_.info_log, "Garbage Collection Open file error: %s", status.ToString().c_str());
        return status;
    }
    log::VlogReader reader(lfile,&report,true,0);

    Slice record;
    std::string scratch;
    // record_offset 每条record 相对文本开头的偏移。
    uint64_t record_offset = 0;
    uint64_t size_offset = 0;
    WriteOptions opt(options_.background_garbage_collection_separate_);
    WriteBatch batch(opt.separate_threshold);
    batch.setGarbageColletion(true);
    WriteBatchInternal::SetSequence(&batch, next_sequence);
    while( reader.ReadRecord(&record,&scratch) ){
        const char* head_record_ptr = record.data();
        record.remove_prefix(log::vHeaderSize + log::wHeaderSize);

        while( record.size() > 0 ){
            const char* head_kv_ptr = record.data();
            // kv对在文本中的偏移
            uint64_t kv_offset = record_offset + head_kv_ptr - head_record_ptr;
            ValueType type = static_cast<ValueType>(record[0]);
            record.remove_prefix(1);
            Slice key;
            Slice value;
            std::string get_value;

            GetLengthPrefixedSlice(&record,&key);
            if( type != kTypeDeletion ){
                GetLengthPrefixedSlice(&record,&value);
            }
            // 需要抛弃的值主要有以下三种情况：0，1，2
            // 0. log 中不是 kv 分离的都抛弃
            if(type != kTypeSeparate){
                continue;
            }

            status = this->GetLsm(key,&get_value);
            // 1. 从LSM tree 中找不到值，说明这个值被删除了，log中要丢弃
            // 2. 找到了值，但是最新值不是kv分离的情况，所以也可以抛弃
            if (status.IsNotFound() || !status.IsSeparate() ) {
                continue;
            }
            // 读取错误，整个文件都不继续进行回收了
            if( !status.ok() ){

                std::cout<<"read the file error "<<std::endl;
                return status;
            }
            // 判断是否要丢弃旧值
            Slice get_slice(get_value);
            uint64_t lsm_fid;
            uint64_t lsm_offset;

            GetVarint64(&get_slice,&lsm_fid);
            GetVarint64(&get_slice,&lsm_offset);
            if( fid == lsm_fid && lsm_offset == kv_offset ){
                batch.Put(key, value);
                ++next_sequence;
                if( kv_offset - size_offset > config::gWriteBatchSize ){
                    Write(opt, &batch);
                    batch.Clear();
                    batch.setGarbageColletion(true);
                    WriteBatchInternal::SetSequence(&batch, next_sequence);
                    uint64_t kv_size;
                    GetVarint64(&get_slice,&kv_size);
                    size_offset = kv_offset + kv_size;
                }
            }

        }

        record_offset += record.data() - head_record_ptr;
    }
    Write(opt, &batch);
    status = env_->RemoveFile(logName);
    if( status.ok() ){
        garbage_colletion_management_->RemoveFileFromMap(fid);
    }
    return status;
}

// 回收任务
void DBImpl::BackGroundGarbageCollection(){
    uint64_t fid;
    uint64_t last_sequence;
    while( true){
        std::cout<<"---------- gc is running --------"<<std::endl;
        if( !garbage_colletion_management_->GetGarbageCollectionQueue(fid,last_sequence) ){
            return;
        }
        // 在线的gc回收的sequence是要提前就分配好的。
        CollectionValueLog(fid,last_sequence);
    }
}

// 可能调度后台线程进行压缩
void DBImpl::MaybeScheduleGarbageCollection() {
    mutex_.AssertHeld();
    if (background_GarbageCollection_scheduled_) {
        // Already scheduled
        // 先检查线程是否已经被调度了，如果已经被调度了，就直接退出。
    } else if (shutting_down_.load(std::memory_order_acquire)) {
        // DB is being deleted; no more background compactions
        // 如果DB已经被关闭，那么就不调度了。
    } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
        // 如果后台线程出错，也不调度。
    } else {
        //设置调度变量,通过detach线程调度;detach线程即使主线程退出,依然可以正常执行完成
        background_GarbageCollection_scheduled_ = true;
        env_->ScheduleForGarbageCollection(&DBImpl::GarbageCollectionBGWork, this);
    }
}
// 后台gc线程中执行的任务
void DBImpl::GarbageCollectionBGWork(void* db) {
    reinterpret_cast<DBImpl*>(db)->GarbageCollectionBackgroundCall();
}

void DBImpl::GarbageCollectionBackgroundCall() {
    assert(background_GarbageCollection_scheduled_);
    if (shutting_down_.load(std::memory_order_acquire)) {
        // No more background work when shutting down.
        // // 如果DB已经被关闭，那么就不调度了。
    } else if (!bg_error_.ok()) {
        // No more background work after a background error.
        // 如果后台线程出错，也不调度。
    } else {
        // 开始后台GC回收线程
        BackGroundGarbageCollection();
    }

    background_GarbageCollection_scheduled_ = false;
    //再调用 MaybeScheduleGarbageCollection 检查是否需要再次调度
    // MaybeScheduleGarbageCollection();
    garbage_collection_work_signal_.SignalAll();
}

// TODO end
// 这个函数在后台线程中跑
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}
// 后台压缩线程调度的任务
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
    // // 如果DB已经被关闭，那么就不调度了。
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
    // 如果后台线程出错，也不调度。
  } else {
    // 开始后台压缩
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  //再调用MaybeScheduleCompaction检查是否需要再次调度。最后唤醒等待条件变量的线程。比如在MakeRoomForWrite中Wait的线程。
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}
// 后台压缩的任务。
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  // minor compaction
  //完成imm_的落盘，CompactMemTable会把imm_转为一个SST文件，放到L0~L2中的一层
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  //选择要合并的level，以及level层和level+1层要合并的文件，结果放到Compaction中。
  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    //是人工触发的Compaction，由外部接口调用产生
    //在 Manual Compaction 中会指定的 begin 和 end，
    //它将会一个level 一个level 的分次的Compact 所有level 中与begin 和 end 有重叠（overlap）的 sst 文件。
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    // major 压缩，采用自动选取层数和文件的方式 生成一个压缩类 
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
    //如果c为null，说明不需要合并
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    //调用IsTrivialMove判断是否可以直接把level层的文件移动到level+1层
    //可以(优化版本major)
    assert(c->num_input_files(0) == 1);
    // input中只有一个文件
    FileMetaData* f = c->input(0, 0);
    //直接把level层的文件移动到level+1层;删除level层文件,新增level+1层文件
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    //调用LogAndApply把VersionEdit的更新应用到LevelDB的版本库。
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    //调用IsTrivialMove判断是否可以直接把level层的文件移动到level+1层
    //不可以
    //走正常的合并流程(major)，调用DoCompactionWork完成文件合并
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    // TODO begin conmpact 后需要考虑是否将 value log 文件进行 gc回收，如果需要将其加入到回收任务队列中。
    // 不进行后台的gc回收，那么也不更新待分配sequence的log了。
    if(!finish_back_garbage_collection_){
        garbage_colletion_management_->UpdateQueue(versions_->ImmLogFileNumber() );
    }
    // TODO end
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  } 
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}
// 用于初始化构建SST文件的，生成build
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  // 这个outfile 不用判空，外面的函数已经判断过了。一般情况下builder 和 outfile 是同时存在的
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}
// compact的文件超过了大小(默认2M)，则关闭当前打开的sstable，持久化到磁盘。
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);
  // outputs 最后一个位置放的就是最新正在整合的sst文件，所以要把它持久化到磁盘中。
  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  // input 就是指向 所要合并的key的迭代器
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    // builder 需要将按照一定的格式进行放置到磁盘中，所以要finish，这一步已经进行了持久化了
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  // 要将 builder删除，后面会新创建一个builder ，将对应的是另外一个sst。
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  // 相应的写句柄也要删除掉，下一次打开的是不同的文件。
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    // 验证以下看 文件是否真的写入了磁盘当中。
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}
// 将结果记录到 version，并更新versionedit，最后持久化
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  // 将刚刚压缩的文件全部都标记成要删除
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  // TODO : 每一个 outputs 对应的是一个新创建的 sst的文件，为什么不应 FileMetaData ，应该是不想记录这么多信息
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

//完成文件的归并操作
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  //获取最旧的仍然在被使用的快照(snapshot)
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  //input用于遍历compact里所有文件的key
  //MakeInputIterator返回的是　MergeIterator　，包含了对c->inputs_两层文件的迭代器。
  //每次调用Next返回下一个最小的key。
  // 这里用　MergeIterator　的好处就是每次向前走一步后，都会选择最小的key作为next
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  //把Iterator指向第一个key
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  //current_user_key记录上一个key，初始为空。
  std::string current_user_key;
  //has_current_user_key表示是否有上一个key
  bool has_current_user_key = false;
  //last_sequence_for_key记录上一个key的sequence，初始为kMaxSequenceNumber，此时表示没有上一个key。
  //注意相同key情况下的排序规则是降序;所以第一个是最新的
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    //如果imm_(即Immutable Memtable）不为空，优先处理imm_落盘
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    // ShouldStopBefor比较当前文件已经插入的key和level+2层重叠的文件的总size是否超过阈值，
    // 如果超过，就结束文件。
    // 避免这个新的level+1层文件和level+2层重叠的文件数太多，
    // 导致以后该level+1层文件合并到level+2层时牵扯到太多level+2层文件
    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      //调用FinishCompactionOutputFile填充文件尾部的数据，并结束文件，一个新的SST文件就诞生了。
      // 完成落盘的任务
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    //找出不需要的旧版本internal key
    bool drop = false;
    //解析产生user key，如果解析失败，那么不会对这个key进行处理，一会儿会被直接插入到新SST文件中。
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      // 如果解析成功，判断是否已经有current_user_key了，如果没有current_user_key，
      // 那么这个internal key就是当前user key的第一个internal key，
      // 因为internal key越新排序越靠前，所以第一个internal key也是最新的。
      // user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0
      // 这个条件就是为了当要删除key的时候，在进入下面的else if 后，还要删除相应的前面插入的 相同的key
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion && // TODO key -value 是在这里被删除的，如果level + 2层有呢
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // 这里采用  level + 2层 是因为 现在正在遍历的就是 level 和 level + 1 层的数据，只有当level + 2层
        // 都没有的时候，说明可以删除了这个key值了。
        // 采用这个条件 ikey.sequence <= compact->smallest_snapshot 是只有当 key的序号小于compact->smallest_snapshot
        // 这个时候才可以删除
        //类型是kTypeDeletion，即删除
        //当前interanl key的sequence小于smallest_snapshot，表示是压缩之前的key。
        //判断level+2及以上的层是否有这个user key;如果没有的话，就可以直接删除;
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
    #if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
    #endif

    //如果drop为true，那么直接跳过本步骤，这个key就被丢弃了
    if (!drop) {
      // Open output file if necessary
      //如果builder为空，新建一个SST文件用于输出
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      // 记录当前SST的最大值和最小值,只有刚刚建立新的sst时候才会有这个记录最小的key值
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      //然后调用Add把当前internal key加入到新文件中 
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      //然后判断新文件的size是否超过了阈值，如果超过了就调用FinishCompactionOutputFile结束当前文件。
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    } else{// TODO begin
        //fid ,key valuesize ,
        Slice drop_value = input->value();
        //  获得type类型
        if( ikey.type == kTypeSeparate ){
            uint64_t fid = 0;
            uint64_t offset = 0;
            uint64_t size = 0;
            GetVarint64(&drop_value,&fid);
            GetVarint64(&drop_value,&offset);
            GetVarint64(&drop_value,&size);
            mutex_.Lock();
            garbage_colletion_management_->UpdateMap(fid,size);
            mutex_.Unlock();
        }
    }// TODO end


    input->Next();
  }

  // 结束多路合并逻辑;主要是将剩余key/value输出
  // TODO 为啥不在 把builder剩余的 字节刷入到磁盘中呢，反而要先判断是不是关闭的。
  // 因为主线程关了 后台运行也是无效的运行
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  //更新统计信息和版本信息
  CompactionStats stats;
  // 记录压缩所用的时间
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  // 最后压缩合并好的文件会放在 level() + 1 层。
  stats_[compact->compaction->level() + 1].Add(stats);
  // 将compact的内容更新version。
  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::GetLsm( const Slice& key,std::string* value){
    MutexLock l(&mutex_);
    ReadOptions options;
    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = versions_->current();
    if( !this->snapshots_.empty() ){
        options.snapshot = this->snapshots_.oldest();
    }
    SequenceNumber snapshot;
    if (options.snapshot != nullptr) {
        snapshot =
                static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
    } else {
        snapshot = versions_->LastSequence();
    }
    Status s;
    mem->Ref();
    // imm 不一定存在，但是 mem 是一定存在的。
    if (imm != nullptr) imm->Ref();
    current->Ref(); // Version 读引用计数增一
    Version::GetStats stats;
    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();
        // First look in the memtable, then in the immutable memtable (if any).
        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &s )) {
            // Done
        } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
            // Done
        } else {
            //在Version中查找是否包含指定key值
            s = current->Get(options, lkey, value, &stats);
        }
        mutex_.Lock();
    }
    mem->Unref();
    if (imm != nullptr) imm->Unref();
    current->Unref(); //Version 读引用计数减一
    return s;
}

// 先查 memtable，再查 imm，再查外存的 sstables。查找 key 值对应的 value。
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  // TODO 这里为什么要放个快照在这里呢？
  // 因为可以通过GetSnapshot 函数 设置 snapshotImpl 的里面的sequence
  // 达到大于这个sequence的key是不看的，只看旧值。
  // 参考： https://zhuanlan.zhihu.com/p/149796078
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  // imm 不一定存在，但是 mem 是一定存在的。
  if (imm != nullptr) imm->Ref();
  current->Ref(); // Version 读引用计数增一
  bool have_stat_update = false;
  Version::GetStats stats;
  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s )) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      //在Version中查找是否包含指定key值
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }
  // 如果在 sst中找到 这个key的话，就要判断是否要进行压缩了
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref(); //Version 读引用计数减一

  // TODO begin
  // 需要从log文本中读取value 的值。现在的 value = fileNumber + offset + log中value的长度。（fileNumber(8字节编码) + offset( 4 字节编码)）
  // offset ：
  if( s.ok() && s.IsSeparate() ){

      struct LogReporter : public log::VlogReader::Reporter {
          Status* status;
          void Corruption(size_t bytes, const Status& s) override {
              if (this->status->ok()) *this->status = s;
          }
      };
      LogReporter reporter;
      Slice input(*value);
      uint64_t fid;
      uint64_t offset;
      uint64_t val_size;
      size_t key_size = key.size();
      GetVarint64(&input,&fid);
      GetVarint64(&input,&offset);
      GetVarint64(&input,&val_size);
      uint64_t encoded_len = 1 + VarintLength(key_size) + key.size() + VarintLength(val_size) + val_size;
      // 开始从文件中读取出具体的value 的值。
      std::string fname = LogFileName(dbname_, fid);
      RandomAccessFile* file;
      s = env_->NewRandomAccessFile(fname,&file);
      if (!s.ok()) {
          return s;
      }
      // TODO 保留现场 还需要一个长度信息 从offset地方读取多长的字符,写入的时候就要知道
      log::VlogReader vReader(file,&reporter,true,0);
      Slice k_value;
      Slice ret_value;
      char* scratch = new char[encoded_len];
      if( vReader.ReadValue(offset,encoded_len, &k_value, scratch) ){
          // 解析出来value的值
          value->clear();
          if ( !ParsedValue(k_value, key, *value,val_size) ){
              s = s.Corruption("has key but dont have value in valuelog");
          }
      }else{
          s = s.Corruption("read valuelog error");
      }

      delete file;
      file = nullptr;
  }
  // TODO end
  return s;
}
// TODO begin
bool DBImpl::ParsedValue( Slice k_value,  Slice key, std::string& value, uint64_t val_size){
    Slice input = k_value;
    if( input[0] != kTypeSeparate ) {
        return false;
    }
    input.remove_prefix(1);
    Slice log_key;
    Slice log_value;
    if(GetLengthPrefixedSlice(&input,&log_key)
        && log_key == key
        && GetLengthPrefixedSlice(&input,&log_value)
        && log_value.size() == val_size ){
        value = log_value.ToString();
        return true;
    }else{
        return false;
    }

}
// TODO end
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  // TODO begin 建立快照 对快照之后的信息不进行回收了。
  finish_back_garbage_collection_ = true;
  // TODO end
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);

  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
  // TODO begin 没有快照了重新进行后台回收
  if( snapshots_.empty() ){
      finish_back_garbage_collection_ = false;
  }
  // TODO end
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  //生成writer对象,带写锁
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  // leveldb中，在面对并发写入时，做了一个处理的优化。
  // 在同一个时刻，只允许一个写入操作将内容写入到日志文件以及内存数据库中。
  // 为了在写入进程较多的情况下，减少日志文件的小写入，增加整体的写入性能，
  // leveldb将一些“小写入”合并成一个“大写入”。
  // 使第一个发起写入的线程作为唯一写者，负责发起写合并、批量化处理来自其他线程的写入操作。

  // 获取写锁,在写锁的保护下加入写队列中
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  // 没有做完
  // 如果当前 Writer 并非队列头部
  // 则 w.cv.Wait() 进入等待信号量状态
  // 在 ready->cv.Signal(); 中被唤醒
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  //等待结束后，如果已经做完(被其他写操作合并)，则直接返回
  if (w.done) {
    return w.status;
  }

  // MakeRoomForWrite 方法会判断 Memtable 是不是满了、当前 L0 的 SSTable 是不是太多了，
  // 从而发起 Compaction 或者限速。
  // May temporarily unlock and wait. 这种耗时的操作期间会释放锁，
  // 这也正是其他线程进入 writers_ 队列攒批量的契机。
  // true 表示强制把Memtable转存为Immutable Memtable。
  Status status = MakeRoomForWrite(updates == nullptr);
  // LastSequence 返回最后一个已用过的 sequence
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    // 里面逻辑并没有使用 last_writer 参数,所以可以理解这个是一个输出参数,表示最后一个被合并的writer指针
    // 返回的write_batch 有可能有两种情况，一种是 writers_.front(); 一种是tmp_batch_
    // last_writer 用来返回合并到的最后一个 writeBatch

    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    // TODO begin gc中的batch全部都是设置好的。此时是不需要设置的。
    if( !write_batch->IsGarbageColletion() ){
        // 判断是否需要进行垃圾回收，如需要，腾出一块sequence的区域,触发垃圾回收将在makeroomforwrite当中。
        // 先进行判断是否要进行gc后台回收，如果建立了快照的话finish_back_garbage_collection_就是true，
        // 此时不进行sequence分配了。
        //
        if( !finish_back_garbage_collection_
            && garbage_colletion_management_->ConvertQueue(last_sequence) ){
            // 尝试调度gc回收线程进行回收。
            MaybeScheduleGarbageCollection();
        }
        //SetSequence在write_batch中写入本次的sequence
        WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
        // Count返回write_batch中的key-value个数
        last_sequence += WriteBatchInternal::Count(write_batch);
    }
        log_kv_numbers_ += WriteBatchInternal::Count(write_batch);
        // TODO 这里设置last_sequence  是为了照顾离线回收的时候，在map存在的时候需要调用 ConvertQueue 给回收任务分配sequence。
        // TODO 针对多线程调用put的时候，为了避免给gc回收的时候分配的sequence重叠。
        versions_->SetLastSequence(last_sequence);
    // TODO end

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      // 这里解锁是为了让其他非Write的写线程可以拿到mutex，
      // 其他Write线程即使拿到了锁，也会发现自己的Writer不在队列头，所以不会执行
      mutex_.Unlock();
      // TODO begin 修改为先写入vlog再写入memtable
      // 写vlog日志 offset 表示这个 write_batch 在vlog中的偏移地址。
      uint64_t offset = 0;
      status = vlog_->AddRecord(WriteBatchInternal::Contents(write_batch),offset);
      bool sync_error = false;
      if (status.ok() && options.sync) {
        // 进行同步落盘的作用
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }

      //把合并的writer写入memtable
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_, logfile_number_, offset);
      }
      // TODO end
      // 重新加锁
      mutex_.Lock();
      if (sync_error) {
        // 如果日志 Sync() 失败，那么会认为这个库的日志已不一致，不应该有任何新的写入，避免日志文件进一步乱掉。
        // RecordBackgroundError 会将错误记录到 bg_error_ 字段中，并唤醒 background_work_finished_signal_ 用于向后台任务周知此错误。
        // 一旦设置 bg_error_，就认为这个库进入了不可恢复的错误状态中，停止一切后续的新写入操作。
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    //// write_batch有可能是使用tmp_batch_来返回的，写入成功后要把tmp_batch_情况，以便下次写入
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    // 设置Sequence
    versions_->SetLastSequence(last_sequence);
  }
  // 将已经合并的 writeBatch 进行删除。
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    //把遍历到的Writer出队，并设置status，把done设置为true，并Signal唤醒该线程。被唤醒的线程拿到锁后，就会发现自己的Writer已经被完成了，直接返回。
    //显然，当前线程不需要被Signal，也不需要同步状态，所以上面的操作跳过了当前线程的任务.
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    //从队列头开始遍历检查Writer，直到碰到last_writer。因为这个是最后一个合并写的writer
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  // 最后检查任务队列writers_是否为空，如果不为空，就唤醒队头的Writer对应的线程，开始下一轮写入。
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  //最后返回status，Writer主流程到此结束，返回时MutexLock的析构函数会触发mutex的解锁操作，释放锁。锁被释放后，其他Write线程就可以拿到锁启动了。
  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
// 返回合并之后的 WriteBatch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  // first分别指向第一个Writer和第一个Writer的WriteBatch
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  // 如果first batch的size小于等于2^17，就调低max_size; 2^17相当于128K; 2^20相当于1M
  // 作者解释这里是为了耗时考虑，避免小size的写操作被延迟太久。
  // 猜测作者这样考虑的原因是通常一个数据库的key-value的size都是比较类似的，如果第一个batch的size就比较小，那么后续的batch大概率也很小。
  // 如果还是按照原来的max_size来实现，可能一次合并大量的写操作。虽然这样吞吐量上去了，但是写操作的延时就上升了。
  // 这里是在吞吐量和延时上做的一个平衡。
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ////从第二个开始遍历（第一个已经在result中了）
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    // TODO begin 写队列中如果碰到是gc的write_batch 停止合并。
    if ( w->sync && !first->sync
        || first->batch->IsGarbageColletion()
        || w->batch->IsGarbageColletion()  ) {
      // 当前的Writer要求 Sync ，而第一个Writer不要求Sync，两个的磁盘写入策略不一致。不做合并操作
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }
    // TODO end
    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) { 
        // result的size已经超过max_size。
        // Do not make batch too big
        break;
      }

      /**
       * 让reuslt指向tmp_batch_，并把first->batch合并到result中，
       * 这段逻辑只要是writers_中有任意一个(不包含第一个）可以合并的Writer就会执行且只执行一次
       * ，反之不会执行。
       * 写简单一点的话，这部分逻辑可以放到循环体的外面，直接把第一个Writer合并到result中。为什么没有这样做呢？
       * 因为可能出现没有Writer可以合并的情况，即最终结果只包含第一个Writer。
       * 这种情况下作者会直接返回first ->batch，一次合并操作都不需要执行，性能高一点。
       */
      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert( WriteBatchInternal::Count(result) == 0 );
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    ////把last_writer指向新加入的Writer
    *last_writer = w;
  }

  //writers_已经遍历完成。
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
// MakeRoomForWrite 方法会判断 Memtable 是不是满了、当前 L0 的 SSTable 是不是太多了，
// 从而发起 Compaction 或者限速。
// 判断是否需要进行压缩,一定会腾出memtable的，因为这样才有空间去写。
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  // TODO begin 判断是否需要新建一个log文件
    if( logfile_->GetSize() > options_.max_value_log_size ){
        uint64_t new_log_number = versions_->NewFileNumber();
        WritableFile* lfile = nullptr;
        s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
        if (!s.ok()) {
            // Avoid chewing through file number space in a tight loop.
            // 前面创建 lfile 的时候，调用了versions_->NewFileNumber(); 导致next_file_number_ + 1 了
            // 所以这里就是让 next_file_number_ --
            versions_->ReuseFileNumber(new_log_number);
        }
        garbage_colletion_management_->WriteFileMap(logfile_number_,log_kv_numbers_,logfile_->GetSize());
        log_kv_numbers_ = 0;
        delete vlog_;
        delete logfile_;
        logfile_ = lfile;
        logfile_number_ = new_log_number;
        vlog_ = new log::VlogWriter(lfile);
    }
  // TODO end
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      // 如果上一次产生了异常,按照异常返回
      s = bg_error_;
      break;// 允许延迟，而且第0层文件个数太多了，超过了触发值了，这个时候就要将延后将cpu给到压缩的进程
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // allow_delay 只延时一次
      // 当L0文件数超过了kL0_SlowdownWritesTrigger,将进行写入延迟处理

      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force && // 如果能延迟，但是mem又有充足的空间，那么直接退出
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // 如果memtable没有写满,正常返回
      // 包括调用了后台的压缩函数后，也是通过这个函数进行返回的。
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) { // 已经有一个mimior的压缩线程了
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      // 不可写memtable存在,说明上一次的compacted还没有做完,因此进行等待状态,不能进行写出操作
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      // kL0_StopWritesTrigger,将进行写入等待状态,不能进行写出操作
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // 进入最后逻辑
      // 将当前memtable冻结成 Immutable MemTable，并创建一个新的 MemTable 对象写入数据
      // 并且通过调用MaybeScheduleCompaction启动一个 Detach 线程将 ImMemTable 合并到 SSTable 中，这个过程被称为 Minor Compaction
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      // TODO begin 当lognumber 大于 max_value_log_size 新建一个lognumber。
      // mem_ 中记录 imm_last_sequence（记得也是 mem_的尾部位置 ） 所在的log文件。
      mem_->SetLogFileNumber(logfile_number_);

      // TODO end
      // 写入 memtable 的 offset。

      imm_ = mem_;

      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      // 修改这个也是为了可以下一次循环
      force = false;  // Do not force another compaction if have room
      // 如果这一次没成功，下一次循环直接就会进入到  else if (imm_ != nullptr) 这个环节，一直等待被处理
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch(opt.separate_threshold);
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();

  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  // 返回 false ,表示复用老的manifest文件,不生成新的manifest文件,后面需要进行落盘;
  // true 的话表示要把版本写入到新的manifest里面了
  Status s = impl->Recover(&edit, &save_manifest);

  // TODO begin
  std::vector<uint64_t> logs;
  s = impl->GetAllValueLog(dbname,logs);
  sort(logs.begin(),logs.end());
  // TODO end

  // 如果 mem 是空的，那么新创建一个新的memtable。
  // 这里应该是用了新的日志，旧的日志是通过 recover 把数据放到了memtable中了，所以不需要以前的数据了
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      // TODO begin
      impl->vlog_ = new log::VlogWriter(lfile);
      // TODO end
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  //这里会通过LogAndApply完成manifest文件最终落盘;对manifest文件的更新也在这一步，记录版本的迭代的信息
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);

    // TODO begin 这里也要设置 imm_last_sequence 设置到新的maifest当中，不然下次就没有值了。
    // 就是全盘恢复了。
    impl->versions_->StartImmLastSequence(true);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    impl->versions_->StartImmLastSequence(false);
    // TODO end
  }
  //这里完成删除无用文件的逻辑
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }

  // TODO beigin 开始全盘的回收。

  if( s.ok() && impl->options_.start_garbage_collection ){
      if( s.ok() ){
          int size = logs.size();
          for( int i = 0; i < size ; i++){
              uint64_t fid = logs[i];
              uint64_t next_sequence = impl->versions_->LastSequence() + 1;
              std::cout<<" collection file : "<<fid<<std::endl;
              impl->mutex_.Unlock();
              Status stmp = impl->CollectionValueLog( fid,next_sequence );
              impl->mutex_.Lock();
              if( !stmp.ok() ) s = stmp;
              impl->versions_->SetLastSequence(next_sequence - 1);
          }
      }
  }
  impl->mutex_.Unlock();
    // TODO end
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
