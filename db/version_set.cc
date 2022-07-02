// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0; //10M
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

//Unref 中的delete this会触发析构函数,将自己从VersionSet中踢出去
Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

//利用二分查找,找到key存在的文件下标编号,key <= ->largest 的值
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}
// user_key 在文件 f之后
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}
// user_key 在文件 f 之前
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}
// 找到 有sst的范围 和 [ smallest_user_key , largest_user_key ]有重叠，找到一个就返回。
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  // level = 0 的时候 
  if (!disjoint_sorted_files) {
    // Need to check against all files
    // 乱序、可能相交的文件集合，依次查找
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  //有序&互不相交，直接二分查找
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    //不存在比smallest_user_key小的ke
    // beginning of range is after all files, so no overlap.
    return false;
  }

  ////保证在largest_user_key之后
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
// LevelFileNumIterator是对leveldb中level > 0的SST文件的迭代器
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

//函数NewConcatenatingIterator()直接返回一个TwoLevelIterator对象
// 这个二级指针 LevelFileNumIterator(vset_->icmp_, &files_[level]) : 同一层的不同文件指针
// 第二级的指针是: 指向具体的文件的指针
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  //对于level=0级别的sstable文件，直接装入cache，level0的sstable文件可能有重合，需要merge。
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  //对于level>0级别的sstable文件，lazy open机制，它们不会有重叠。
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
// TODO begin
    enum SaverSeparate {
        kNoSeparate,
        kSeparate
    };
// TODO end
struct Saver {
  // TODO begin
  SaverSeparate separate = kNoSeparate;
  // TODO end
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue || parsed_key.type == kTypeSeparate) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
        // TODO begin
        s->separate =  ( parsed_key.type == kTypeSeparate ) ? kSeparate : kNoSeparate;
        // TODO end
      }
    }
  }
}
// 文件编号越大，那么说明越新
static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

//在lsm-tree中查找指定key的具体的查找方法
// 遍历每一层 找到用户具体的key的值，具体的匹配的方法是在 func中的
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  // 判断待查找key是否在level 0 sstable范围内
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    //在level0中查找,如果找到直接返回
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      // 如果匹配成功那么就返回
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  // 在level > 1的层中查找user_key
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    // 利用二分查找,找到key存在的文件下标编号
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        //在具体sstable文件中查找是否包含该key;如果找到返回结果
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    // 找到之后一些相应的数据是放在这里面的，比如key对应的value，找到的的数据是删除还是显示找到
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    //在具体sstable中查找key的函数
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      // TODO ： 这里到底是干啥的，没看懂，虽然和后面的压缩是有关系的。
      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        // Get不止seek了一个文件，只记录第一个文件到stat并返回
        // Stat表明在指定key range查找key时，都要先seek此文件，才能在后续的sstable文件中找到key
        // 对所要查找的key 第0层的中所有包含这个key的sst的第一个sst中如果没有找到的话，
        // 那么这个 seek_file 就会记录这个值。就是说
        // 如果这个文件被访问的频次（没有找到）太多的话就需要压缩的。
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      //在VersionSet中存储的table_cache_查找
      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      // TODO begin
      if( state->saver.separate == kSeparate ){
          state->s.SetSeparate();
      } else{
          state->s.SetNoSeparate();
      }
      // TODO end

      // 查找的过程中出现了问题，那么就不在进行查找了。
      // TODO 但是 state->found = true; 为什么？？？
      if (!state->s.ok()) {
        state->found = true;
        //返回false表示不在继续查找
        return false;
      }
      // 只有当没有找到的时候才返回true。
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value; // 找到的话 返回的value就放在这里面。
  // 遍历每一层的 level 上的文件进行查找
  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      //记录待compaction的文件元数据以及level信息
      ////如果allowed_seeks降到0，则记录该文件需要compact了
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}
// 记录对key的访问的频次，然后决定是否要进行compact,
// 一个key在两个sst中找到了，说明有合并的可能性，在找到key的文件中，f->allowed_seeks--;
// 如果某一个文件被访问的次数太多,那么就需要考虑进行compact。
bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  //如果引用计数为0,表示不在被使用;
  //delete this会触发调用析构函数;析构函数中会完成VersionSet中元素的踢出
  if (refs_ == 0) {
    delete this;
  }
}
// OverlapInLevel函数检查 [smallest_user_key , largest_user_key ]是否和指定level的文件有重合的sst
bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}


//初始level为0，最高放到kMaxMemCompactLevel层。
//能升到第level+1层需要满足一下要求：
//level小于kMaxMemCompactLevel。
//Key范围和level+1层的文件没有重叠。
//Key范围和level+2层的文件可以有重叠，但是有重叠的文件的总size超过10倍默认文件最大值
// 这个时候就考虑放在level 层。
// minor compaction时，选择要dump的level级别。由于第0层文件频繁被访问，
// 而且有严格的数量限制，另外多个SST之间还存在重叠，所以为了减少读放大，
// 我们是否可以考虑将内存中的文件dump到磁盘时尽可能送到高层呢？
// PickLevelForMemTableOutput函数就是起这个作用的，该函数内部选择目标level，有如下几个原则：
// 返回所要选择的层数。
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  //首先检查新的SST文件和level 0的文件是否有重叠的key范围，
  //如果有(true)就返回level 0，
  //如果没有重叠，进入内部逻辑，level此时值为0.
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      //检查是否和level+1有重叠,如果有(true)，返回level。
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        //检查level+2层和新文件有key范围重叠的文件;返回所有重叠文件的元数据
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        //如果有重叠的文件总size超过阈值（默认为文件最大size的10倍）就返回level
        const int64_t sum = TotalFileSize(overlaps);
        if ( sum > MaxGrandParentOverlapBytes(vset_->options_) ) {
          break;
        }
      }
      //如果没有，level加1
      level++;
    }
  }
  return level;
}

  // Store in "*inputs" all files in "level" that overlap [begin,end]
  // 在所给定的level中找出和[begin, end]有重合的sstable文件。其中由于第0层，
  // 多个文件存在重叠，该函数常被用来压缩时候使用，而根据leveldb的设计，
  // level层合level+1层merge时候，level中所有重叠的sst都会参与，这一点需要特别注意。
  // 返回的结果存放在 inputs 当中。
  // 在这里需要补充一下针对0层文件的特殊处理，也就是图中的函数GetOverlappingInputs
  // 由于0层的文件有重叠部分，所以在压缩选择的时候，需要将所有重叠的部分全部选择进来。
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  //首先根据参数初始化查找变量。
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  //遍历该层的sstable文件，比较sstable的{minkey,max key}和传入的[begin, end]，如果有重合就记录文件到@inputs中。
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      //要注意的是，对于level0，由于文件可能有重合，其处理具有特殊性。
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        //level 0 的文件之间是无序的;彼此之间可能存在交集;
        //因此sstable的{minkey,max key}和传入的[begin, end]有交集的时候;需要扩大到两个的并集,继续查找;
        //对应的做法就是当选出文件后，判断还有哪些文件有重叠，把这些文件都加入进来，最终将所有有交集的全部加进来
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
//Builder是一个辅助类，实现Version + VersionEdit = Version'的功能，其中+ =分别对应Apply SaveTo两个接口。
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  //Builder的vset_与base_都是调用者传入的
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  //该函数将edit中的修改应用到当前状态中。
  //注意除了compaction点直接修改了vset_，其它删除和新加文件的变动只是先存储在Builder自己的成员变量中，在调用SaveTo(v)函数时才施加到v上。
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    //把edit记录的compaction点应用到当前状态
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    //把edit记录的已删除文件应用到当前状态
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    //把edit记录的新加文件应用到当前状态，这里会初始化文件的allowed_seeks值，
    //以在文件被无谓seek指定次数后自动执行compaction，这里作者阐述了其设置规则。
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      //值allowed_seeks事关compaction的优化，其计算依据如下，首先假设：
      // 1. 1次seek花费10ms
      // 2. 1M读写花费10ms(100MB/s)
      // 3. 1M文件的compact需要25M IO(读写10-12MB的下一层文件)，为什么10-12M?经验值?
      // 因此1M的compact时间 = 25次seek时间 = 250ms
      // 也就是40K的compact时间 = 1次seek时间，保守点取16KB，即t = 16K的compact时间 = 1次seek时间
      // compact这个文件的时间: file_size / 16K
      // 如果文件seek很多次但是没有找到key，时间和已经比compact时间要大，就应该compact了
      // 这个次数记录到f->allowed_seeks

      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  //把当前的状态存储到v中返回
  //For循环遍历所有的level[0, config::kNumLevels-1]，把新加的文件和已存在的文件merge在一起，丢弃已删除的文件，结果保存在v中。
  //对于level> 0，还要确保集合中的文件没有重合。
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    // 类似于是插入排序
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        // 按照 cmp 排序 找到原有文件中比added_iter小的文件，加入到V。
        // upper_bound ；查找[first, last)区域中第一个不符合 comp 规则的元素
        // 其中，first 和 last 都为正向迭代器，[first, last) 用于指定该函数的作用范围；
        // val 用于执行目标值；comp 作用自定义查找规则，
        // 此参数可接收一个包含 2 个形参（第一个形参值始终为 val）且返回值为 bool 类型的函数，
        // 可以是普通函数，也可以是函数对象。
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  //该函数尝试将f加入到levels_[level]文件set中。
  //要满足两个条件：
  //>1 文件不能被删除，也就是不能在levels_[level].deleted_files集合中；
  //>2 保证文件之间的key是连续的，即基于比较器vset_->icmp_，f的min key要大于levels_[level]集合中最后一个文件的max key；
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    // 看deleted_files文件中是否存在 f->number 如果存在那么就返回 1 反之为0
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache), //VersionSet会使用到TableCache，这个是调用者传入的。TableCache用于Get k/v操作
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      imm_last_sequence_(0),
      imm_log_file_number_(0),
      save_imm_last_sequence_(false),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  //next_file_number_从2开始；
  //创建新的Version并加入到Version链表中，并设置CURRENT=新创建version；
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

//把v加入到versionset中，并设置为current version。并对老的current version执行Uref()。
//在双向循环链表中的位置在dummy_versions_之前。
void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    //当自己不在是current的时候,计数减去一;如果没有人使用的时候,可以被踢出VersionSet
    current_->Unref();
  }
  current_ = v;
  //加入VersionSet的时候加一,保证不会被踢出VersionSet
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

// 该函数将产生一个新的version，如重新打开数据库或者发生压缩
// 用之前要加锁
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  //要保证edit自己的log number是比较大的那个，否则就是致命错误。
  //保证edit的log number小于next file number，否则就是致命错误。
  // next_file_number_ 总是现有基础上+1.
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    //如果edit中没有该字段,用current log_number_代替
    edit->SetLogNumber(log_number_);
  }
  //该参数废弃了
  if (!edit->has_prev_log_number_) {

    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  // TODO begin
  if( SaveImmLastSequence() ){
      edit->SetImmLastSequence(imm_last_sequence_,imm_log_file_number_);
  }
  // TODO end

  //创建一个新的Version v，并把新的edit变动保存到v中。
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  // 为v计算执行compaction的最佳level
  // 选择下一次需要压缩的文件，因为是单线程的，所以要尽可能的收集
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  // 如果指向manifest 文件的句柄不存在的话 直接再开一个
  if (descriptor_log_ == nullptr) {
    //如果MANIFEST文件指针不存在，就创建并初始化一个新的MANIFEST文件。
    //这只会发生在第一次打开数据库时。这个MANIFEST文件保存了current version的快照。
    //// 这里不需要unlock *mu因为我们只会在第一次调用LogAndApply时才走到这里(打开数据库时).
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr); // 文件指针和log::Writer都应该是NULL
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      //// 写入快照
      //把currentversion保存到*log中，信息包括comparator名字、compaction点和各级sstable文件，函数逻辑很直观。
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  //向MANIFEST写入一条新的log，记录current version的信息。
  //在文件写操作时unlock锁，写入完成后，再重新lock，以防止浪费在长时间的IO操作上。
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      /*先调用VersionEdit的 EncodeTo方法，序列化成字符串*/
      std::string record;
      // 从这里可以看出，manifest 开始记录的是快照，后面是记录每个版本迭代的增量信息。
      edit->EncodeTo(&record);
      /*注意，descriptor_log_是log文件*/
      //本质上说，MANIFEST文件是log类型的文件
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        /*调用Sync持久化*/
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    ////如果刚才创建了一个MANIFEST文件，通过写一个指向它的CURRENT文件
    //安装它；不需要再次检查MANIFEST是否出错，因为如果出错后面会删除它
    // 如果manifest文件有新增，此时CURRENT文件也需要进行更新，因为CURRENT永远指向的是最新的。
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  //安装这个新的version
  if (s.ok()) {
    /* 新的版本已经生成(通过current_和VersionEdit)， VersionEdit也已经写入MANIFEST，
     * 此时可以将v设置成current_, 同时将最新的version v链入VersionSet的双向链表*/
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    // 失败了，删除
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

  //Recover通过Manifest恢复VersionSet及Current Version信息，
  //恢复完毕后Alive的Version列表中仅包含当Current Version对象。
  //过程:
  //1.利用Current文件读取最近使用的manifest文件；
  //2.创建一个空的version，并利用manifest文件中的session record依次作apply操作，还原出一个最新的version，
  //注意manifest的第一条session record是一个version的快照，后续的session record记录的都是增量的变化；
  //3.将非current文件指向的其他过期的manifest文件删除；
  //4.将新建的version作为当前数据库的version；

  //注意，随着leveldb运行时间的增长，一个manifest中包含的session record会越来越多，故leveldb在每次启动时都会重新创建一个manifest文件，
  //并将第一条session record中记录当前version的快照状态。
  //其他过期的manifest文件会在下次启动的recover流程中进行删除。
  //leveldb通过这种方式，来控制manifest文件的大小，但是数据库本身没有重启，manifest还是会一直增长。
  // save_manifest ：是否用老的manifest，如果old manifest的size大于 2k的话，就会用新的，否则继续用老的。
Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  /*CURRENT文件记录着MANIFEST的文件名字，名字为MANIFEST-number */
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  ////打开mainfest
  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;

  // TODO begin
  bool have_imm_last_sequence = false;
  uint64_t imm_last_sequence = 0;
  uint64_t imm_log_file_number = 0;
  // TODO end

  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    //依次读取manifest中的VersionEdit信息，构建VersionSet
    // 当 ReadRecord 读完整个文件的时候返回的是false，读完一条记录的时候返回的是true。
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        ////Comparator不一致时，返回错误信息
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      ////构建当前Version
      /*按照次序，将Verison的变化量层层回放，最重会得到最终版本的Version*/
      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
      // TODO begin
      if (edit.has_imm_last_sequence_) {
          imm_last_sequence = edit.imm_last_sequence_;
          imm_log_file_number = edit.imm_log_file_number_;
          have_imm_last_sequence = true;
      }
      // TODO end
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    /*通过回放所有的VersionEdit，得到最终版本的Version，存入v*/
    Version* v = new Version(this);
    builder.SaveTo(v);
    //计算下次执行压缩的Level
    // Install recovered version
    Finalize(v);
    /* AppendVersion将版本v放入VersionSet集合，同时设置curret_等于v */
    //将新建的version作为当前数据库的version；
    AppendVersion(v);
    manifest_file_number_ = next_file; //edit.next_file_number_
    next_file_number_ = next_file + 1; //修改versionset中的next_file_number_
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
    imm_last_sequence_ = imm_last_sequence;
    imm_log_file_number_ = imm_log_file_number;

    // See if we can reuse the existing MANIFEST file.
    //当老的manifest文件大于2K的时候,返回false,表示需要生成新的manifest文件;(生成新的文件统一在LogAndApply中完成)
    //如果小于2K的时候,返回true,表示复用老的manifest文件,不生成新的manifest文件;
    //这里通过output参数save_manifest传递到外部函数,完成具体的逻辑
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

//传入的参数是还没有删除的manifest文件
//当老的manifest文件大于2K的时候,返回false,表示需要生成新的manifest文件;(生成新的文件统一在LogAndApply中完成)
//如果小于2K的时候,返回true,表示复用老的manifest文件,不生成新的manifest文件;
bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      //默认配置为2K;也就是当老的manifest文件大于2K的时候才会生成新的manifest,废弃老的manifest文件
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  //用老的manifest文件赋值descriptor_file_,descriptor_log_变量;表示重复使用老的manifest文件
  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

//该函数依照规则为下次的compaction计算出最适用的level，对于level 0和>0需要分别对待
//level == 0是看sst 的文件的数量 score = file_numbers / 4.
//level > 0 的时候看的是 score = level_Total_bytes / MaxBytesForLevel (参数)。
// 综合选择一个score的 分数最大的值。 
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      //为何level0和其它level计算方法不同，原因如下，这也是leveldb为compaction所做的另一个优化。
      //>1 对于较大的写缓存（write-buffer），做太多的level 0 compaction并不好
      //>2 每次read操作都要merge level 0的所有文件，因此我们不希望level 0有太多的小文件存在
      //（比如写缓存太小，或者压缩比较高，或者覆盖/删除较多导致小文件太多）。
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      //对于level 0以文件个数计算，kL0_CompactionTrigger默认配置为4。
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      //对于level>0，根据level内的文件总大小计算
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  } 
  //最后把计算结果保存到v的两个成员compaction_level_和compaction_score_中
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

//把currentversion保存到*log中，信息包括comparator名字、compaction点和各级sstable文件，函数逻辑很直观。
//写入到MANIFEST时，先将current version的db元信息保存到一个VersionEdit中，然后在组织成一个log record写入文件；
// 对当前版本拍一个照，记录在manifest文件当中，manifest 文件的句柄就是 log。
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  //首先声明一个新的VersionEdit edit；
  VersionEdit edit;
  //设置comparator
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  //遍历所有level，根据compact_pointer_[level]，设置compaction点：
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  //遍历所有level，根据current_->files_，设置sstable文件集合
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  //根据序列化并append到log（MANIFEST文件）中
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}
// 查询某个key大约需要访问的总字节数(主要是SST的大小)
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

//返回存在所有Version中的sstable文件,读取所有版本的数据
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}
// 某一层所有的文件size的总和
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}
// 获取 level层 的文件 和level+1层(level >= 1)重叠部分的字节数 中最大值
int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
// 计算 vector inputs中的文件 最大值和最小值。
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
// 计算 vector inputs1 和 inputs2 中的文件 最大值和最小值。
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}
// 创建多层合并迭代器，用于Compaction后的数据进行merge。
// - 第0层需要互相merge，因此可能涉及到文件有多个
// - 其他层，只需要创建一个即可
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  // c->inputs_[0].size() + 1 是因为 inputs_[1] 也有一个迭代器。
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        // 第0层的文件每个都需要创建好
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        // 创建两层迭代器，第一级迭代器为文件层面的，第二级迭代器为sst层面的
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}
// 对versionSet中需要压缩的文件 根据策略（size_compaction 或者 seek_compaction）生成一个压缩类
// 并且计算 压缩类中 inputs_ 
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  /*注意注释，优先按照size来选择compaction的层级，选择的依据即按照Finalize函数计算的score*/
  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  //初始化c->inputs_[0],即需要compaction的level层文件
  // size_compaction ： 根据文件的大小和数量来判定的。
  // seek_compaction ： 根据 allowed_seeks 的值来判断的，其实就是频数。一个sst被查找但是查找不到的的频数越高
  // 说明是需要进行压缩的
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  // 因为压缩会消耗CPU，因此保证每次不能太多，而大小阈值权重相比seek更大
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      // 如果compact_pointer_[level].empty() == true 的话说明这一层还没有合并过
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        //compact_pointer_[level]保存了level层上次被合并的文件的largest key
        //这个机制保证了level层所有文件都有均等的机会被合并，避免了一直合并头部的文件，
        //导致后面的文件没有机会被合并。
        c->inputs_[0].push_back(f);
        break;
      }
    }
    // TODO : 能进入这个条件应该是当 f->largest.Encode()<= compact_pointer_[level]，那么说明压缩完了
    // 或者是因为 current_->files_[level].size() == 0 ，
    // 如果为空的话，那么说明压缩点是level层的最大的数了，这个时候应该从头开始压缩了。
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    //比较简单
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  //level 0 的文件之间是无序的;彼此之间可能存在交集;
  //因此sstable的{minkey,max key}和传入的[begin, end]有交集的时候;需要扩大到两个的并集,继续查找;
  //对应的做法就是当选出文件后，判断还有哪些文件有重叠，把这些文件都加入进来，最终将所有有交集的全部加进来
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    // 在第0层中找到 范围为 [smallest , largest]的所有的文件，并把它放入到 inputs当中。
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  //选择level+1层文件
  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
// 在files中找到 最大的 key并返回到 largest_key这个值当中。
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
// 找到这样一个文件 满足以下条件，并且level_file.smallest_key是所有文件中是最小的值。
// largest_key < level_file.smallest_key
// largest_key.user_key == level_file.smallest_key.user_key
// TODO ：这里计算的层数最小也是第二层的。
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    ////SST文件中存储的key是internal key，由user key + seq + type 组成。user key相同时，seq越大，则internal key越小。
    //两个user key相同的internal key，seq大的key有效性高于seq小的key，所以此时input_file的largest_key的有效性高于level_file的smallest_key。
    //如果把input_file合并到了level+1层，而把level_file留在了level层。那么下次查找这个user key就会在level层找到level_file中已经无效地user key。
    //所以要把level_file也合并到level+1层中。新加入的文件同样可能存在上述情况，所以要递归地去添加level层文件。
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
//如果满足一下两个条件,将level_file加入到compaction中
//input_file.largest_key < level_file.smallest_key
//input_file.largest_key.user_key == level_file.smallest_key.user_key
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  // 获取待 compaction 最大 key
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

//根据inputs_[0]确定inputs_[1]
//根据inputs_[1]反过来看下能否扩大inputs_[0]
//inptus_[0]扩大的话，记录到expanded0
//根据expanded[0]看下是否会导致inputs_[1]增大
//如果inputs[1]没有增大，那就扩大 compact 的 level 层的文件范围,否则就用原来的 inputs_[0] 和 inputs_[1]
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  //在同level中;查找满足边界补充条件的sstable加入到compaction中
  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  //inputs_[0]所有文件的key range -> [smallest, largest]
  GetRange(c->inputs_[0], &smallest, &largest);
  //获取level+1层,所有smallest,largest有重叠的sstable文件
  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);

  // Get entire range covered by compaction
  //inputs_[0, 1]两层所有文件的key range -> [all_start, all_limit]
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  //根据inputs_[1]反推下 level 层有多少 key range 有重叠的文件，记录到expanded0
  if (!c->inputs_[1].empty()) {
    //查找level层和[all_start,all_limit]有重叠的文件集合，显然这个集合一定会是inputs_[0]的超集expanded0
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    //调用AddBoundaryInputs检查level层是否有更多的文件需要被加入
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    //inputs0_size、inputs1_size和expanded0_size。分别表示原level层选中文件总size，level+1层选中文件总size，level层扩展后的文件总size。
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    //如果level层扩展后选中的文件数量超过原level层选中的文件数量，即有增加文件。
    //且inputs1_size和expanded0_size之和没有超过扩展size阈值,则进行扩展
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      //检查扩展后，level+1层是否需要新增文件
      //如果没有,那么level层使用扩展文件集合,并更新all_start和all_limit。
      //如果需要新增文件，就不适用扩展集合。
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  // level + 2层有overlap的文件，记录到c->grandparents_
  //为了避免这些文件合并到 level + 1 层后，跟 level + 2 层有重叠的文件太多，届时合并 level + 1 和 level + 2 层压力太大
  //，因此我们还需要记录下 level + 2 层的文件，后续 compact 时用于提前结束的判断
  //这个在IsTrivialMove中会被用于判断是否可以直接把level层文件升到level+1层。
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  //记录compact_pointer_到c->edit_，在后续 PickCompaction 入口时使用
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}
// 该函数主要用于手动触发压缩。
Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  ////它在指定level中找出和[begin, end]有重合的sstable文件
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  // 先一步进行扩容，
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

// 同时满足以下条件时，我们只要简单的把文件从level标记到level + 1层就可以了
  // 1. level层只有一个文件
  // 2. level + 1层没有重叠文件
  // 3. 跟level + 2层overlap的文件没有超过25M（避免后面到level+1层时候和level+2重叠度太大）
  // 注：条件三主要是(避免mv到level + 1后，导致level + 1 与 level + 2层compact压力过大)
bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}
// 将所有需要删除SST文件添加到*edit。因为input经过变化生成output，
// 因此input对应到deleted_file容器，output进入added_file容器。
// 需要注意在前面在add的时候，先忽略掉deleted里面的，
// 因为IsTrivialMove是直接移动文件，到处都是细节。
void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}
// 判断当前user_key在>=(level+2)层中是否已经存在。主要是用于key的type=deletion时可不可以将该key删除掉。
// 如果存在的话就返回false，否则就返回true。
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      // 当 user_key 大于 f->largest.user_key())的时候就 ++
      level_ptrs_[lvl]++;
    }
  }
  return true;
}
 // 为了避免合并到level+1层之后和level+2层重叠太多，导致下次合并level+1时候时间太久，
 // 因此需要及时停止输出，并生成新的SST
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  //// 从level+2层找到第一个和internal_key产生冲突的文件
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >0) {
    // 如果是第一个key，无需处理
    if (seen_key_) {
      //增加与level+2层冲突的字节数
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  // 第二次开始就进行处理
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    // 与level+2层冲突太多后，需要产生新的输出文件用于合并
    // 记得清0冲突文件的大小总和
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
