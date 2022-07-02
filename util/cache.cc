// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
//  https://zhuanlan.zhihu.com/p/370972240
//  https://izualzhy.cn/leveldb-cache
struct LRUHandle {
  //缓存的sstalbe具体内容;TableAndFile对象或者 Block 等
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash; // HandleTable hash冲突时指向下一个LRUHandle*
  LRUHandle* next;      //LRU链表双向指针
  LRUHandle* prev;      //LRU链表双向指针
  // 用于计算LRUCache容量,有可能是entry的条数
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;    // key长度
  bool in_cache;     // Whether entry is in the cache.
  // in_cache = true && refs = 1 表明该节点就在 lru 中.
  // in_cache = true && refs >= 2 表明该节点就在 in_use.
  uint32_t refs;     // References, including cache reference, if present.
  // key 对应的hash值
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  // 占位符，结构体末尾，通过key_length获取真正的key
  char key_data[1];  // Beginning of key

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// leveldb 为了更丰富的结果以及自动化的 value 清理，

class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }
  // 如果存在就反回找到的 key的位置的的二级指针，也就是 保存 key的 LRUHandle* 的地址 即 LRUHandle**
  // 将要插入的h 插入到合适的地方，如果有相同的，则返回老的LRUHandle，反之则返回NULL。
  // 如果找到相同的 会将老的从hashtable中删除掉，用新的代替他
  LRUHandle* Insert(LRUHandle* h) {
    // 找到  存放 LRUHandle的地址 的内存地址 
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    // *ptr == 上一个LRUHandle中的next_hash的值
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }
  // 
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      // *ptr = 上一个节点的 next_hash ，所以是把 result 这个节点摘了出来然后返回
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;   // 哈希数组的大小
  uint32_t elems_;    // 哈希表中元素的个数
  LRUHandle** list_;  // 哈希使用数组来存储元素的

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // 如果某个LRUHandle*存在相同的hash&&key值，则返回该LRUHandle*的二级指针，就是装下 LRUHandle* 的内存的地址）
  // 如果不存在这样的LRUHandle*，则返回指向该bucket的最后一个LRUHandle*的next_hash的二级指针，其值为nullptr.
  // ptr: 存放 LRUHandle* 这个指针的内存地址（这个值都在上一个LRUHandle 中的next_hash ） 
  // 其实就是存放的是上一个LRUHandle中的next_hash这个指针的内存地址,
  // 通过 ptr 可以拿到上一个 LRUHandle中的 next_hash ，
  // 因为 上一个 LRUHandle*->next_hash == *ptr;
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    //先查找处于哪个桶
    // 这里采用&的方式来取余，这是有前提条件的，除数必须是2的n次幂才行
    // 桶的长度是以2来递增的
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    //某个LRUHandle* hash和key的值都与目标值相同;这里是通过顺序便利的方式查找,时间复杂度O(n)
    // TODO 有可能不同的 hash值取模后其实是一样的情况。也有可能hash值不一样但是key值一样？？这个有这个可能吗
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    
    return ptr;
  }

  void Resize() {
    //桶的个数初始化大小为4
    uint32_t new_length = 4;
    while (new_length < elems_) {
      //如果空间不够,成倍增长
      new_length *= 2;
    }
    //resize 的操作比较重，因为需要对所有节点进行重新分桶，而为了保证线程安全，需要加锁，但这会使得哈希表一段时间不能提供服务。
    //当然通过分片已经减小了单个分片中节点的数量，但如果分片不均匀，仍然会比较重。
    //这里有提到一种渐进式的迁移方法：Dynamic-sized NonBlocking Hash table，可以将迁移时间进行均摊。
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        // 头插法，*ptr其实指向的是放入桶中第一个元素
        h->next_hash = *ptr;
        // 将h放入到新的桶中
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
// 返回值统一定义为Cache::Handle*类型(实际类型为leveldb::(anonymous namespace)::LRUHandle*)，
// 需要手动调用Cache::Release(Cache::Handle*)清理函数。
// 内部的数据还是缓存在hashtable中，但是新增了两个 链表 一个in_use表示 在使用，一个lru_用来动态删除的。
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  // LRU容量，可以不固定：字节数或者条数均可
  size_t capacity_;

  // mutex_ protects the following state.
  // Insert/Lookup等操作时都先加锁
  mutable port::Mutex mutex_;
  // 用于跟capacity_比较，判断是否超过容量
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry. 表头的表示最新的,所以插入总是在表头进行
  // Entries have refs==1 and in_cache==true.
  // 所有已经不再为客户端使用的条目都放在 lru 链表中，
  // 该链表按最近使用时间有序，当容量不够用时，会驱逐此链表中最久没有被使用的条目。
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  //所有正在被客户端使用的数据条目（an kv item）都存在该链表中，该链表是无序的，
  //因为在容量不够时，此链表中的条目是一定不能够被驱逐的，因此也并不需要维持一个驱逐顺序。
  LRUHandle in_use_ GUARDED_BY(mutex_);

  //guarded_by属性是为了保证线程安全，使用该属性后，线程要使用相应变量，必须先锁定mutex_
  //table_是为了记录key和节点的映射关系，通过key可以快速定位到某个节点
  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

// 辅助函数：增加链节 e 的引用
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    // 将 e 插入到 in_use之前，其实就是插入到 in_use链表的末尾
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

// 辅助函数：减少链节 e 的引用
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate. 
    // 这个主要是在 lru的淘汰策略中实行的
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) { // 当满足这两个条件的时候才说明 LRUHandle 在缓存中是唯一的，没有其他地方引用他
    // No longer in use; move to lru_ list.
    // 这里是将e这个节点从 in_use_ 中删除掉.
    LRU_Remove(e);
    // 将 e 插入到 lru_ 之前，其实就是插入到 lru_ 链表的末尾
    LRU_Append(&lru_, e);
  }
}

//// 辅助函数：将链节 e 从双向链表中摘除 并不会从哈希表中删除,也没有释放e的空间
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

//// 辅助函数：将链节 e 追加到list前边
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}
// 通过key 和 hash值来查找哈希表中的 LRUHandle* 
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    // 该对象会返回给调用方 所以要引用一次
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}
// 加锁 将LRUHandle* 从in_use_中删除掉，重新加入到 lru_.
// 为了保证refs值准确，无论Insert还是Lookup，都需要及时调用Release释放，
// 使得节点能够进入lru_或者释放内存。 
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

// 为了保证refs值准确，无论Insert还是Lookup，都需要及时调用Release释放，
// 使得节点能够进入lru_或者释放内存。
// 插入一个 key 节点，如果这个 key之前在hashtable 中有的话删掉老的，替换成新的。
// 并且在 in_use 队列中加入这个节点 
// 删除的话 如果缓存之外有引用的话，那么就会减少引用次数，如果没有被其他引用的话，直接删除掉
Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);
  // LRUHandle 中 有一个占位符 char key_data[1];
  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  //TableAndFile结构体
  // 申请动态大小的LRUHandle内存，初始化该结构体
  // refs = 2:
  // 1. 返回的Cache::Handle*
  // 2. in_use_链表里记录
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  // 该LRUHandle对象指针会返回给调用方，因此refs此时为1。
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    // 添加到in_use_链表尾部，因此refs此时为2
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    //因为该节点会在外面使用，所以节点应该放在in_use链上
    LRU_Append(&in_use_, e);
    //新加新增的字节数
    usage_ += charge;
    // 如果 key&&hash 之前存在，那么HandleTable::Insert会返回原来的LRUHandle*对象指针
    // 这个指针的next_hash指向的是 找到的 key的位置，如果不存在 insert返回null
    // 如果 table_.Insert(e) 不为 NULL 的话那么 就要把这个 LRUHandle 删除
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }

  //// 如果超过了容量限制，根据lru_按照lru策略淘汰
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
// 辅助函数：从缓存中删除单个链节 e
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

// 辅助函数：从缓存中删除单个链节 e; 并且从哈希表中删除对应节点
// TODO ： 如果 e->refs != 1 的话 说明其他地方也会用到 所以不能删除
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}
// 手动检测是否有需要删除的节点，发生在节点超过容量之后
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
//16
static const int kNumShards = 1 << kNumShardBits;

//引入 SharedLRUCache 的目的在于减小加锁的粒度，提高读写并行度。
//将元素均分到各个的分片中，每一个分片拥有的元素数量就会少很多，扩容的机会会少一点
//策略比较简洁—— 利用 key 哈希值的前 kNumShardBits = 4 个 bit 作为分片路由，
//可以支持 kNumShards = 1 << kNumShardBits 16 个分片。
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_; 
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    // 计算hash值
    return Hash(s.data(), s.size(), 0);
  }
  // 选择一个 片进行存储。， hash值是32位的 右移(32 - kNumShardBits)
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  // NewId会加锁，返回一个全局唯一的自增ID.
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
