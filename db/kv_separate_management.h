//
// Created by buildings on 6/13/22.
//
#ifndef LEVELDB_KV_SEPARATE_MANAGEMENT_H
#define LEVELDB_KV_SEPARATE_MANAGEMENT_H

#include <unordered_map>
#include <unordered_set>

#include <deque>
#include "leveldb/slice.h"
#include "garbage_collection.h"
#include <iterator>


namespace leveldb {
    // 用来gc回收的时候，传递信息的作用的。
    typedef struct ValueLogInfo {
        uint64_t last_sequence_;
        size_t file_size_;        // 文件大小
        uint64_t logfile_number_;           // 文件编号
        int left_kv_numbers_;   // 剩下的kv数量
        uint64_t invalid_memory_;     // value log 中无效的空间大小
    }ValueLogInfo;

    struct MapCmp{
        bool operator ()(const ValueLogInfo* a, const ValueLogInfo* b)
        {
            return a->invalid_memory_ < b->invalid_memory_; // 按照value从大到小排列
        }
    };

    class SeparateManagement {

    public:
        SeparateManagement( uint64_t garbage_collection_threshold)
        : garbage_collection_threshold_(garbage_collection_threshold) {}

        ~SeparateManagement(){}

        bool ConvertQueue(uint64_t& db_sequence); // 改变 db 的 last_sequence

        void UpdateMap(uint64_t fid,uint64_t abandon_memory);

        void UpdateQueue(uint64_t fid);

        bool GetGarbageCollectionQueue(uint64_t& fid,uint64_t& last_sequence);

        void WriteFileMap(uint64_t fid, int kv_numbers, size_t log_memory);

        bool MayNeedGarbageCollection(){ return !garbage_collection_.empty(); }

        void RemoveFileFromMap(uint64_t fid){ map_file_info_.erase(fid); }

        bool EmptyMap(){ return map_file_info_.empty(); }

        void ColletionMap();

    private:

        uint64_t garbage_collection_threshold_;
        // 当前版本的所有的vlog文件的索引。
        std::unordered_map<uint64_t,ValueLogInfo*> map_file_info_;
        // 垃圾回收队列，这个队列中表示的文件info 将来是要进行gc回收的
        std::deque<ValueLogInfo*> garbage_collection_;
        // 需要触发gc回收的，但是还没有进行分配sequencen的info
        std::deque<ValueLogInfo*> need_updates_;
        //
        std::unordered_set<uint64_t> delete_files_;
    };
}



#endif //LEVELDB_KV_SEPARATE_MANAGEMENT_H
