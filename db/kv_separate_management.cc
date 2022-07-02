//
// Created by buildings on 6/13/22.
//

#include "kv_separate_management.h"
#include <queue>
#include <vector>
namespace leveldb {
    // 改变 db 的 last_sequence，给每一个需要进行gc回收的value log 文件分配 新的sequence的序号，
    // 以便对value log 中的有效key的重新put进新的value log 中。返回值决定是否进行gc
    bool SeparateManagement::ConvertQueue(uint64_t& db_sequence) {
        if( !need_updates_.empty() ) {
            db_sequence++;
        }else {
            return false;
        }
        while( !need_updates_.empty() ){
            ValueLogInfo* info = need_updates_.front();
            need_updates_.pop_front();
            // TODO 这里还需要删除 现场保留
            map_file_info_.erase(info->logfile_number_);
            info->last_sequence_ = db_sequence;
            db_sequence += info->left_kv_numbers_;
            garbage_collection_.push_back(info);
        }
        assert( db_sequence <= kMaxSequenceNumber);
        return true;
    }
    // 每一个vlog 罗盘的时候都会在map_file_info_ 中添加索引 ，这个在新建一个value log的时候会用到。
    void SeparateManagement::WriteFileMap(uint64_t fid, int kv_numbers,size_t log_memory){
        assert( map_file_info_.find(fid) == map_file_info_.end() );
        ValueLogInfo* info = new ValueLogInfo();
        info->logfile_number_ = fid;
        info->left_kv_numbers_ = kv_numbers;
        assert( kv_numbers <= kMaxSequenceNumber );
        info->invalid_memory_ = 0;
        info->last_sequence_ = -1;
        info->file_size_ = log_memory;
        map_file_info_.insert( std::make_pair(fid,info) );
        return;
    }
    // map_file_info_ 存放了所有的value log 的信息，每次删除一个key的时候要对这个key对应的value log计算空间无效利用率
    // 所以要统计有多少空间是无效的，以便后面进行触发gc的过程。
    void SeparateManagement::UpdateMap(uint64_t fid,uint64_t abandon_memory){
        // 在map 中进行检测中发现是有这个
        if( map_file_info_.find(fid) != map_file_info_.end()) {
            ValueLogInfo* info = map_file_info_[fid];
            info->left_kv_numbers_--;
            info->invalid_memory_ += abandon_memory;
        }
        return;
    }
    // 遍历 map_file_info_ 中所有的file 找到无效空间最大的log 进行gc回收 ，这个文件要不存在 delete_files_中
    void SeparateManagement::UpdateQueue(uint64_t fid){
        auto iter = map_file_info_.begin();
        std::priority_queue<ValueLogInfo*,std::vector<ValueLogInfo*>,MapCmp> sort_priority_;

        for( iter; iter != map_file_info_.end(); ++iter){
            if( delete_files_.find( iter->first) == delete_files_.end()){
                sort_priority_.push(iter->second);
            }
        }
        int num = 1;
        int threshold = garbage_collection_threshold_;
        // 应该是大于两个阈值的情况 大于一个阈值的时候只取一个，大于两个阈值的
        if(!sort_priority_.empty()
            && sort_priority_.top()->invalid_memory_ >= garbage_collection_threshold_ * 1.2){
            num = 3;
            threshold = garbage_collection_threshold_ * 1.2;
        }
        while( !sort_priority_.empty() && num > 0){
            ValueLogInfo* info = sort_priority_.top();
            sort_priority_.pop();
            if( info->logfile_number_ > fid ){
                continue;
            }
            num--;
            if( info->invalid_memory_ >= threshold){
                need_updates_.push_back(info);
                delete_files_.insert(info->logfile_number_);
            }
        }
        return;
    }
    // gc回收线程用来获得需要回收的文件
    bool SeparateManagement::GetGarbageCollectionQueue(uint64_t& fid,uint64_t& last_sequence){
        if( garbage_collection_.empty() ) {
            return false;
        }
        else{
            ValueLogInfo* info = garbage_collection_.front();
            garbage_collection_.pop_front();
            fid = info->logfile_number_;
            last_sequence = info->last_sequence_;
            return true;
        }
    }

    void SeparateManagement::ColletionMap(){
        if( map_file_info_.empty() ) return;

        for( auto iter : map_file_info_ ){
            uint64_t fid  = iter.first;
            ValueLogInfo* info = iter.second;
            if( delete_files_.find(fid) == delete_files_.end() ){
                need_updates_.push_back(info);
                delete_files_.insert(info->logfile_number_);
            }
        }
        return;
    }

}