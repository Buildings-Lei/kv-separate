//
// Created by buildings on 6/13/22.
//

#ifndef LEVELDB_GARBAGE_COLLECTION_H
#define LEVELDB_GARBAGE_COLLECTION_H

#include "db/log_reader.h"
#include "db/db_impl.h"
#include "leveldb/env.h"

namespace leveldb {


class DBImpl;


class GarbageCollection {
public:
    struct ValueLogReporter : public log::Reader::Reporter {
        Status* status;
        void Corruption(size_t bytes, const Status& s) override {

        }
    };
    GarbageCollection(DBImpl* db): db_(db){}
    ~GarbageCollection(){}
    // gc 的后台回收任务函数
    void GcBackGroundWork(void* arg){
//        DBImpl* db = reinterpret_cast<DBImpl*>(arg);
//        SequentialFile* fileread;
//        ValueLogReporter* reporter;
//        status s =  NewSequentialFile("/home/buildings/kv/test.vlog",&fileread);
//        log::Reader reader(fileread,reporter,true,0);
    }

private:
    DBImpl* db_;
    int file_num_;
    int last_file_offset_; //
};

}


#endif //LEVELDB_GARBAGE_COLLECTION_H
