//
// Created by buildings on 2022/6/15.
//
#include "value_log_writer.h"
#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
    namespace log {

        static void InitTypeCrc(uint32_t* type_crc) {
            for (int i = 0; i <= kMaxRecordType; i++) {
                char t = static_cast<char>(i);
                type_crc[i] = crc32c::Value(&t, 1);
            }
        }

        VlogWriter::VlogWriter(WritableFile* dest) : dest_(dest),head_(0) {
            InitTypeCrc(type_crc_);
        }

        VlogWriter::VlogWriter(WritableFile* dest, uint64_t dest_length)
                : dest_(dest), head_(0) {
            InitTypeCrc(type_crc_);
        }

        VlogWriter::~VlogWriter() = default;

// 将一条操作写入到缓存中，写入一条日志
        Status VlogWriter::AddRecord(const Slice& slice, uint64_t& offset) {
            const char* ptr = slice.data();
            size_t left = slice.size();

            // Fragment the record if necessary and emit it.  Note that if slice
            // is empty, we still want to iterate once to emit a single
            // zero-length record
            Status s;
            s = EmitPhysicalRecord(ptr, left,offset);

            return s;
        }
// 写入缓存区中
        Status VlogWriter::EmitPhysicalRecord( const char* ptr,
                                          size_t length,uint64_t& offset) {
            assert(length <= 0xffffffff);  // Must fit in two bytes

            // Format the header
            // check - 4个字节， length - 4个字节
            char buf[8];
            // Compute the crc of the record type and the payload
            // 计算crc; 只计算 type + data 对应的crc
            // uint32_t crc = crc32c::Extend(type_crc_[kFullType], ptr, length);
            // 计算crc; 只计算 data 对应的crc
            uint32_t crc = crc32c::Value(ptr,length);
            crc = crc32c::Mask(crc);  // Adjust for storage
            EncodeFixed32(buf, crc);
            EncodeFixed32(&buf[4], length);

            // Write the header and the payload
            //头部信息写入dest
            Status s = dest_->Append(Slice(buf, 8));
            if (s.ok()) {
                //data信息写入dest
                s = dest_->Append(Slice(ptr, length));
                if (s.ok()) {
                    //将内存中的剩余部分写入句柄中;其内容还留在操作系统管理的缓存中,并没有真真刷到磁盘上
                    s = dest_->Flush();
                    offset = head_ + 8;
                    head_ += 8 + length;
                }
            }
            return s;
        }

    }  // namespace log
}  // namespace leveldb
