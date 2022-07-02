//
// Created by buildings on 6/10/22.
//

#include "value_log_reader.h"
#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
    namespace log {
        VlogReader::Reporter::~Reporter() = default;

        VlogReader::VlogReader(SequentialFile *file, Reporter *reporter, bool checksum,
                               uint64_t initial_offset)
                : file_(file),
                  file_random_(NULL),
                  reporter_(reporter),
                  checksum_(checksum),
                  backing_store_(new char[kBlockSize]),
                  buffer_(),
                  eof_(false),
                  last_record_offset_(0),
                  end_of_buffer_offset_(0),
                  initial_offset_(initial_offset),
                  resyncing_(initial_offset > 0) {}

        VlogReader::VlogReader(RandomAccessFile *file, Reporter *reporter, bool checksum,
                               uint64_t initial_offset)
                : file_(NULL),
                  file_random_(file),
                  reporter_(reporter),
                  checksum_(checksum),
                  backing_store_(new char[kBlockSize]),
                  buffer_(),
                  eof_(false),
                  last_record_offset_(0),
                  end_of_buffer_offset_(0),
                  initial_offset_(initial_offset),
                  resyncing_(initial_offset > 0) {}

        VlogReader::~VlogReader() { delete[] backing_store_; }

        bool VlogReader::SkipToInitialBlock() {

            return true;
        }

        bool VlogReader::ReadValue(uint64_t offset, size_t length, Slice *value, char *scratch) {

            if (file_random_ == NULL) {
                return false;
            }
            Status status = file_random_->Read(offset, length, value, scratch);
            if (!status.ok()) {
                return false;
            }
            return true;
        }

        // 一次读取一条record ，返回的是头部带有crc和长度信息的 ，和leveldb的log_reader是不一样的。
        bool VlogReader::ReadRecord(Slice *record, std::string *scratch) {

            if( ReadPhysicalRecord(scratch) ){
                *record = *scratch;
                return true;
            }
            return false;
        }

        uint64_t VlogReader::LastRecordOffset() { return last_record_offset_; }

        void VlogReader::ReportCorruption(uint64_t bytes, const char *reason) {
            ReportDrop(bytes, Status::Corruption(reason));
        }

        void VlogReader::ReportDrop(uint64_t bytes, const Status &reason) {
            if (reporter_ != nullptr &&
                end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
                reporter_->Corruption(static_cast<size_t>(bytes), reason);
            }
        }

    bool VlogReader::ReadPhysicalRecord(std::string *result) {
        result->clear();
        buffer_.clear();
        // 少于 7个字节，说明连头的内容都不够，需要从物理上读取了，否则还是继续解析之前的。
        char* tmp_head = new char[vHeaderSize];
        Status status = file_->Read(vHeaderSize, &buffer_, tmp_head);
        if (!status.ok()) {
            buffer_.clear();
            ReportDrop(kBlockSize, status);
            eof_ = true;
            return false;
        } else if (buffer_.size() < vHeaderSize) {
            eof_ = true;
        }
        if( !eof_) {
            result->assign(buffer_.data(),buffer_.size());
            uint32_t expected_crc = DecodeFixed32(buffer_.data() );
            buffer_.remove_prefix(4);
            uint32_t length = DecodeFixed32(buffer_.data() );
            buffer_.clear();
            char* tmp = new char[length];
            status = file_->Read(length, &buffer_, tmp);
            if( status.ok() && buffer_.size() == length){
                if (checksum_) {
                    uint32_t actual_crc = crc32c::Value(tmp, length);
                    actual_crc = crc32c::Mask(actual_crc);
                    if (actual_crc != expected_crc) {
                        // Drop the rest of the buffer since "length" itself may have
                        // been corrupted and if we trust it, we could find some
                        // fragment of a real log record that just happens to look
                        // like a valid log record.
                        size_t drop_size = buffer_.size();
                        buffer_.clear();
                        result->clear();
                        ReportCorruption(drop_size, "checksum mismatch");
                        eof_ = true;
                        return false;
                    }
                }
                //result->append(tmp);
                *result += buffer_.ToString();
            }else{
                eof_ = true;
            }
            delete [] tmp;
        }
        delete [] tmp_head;
        if(eof_){
            result->clear();
            return false;
        }
        return true;
}

}  // namespace log

}
