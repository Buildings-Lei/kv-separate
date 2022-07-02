//
// Created by buildings on 6/10/22.
//

#ifndef LEVELDB_VALUE_LOG_READER_H
#define LEVELDB_VALUE_LOG_READER_H

#include <cstdint>


#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

    class SequentialFile;
    class RandomAccessFile;

    namespace log {

        class VlogReader {
        public:
            // Interface for reporting errors.
            class Reporter {
            public:
                virtual ~Reporter();

                // Some corruption was detected.  "size" is the approximate number
                // of bytes dropped due to the corruption.
                virtual void Corruption(size_t bytes, const Status& status) = 0;
            };

            // Create a reader that will return log records from "*file".
            // "*file" must remain live while this Reader is in use.
            //
            // If "reporter" is non-null, it is notified whenever some data is
            // dropped due to a detected corruption.  "*reporter" must remain
            // live while this Reader is in use.
            //
            // If "checksum" is true, verify checksums if available.
            //
            // The Reader will start reading at the first record located at physical
            // position >= initial_offset within the file.
            VlogReader(SequentialFile* file, Reporter* reporter, bool checksum,
                   uint64_t initial_offset);
            VlogReader(RandomAccessFile* file, Reporter* reporter, bool checksum,
                       uint64_t initial_offset);

            VlogReader(const VlogReader&) = delete;
            VlogReader& operator=(const VlogReader&) = delete;

            ~VlogReader();
            // 从valuelog 中读取一条具体的值
            bool ReadValue( uint64_t offset, size_t length, Slice* value, char* scratch);

            // Read the next record into *record.  Returns true if read
            // successfully, false if we hit end of the input.  May use
            // "*scratch" as temporary storage.  The contents filled in *record
            // will only be valid until the next mutating operation on this
            // reader or the next mutation to *scratch.
            bool ReadRecord(Slice* record, std::string* scratch);
            // 在具体的位置中读取出 key-value 对。
            //bool ReadValue(uint64_t fid, size_t pos, slice& val);
            // Returns the physical offset of the last record returned by ReadRecord.
            //
            // Undefined before the first call to ReadRecord.
            uint64_t LastRecordOffset();

        private:
            // Extend record types with the following special values
            enum {
                kEof = kMaxRecordType + 1,
                // Returned whenever we find an invalid physical record.
                // Currently there are three situations in which this happens:
                // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
                // * The record is a 0-length record (No drop is reported)
                // * The record is below constructor's initial_offset (No drop is reported)
                kBadRecord = kMaxRecordType + 2
            };

            // Skips all blocks that are completely before "initial_offset_".
            //
            // Returns true on success. Handles reporting.
            bool SkipToInitialBlock();

            // Return type, or one of the preceding special values
            bool ReadPhysicalRecord(std::string* result);

            // Reports dropped bytes to the reporter.
            // buffer_ must be updated to remove the dropped bytes prior to invocation.
            void ReportCorruption(uint64_t bytes, const char* reason);
            void ReportDrop(uint64_t bytes, const Status& reason);

            SequentialFile* const file_;
            RandomAccessFile* const file_random_;
            Reporter* const reporter_;
            bool const checksum_;
            char* const backing_store_;
            std::string buffer_left_;
            Slice buffer_;
            bool eof_;  // Last Read() indicated EOF by returning < kBlockSize

            // Offset of the last record returned by ReadRecord.
            uint64_t last_record_offset_;
            // Offset of the first location past the end of buffer_.
            uint64_t end_of_buffer_offset_;

            // Offset at which to start looking for the first record to return
            uint64_t const initial_offset_;

            // True if we are resynchronizing after a seek (initial_offset_ > 0). In
            // particular, a run of kMiddleType and kLastType records can be silently
            // skipped in this mode
            bool resyncing_;
        };

    }  // namespace log
}  // namespace leveldb

#endif //LEVELDB_VALUE_LOG_READER_H
