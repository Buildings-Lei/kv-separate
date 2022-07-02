// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
  // 在Linux下 这个是防止fd文件泄露给fork出来的子进程的。
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

constexpr const size_t kWritableFileBufferSize = 65536;

// 信息报错
Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
// 计数器;如果调用超过指定的max_acquires最大值,将不能在调用
// Helper 类限制资源使用，避免耗尽。目前用于限制只读文件描述符和 mmap 文件使用，
// 这样我们就不会用完文件描述符或虚拟内存，或者遇到非常大的数据库的内核性能问题。
class Limiter {
 public:
  // Limit maximum number of resources to |max_acquires|.
  Limiter(int max_acquires) : acquires_allowed_(max_acquires) {}

  Limiter(const Limiter&) = delete;
  Limiter operator=(const Limiter&) = delete;

  // If another resource is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    int old_acquires_allowed =
        acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);

    if (old_acquires_allowed > 0) return true;

    acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Release a resource acquired by a previous call to Acquire() that returned
  // true.
  void Release() { acquires_allowed_.fetch_add(1, std::memory_order_relaxed); }

 private:
  // The number of available resources.
  //
  // This is a counter and is not tied to the invariants of any other class, so
  // it can be operated on safely using std::memory_order_relaxed.
  std::atomic<int> acquires_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
// 顺序读文件的内容，从文件头开始读
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(filename) {}
  ~PosixSequentialFile() override { close(fd_); }
  // 从文件中读取 n个字节大小到 result 和 scratch中，从头开始读
  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    while (true) {
      ::ssize_t read_size = ::read(fd_, scratch, n);
      if (read_size < 0) {  // Read error.
        if (errno == EINTR) {
          continue;  // Retry
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }
  // 文件中 偏移量偏移n。
  Status Skip(uint64_t n) override {
    // SEEK_CUR：读写偏移量将指向当前位置偏移量 + offset 字节位置处， 
    // offset 可以为正、也可以为负，如果是正数表示往后偏移，如果是负数则表示往前偏移；
    // 返回偏移量
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;
  const std::string filename_;
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
// 线程安全的，用于随机读取文件内容的文件抽象
// fd_limiter 要活的比这个类的时间长，
// 如果 文件描述符 和mmp 还没有超过Limiter 的限制的话
// 那么可以让文件描述符永远存在has_permanent_fd_ 设置为true.
class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
  // instance, and will be used to determine if .
  // fd_limiter 要活的比这个类的时间长，
  // 如果 文件描述符 和mmp 还没有超过Limiter 的限制的话
  // 那么可以让文件描述符fd 永远存在has_permanent_fd_ 设置为true.否则就关闭这个fd
  PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
      : has_permanent_fd_(fd_limiter->Acquire()),
        fd_(has_permanent_fd_ ? fd : -1),
        fd_limiter_(fd_limiter),
        filename_(std::move(filename)) {
    if (!has_permanent_fd_) {
      assert(fd_ == -1);
      ::close(fd);  // The file will be opened on every read.
    }
  }

  ~PosixRandomAccessFile() override {
    if (has_permanent_fd_) {
      assert(fd_ != -1);
      ::close(fd_);
      fd_limiter_->Release();
    }
  }
  // 从文件中读取 文件偏移量为offset ，大小为 n的字节数到 scratch中，并且生成 slice 到result中
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    int fd = fd_;
    if (!has_permanent_fd_) {
      fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
      if (fd < 0) {
        return PosixError(filename_, errno);
      }
    }

    assert(fd != -1);

    Status status;
    // pread: Read NBYTES into BUF from FD at the given position OFFSET without
    // changing the file pointer.  Return the number read, -1 for errors
    // or 0 for EOF. 
    // 读并且移动 文件指针，这两个在pread中是一个原子操作。
    ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
    if (read_size < 0) {
      // An error: return a non-ok status.
      status = PosixError(filename_, errno);
    }
    if (!has_permanent_fd_) {
      // Close the temporary file descriptor opened earlier.
      assert(fd != fd_);
      ::close(fd);
    }
    return status;
  }

 private:
  // 表示是否每次read都需要打开文件
  const bool has_permanent_fd_;  // If false, the file is opened on every read.
  const int fd_;                 // -1 if has_permanent_fd_ is false.
  Limiter* const fd_limiter_;
  const std::string filename_;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
// 共享内存提高性能，减少读文件的的一次拷贝 但是 mmap_base 要先创建，直接从磁盘文件拷贝到进程
// 正常的读取 是先从磁盘文件拷贝到内核，再从内核中拷贝到进程中，两次拷贝
// 一般说来，进程在映射空间的对共享内容的改变并不直接写回到磁盘文件中，
// 往往在调用munmap（）后才执行该操作。
// 可以通过调用msync()实现磁盘上文件内容与共享内存区的内容一致。
class PosixMmapReadableFile final : public RandomAccessFile {
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // aquired the right to use one mmap region, which will be released when this
  // instance is destroyed.
  PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,
                        Limiter* mmap_limiter)
      : mmap_base_(mmap_base),
        length_(length),
        mmap_limiter_(mmap_limiter),
        filename_(std::move(filename)) {}

  ~PosixMmapReadableFile() override {
    ::munmap(static_cast<void*>(mmap_base_), length_);
    mmap_limiter_->Release();
  }
  // 映射后 就像读取普通的内存一样可以读取文件中的内容
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char* const mmap_base_;
  const size_t length_;
  Limiter* const mmap_limiter_;
  const std::string filename_;
};

// linux 下封装了写文件的一些操作
class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
      : pos_(0),
        fd_(fd),
        file_size_(0),
        is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)),
        dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }
  // TODO begin
  size_t GetSize() { return file_size_;}
  // TODO end
  // 将数据先放到buffer中，如果满了则写入到文件中
  // 如果要写入的文件实在是太大了的话，直接写入文件中。
  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();
    // TODO begin
    file_size_ += write_size;
    // TODO end
    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    // 走到这一步 说明要写入的size太大了，先写入buffer满了清空之后，buffer中大小还是不能满足
    // 所以直接把它写入到文件中
    return WriteUnbuffered(write_data, write_size);
  }
  
  // 先把缓存写入文件中，然后调用linux中的系统调用 关闭文件，返回状态
  Status Close() override {
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }
  // 这个函数会将指向缓存的pos置位0，相当于把缓存写入到文件中然后清空缓存
  // 会返回状态
  Status Flush() override { return FlushBuffer(); }
  // 同步措施，如果是 manifest 的话是要实时刷盘的，实时的写入到硬盘中的。
  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }
    // 当上面的强行同步写入到硬盘没有成功的时候，就试一试使用write来写
    status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }
    // write 写入还没成功的话，这会再来试一下 使用同步写入硬盘中
    return SyncFd(fd_, filename_);
  }

 private:
  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }
  // 将buffer 写入到实际的文件中 fd
  Status WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;  // Retry
        }
        return PosixError(filename_, errno);
      }
      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }
    // 这里设置为只读属性，因为后面SyncFd会强制直接写入硬盘中，
    // 而不像write 不是直接写入到硬盘中，而是等系统在满足一定的条件的时候统一写入硬盘中。
    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  // 一般情况下，对硬盘（或者其他持久存储设备）文件的write操作，更新的只是内存中的页缓存（page cache），而脏页面不会立即更新到硬盘中，而是由操作系统统一调度，如由专门的flusher内核线程在满足一定条件时（如一定时间间隔、内存中的脏页达到一定比例）内将脏页面同步到硬盘上（放入设备的IO请求队列）。
  /*
  write调用不会等到硬盘IO完成之后才返回，
  因此如果OS在write调用之后、硬盘同步之前崩溃，则数据可能丢失。虽然这样的时间窗口很小，
  但是对于需要保证事务的持久化（durability）和一致性（consistency）的数据库程序来说，
  write()所提供的“松散的异步语义”是不够的，
  通常需要OS提供的同步IO（synchronized-IO）原语来保证
  原文链接：https://blog.csdn.net/misayaaaaa/article/details/101075038
  是要先 write 写了之后再调用这个函数，这个是确保能刷盘到磁盘中
  */
  static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
    // On macOS and iOS, fsync() doesn't guarantee durability past power
    // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
    // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
    // fsync().
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
      return Status::OK();
    }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
    // 如果能保证不在乎metadata的话可以用下面这个
    bool sync_success = ::fdatasync(fd) == 0;
#else
    bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fd_path, errno);
  }

  // Returns the directory name in a path pointing to a file.
  //
  // Returns "." if the path does not contain any directory separator.
  static std::string Dirname(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  static Slice Basename(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return Slice(filename.data() + separator_pos + 1,
                 filename.length() - separator_pos - 1);
  }

  // True if the given file is a manifest file.
  static bool IsManifest(const std::string& filename) {
    return Basename(filename).starts_with("MANIFEST");
  }

  // buf_[0, pos_ - 1] contains data to be written to fd_.
  // 缓冲区 64k的大小
  char buf_[kWritableFileBufferSize];
  // buf_当前已经使用的字节的位置
  size_t pos_;
  int fd_;
  // TODO begin
  int file_size_;
  // TODO end

  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
  const std::string filename_;
  const std::string dirname_;  // The directory of filename_.
};

// 给文件上锁 或者是解锁，通过lock变量来控制是解锁还是上锁
// 不同进程如果要再次加载 就失败了就返回 -1.
// TODO 这里有待实验， 但是不同的线程进行锁的时候 发现返回的还是0
// fcntl() 功能是针对文件描述符提供控制，
// 根据不同的 cmd 对文件描述符可以执行的操作也非常多，
// 用的最多的是文件记录锁，也就是 F_SETLK 命令，此命令搭配 flock 结构体，
// 对文件进行加解锁操作，例如执行加锁操作，如果不解锁，
// 本进程或者其他进程再次使用 
// F_SETLK 命令访问同一文件则会告知目前此文件已经上锁，加锁进程退出（正常、异常）后会自行解锁，
// 使用此特性可以实现避免程序多次运行、锁定文件防止其他进行访问等操作。
// 原文链接：https://blog.csdn.net/qq_37733540/article/details/107864015
int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;  // Lock/unlock entire file.
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}

// Instances are thread-safe because they are immutable.
// PosixFileLock 对fd_和 filename_的一个包装 ，方便给PosixLockTable进行使用
// 在 UnlockFile 和 LockFile 中会被使用到
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}

  int fd() const { return fd_; }
  const std::string& filename() const { return filename_; }

 private:
  const int fd_;
  const std::string filename_;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
// 内部维护了一个哈希表，对文件名进行维护
class PosixLockTable {
 public:
  bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    bool succeeded = locked_files_.insert(fname).second;
    mu_.Unlock();
    return succeeded;
  }
  void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    locked_files_.erase(fname);
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_ GUARDED_BY(mu_);
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static const char msg[] =
        "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }
  // 获取顺序读文件的类，filename 表示要进行读取的文件名
  // SequentialFile** result 返回对这个文件顺序读取操作的类，
  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }
  
  // 获取随机读文件的类，filename 表示要进行读取的文件名
  // RandomAccessFile** result 返回对这个文件随机读取操作的类，
  // 如果有mmap数量没有用完的话，就返回的是 PosixMmapReadableFile 
  // 反之返回的就是 PosixRandomAccessFile
  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    *result = nullptr;
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    //如果mmap资源已经用完,则使用文件读取方法
    //64bit主机,默认mmap资源为1000
    if (!mmap_limiter_.Acquire()) {
      *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
      return Status::OK();
    }

    //使用mmap资源读取文件
    uint64_t file_size;
    Status status = GetFileSize(filename, &file_size);
    if (status.ok()) {
      void* mmap_base =
          ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmap_base != MAP_FAILED) {
        *result = new PosixMmapReadableFile(filename,
                                            reinterpret_cast<char*>(mmap_base),
                                            file_size, &mmap_limiter_);
      } else {
        status = PosixError(filename, errno);
      }
    }
    ::close(fd);
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }

  // 获取写文件的类，filename 表示要进行写入的文件名
  // WritableFile** result 返回对这个文件随机写操作的类，PosixWritableFile
  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }
  // 获取写文件的类，filename 表示要进行写入的文件名
  // WritableFile** result 返回对这个文件随机写操作的类，PosixWritableFile
  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    return ::access(filename.c_str(), F_OK) == 0;
  }
  // 获取目录的路径下的所有的文件，包括目录还有 . 和  .. 这两个
  // . 表示当前目录  .. 表示上一级目录, 对于linux下是和目录名字一样等级的，因为都是可以用cd命令。
  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();
    ::DIR* dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
      return PosixError(directory_path, errno);
    }
    struct ::dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
      result->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
  }
  // 删除一个文件
  Status RemoveFile(const std::string& filename) override {
    if (::unlink(filename.c_str()) != 0) {
      return PosixError(filename, errno);
    }
    return Status::OK();
  }
  // 创建一个目录
  Status CreateDir(const std::string& dirname) override {
    if (::mkdir(dirname.c_str(), 0755) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }
  // 删除一个目录
  Status RemoveDir(const std::string& dirname) override {
    if (::rmdir(dirname.c_str()) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }
  // 获取文件的大小
  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) != 0) {
      *size = 0;
      return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if (std::rename(from.c_str(), to.c_str()) != 0) {
      return PosixError(from, errno);
    }
    return Status::OK();
  }
  // FileLock** lock 是一个返回值，里面含有打开文件后的 fd
  // filename : 需要锁的文件名 
  // 如果已经被加锁了，返回已被加锁的错误信息
  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!locks_.Insert(filename)) {
      ::close(fd);
      return Status::IOError("lock " + filename, "already held by process");
    }

    if (LockOrUnlock(fd, true) == -1) {
      int lock_errno = errno;
      ::close(fd);
      locks_.Remove(filename);
      return PosixError("lock " + filename, lock_errno);
    }

    *lock = new PosixFileLock(fd, filename);
    return Status::OK();
  }
  // 解锁 
  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
      return PosixError("unlock " + posix_file_lock->filename(), errno);
    }
    locks_.Remove(posix_file_lock->filename());
    ::close(posix_file_lock->fd());
    delete posix_file_lock;
    return Status::OK();
  }
  // 任务调度，加入一个任务到任务队列中，让后台线程执行，这里只有一个线程去执行
  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;
  void ScheduleForGarbageCollection(void (*background_work_function)(void* background_work_arg),
                                void* background_work_arg) override;
  // 启动线程
  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override {
    std::thread new_thread(thread_main, thread_main_arg);
    new_thread.detach();
  }
  // 获取测试文件路径
  Status GetTestDirectory(std::string* result) override {
    const char* env = std::getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                    static_cast<int>(::geteuid()));
      *result = buf;
    }

    // The CreateDir status is ignored because the directory may already exist.
    CreateDir(*result);

    return Status::OK();
  }
  // 新建立一个错误日志打印器
  Status NewLogger(const std::string& filename, Logger** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    std::FILE* fp = ::fdopen(fd, "w");
    if (fp == nullptr) {
      ::close(fd);
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
  }
  // 获取时间
  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }
  // 休眠 micros 时间
  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

 private:
  void BackgroundThreadMain();
  void BackgroundThreadMainGarbageCollection();
  // 后台多线程的切入点
  static void BackgroundThreadEntryPoint(PosixEnv* env) {
    env->BackgroundThreadMain();
  }
  // TODO begin 切换
  // 后台多线程的切入点
  static void BackgroundThreadEntryPointforGlobalCollection(PosixEnv* env) {
    env->BackgroundThreadMainGarbageCollection();
  }
  // TODO end
  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe beacuse it is immutable.
  // 保存要运行的任务的相关信息
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  // 线程池中的任务队列，不断会有线程从这个队列中获取任务执行
  // GUARDED_BY是数据成员的属性，该属性声明数据成员受给定功能保护。
  // 对数据的读操作需要共享访问，而写操作则需要互斥访问。
  // 该 GUARDED_BY属性声明线程必须先锁定 background_work_mutex_
  // 才能对其进行读写 background_work_queue_ ，从而确保增量和减量操作是原子的
  // 原文链接：https://blog.csdn.net/weixin_38140931/article/details/103958906
  // 该语句没有运行开销，仅仅只在编译器是有效的，编译期进行静态分析
  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);
    // TODO begin gc 回收相关的变量
    port::Mutex background_GlobalCollection_work_mutex_;
    port::CondVar background_GlobalCollection_work_cv_ GUARDED_BY(background_GlobalCollection_work_mutex_);

    std::queue<BackgroundWorkItem> background_GlobalCollection_work_queue_
    GUARDED_BY(background_GlobalCollection_work_mutex_);
    // TODO end


  PosixLockTable locks_;  // Thread-safe.
  Limiter mmap_limiter_;  // Thread-safe.
  Limiter fd_limiter_;    // Thread-safe.
};

// Return the maximum number of concurrent mmaps.
int MaxMmaps() { return g_mmap_limit; }

// Return the maximum number of read-only files to keep open.
int MaxOpenFiles() {
  if (g_open_read_only_file_limit >= 0) {
    return g_open_read_only_file_limit;
  }
  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
    // getrlimit failed, fallback to hard-coded default.
    g_open_read_only_file_limit = 50;
  } else if (rlim.rlim_cur == RLIM_INFINITY) {
    g_open_read_only_file_limit = std::numeric_limits<int>::max();
  } else {
    // Allow use of 20% of available file descriptors for read-only files.
    g_open_read_only_file_limit = rlim.rlim_cur / 5;
  }
  return g_open_read_only_file_limit;
}

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      background_GlobalCollection_work_cv_(&background_GlobalCollection_work_mutex_),
      started_background_thread_(false),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()) {}
// 任务调度，加入一个任务到任务队列中，让后台线程执行，这里只有一个线程去执行
void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
    // 创建 gc 回收线程
   std::thread background_GlobalCollection_thread(PosixEnv::BackgroundThreadEntryPointforGlobalCollection, this);
   background_GlobalCollection_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }
  // 因为是锁住了 所以可以先 signal 再 emplace。
  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

// 回调函数。后台任务执行的线程，不断的从队列中获取任务然后执行，
    void PosixEnv::BackgroundThreadMain() {
        while (true) {
            background_work_mutex_.Lock();

            // Wait until there is work to be done.
            while (background_work_queue_.empty()) {
                background_work_cv_.Wait();
            }

            assert(!background_work_queue_.empty());
            auto background_work_function = background_work_queue_.front().function;
            void* background_work_arg = background_work_queue_.front().arg;
            background_work_queue_.pop();

            background_work_mutex_.Unlock();
            background_work_function(background_work_arg);
        }
    }

void PosixEnv::ScheduleForGarbageCollection(
        void (*background_work_function)(void* background_work_arg),
        void* background_work_arg) {
    background_GlobalCollection_work_mutex_.Lock();


    // If the queue is empty, the background thread may be waiting for work.
    if (background_GlobalCollection_work_queue_.empty()) {
        background_GlobalCollection_work_cv_.Signal();
    }
    // 因为是锁住了 所以可以先 signal 再 emplace。
    background_GlobalCollection_work_queue_.emplace(background_work_function, background_work_arg);
    background_GlobalCollection_work_mutex_.Unlock();
}

// gc 的后台回收任务
void PosixEnv::BackgroundThreadMainGarbageCollection() {
    while (true) {
        background_GlobalCollection_work_mutex_.Lock();

        // Wait until there is work to be done.
        while (background_GlobalCollection_work_queue_.empty()) {
            background_GlobalCollection_work_cv_.Wait();
        }

        assert(!background_GlobalCollection_work_queue_.empty());
        auto background_work_function = background_GlobalCollection_work_queue_.front().function;
        void* background_work_arg = background_GlobalCollection_work_queue_.front().arg;
        background_GlobalCollection_work_queue_.pop();

        background_GlobalCollection_work_mutex_.Unlock();
        background_work_function(background_work_arg);
    }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
// 单例模式
// alignof: 获取按多少字节对齐的信息，decltype：获取类型
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
 // 其实就是 EnvType env_storage_;
 // 但是考虑到了内存的对齐问题
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

Env* Env::Default() {
  // 默认的构造Linux下的 PosixEnv
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb
