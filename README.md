**LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string keys to string values. KVDB is a key-value storage library base on leveldb, we separate the value and key when it is needed**


# 特点
  * keys 和 value 的分离可根据阈值进行动态的分离，即在KVDB中，可实时的根据不同的业务需求进行分离阈值的调整。
  * 对于小于阈值的key-value 对，并没有进行分离，而是采用leveldb一样的存放的方式，都是放入sst当中。
  * 在leveldb的基础上增加了垃圾回收的机制，垃圾回收的线程在后台进行，KVDB运行的线程的优先级是 主线程 > Compact 线程 > 后台垃圾回收线程。
  * 垃圾回收支持自动在线回收和手动回收，同时在打开数据的时候也可选择是否要进行垃圾回收。
  * 若是没有开启在线垃圾回收，将支持快照，若是在数据库运行期间发生了快照，那么快照以后的数据都不进行垃圾回收。
  * 可进行手动设置 每一个valuelog的大小。
  * levelDB 具有写放大和读放大，KVDB 因为kv分离了，可减少写放大和读放大，提高运行的效率。

# 数据组织形式
      record :
      checksum: uint32     // crc32c of type and data[] ; little-endian
      length: uint16       // record的长度
      Sequence: uint64     // record 中的第一对kv所对应的sequence
      kv Number：uint16    // 这个record 中所具有的kv对的数量 
      data:                //  保存的数据

      log中的data 的数据格式为：
      data：
           type:           uint8         // 是否要进行kv分离
           keysize:        uint16        // key的长度    
           key:                          // key值
           valuesize:      uint16        // value 的长度
           value:                        // value值

      data中的value的数据格式为：  
      若是不分离的则value保存的是存入的值，如是需要分离的则保存为以下的格式     
      value:
          file number:    uint64        // kv对需要存入的log的编号
          offset:         uint64        // 这个kv对的起始位置在log中的偏移，方便get的时候读取，采用随机读取方式
          kv_value_size:  uint64        // 指record中第一个kv对开始到该kv对的偏移

![image](https://github.com/Buildings-Lei/kv-separate/blob/main/image/format.png)

# 写流程
在原本leveldb的基础上加上了value的大小判断，若是大于设定的阈值，将会进行kv分离，
写入batch中。 若是小于阈值，则将会按照leveldb中原本的方式进行存储。
  
# 读流程
先从mem，imm和LSM 中查找到key值对应的value的值，解析出来后，若是已经kv分离的话，value保存的是log文件编号，偏移地址，以及value的大小。查找到的值若是不分离的，直接返回，若是分离的，则需要根据value的值，进一步去log文件中查找。

# garbage collection 
创建一个独立后台线程，对需删除的vlog文件中的无效 kv 进行删除，有效 kv重新存储到数据库中。利用管理类决定哪些vlog文件需删除。重新存储方案：为保证 有效 kv 重新存储的正确性，不影响后续 kv 对的正确存储，采用预分配时间序列的策略，在具体回收前，先为其分配一段不影响查找准确性的seq给有效kv对。
# 性能测试

 ## 测试环境
 创建一个5万条kv记录的数据库，其中每条记录的key为16个字节，value为 1 M ，分离的阈值为 1 M，不开启snappy压缩，所以写入磁盘的文件大小就是数据的原始大小。
 
    LevelDB:    version 1.23
    CPU:        8 * AMD Ryzen 7 4800H with Radeon Graphics
    CPUCache:   512 KB
    Keys:       16 bytes each
    Values:     1048576 bytes each (524288 bytes after compression)
    Entries:    50000
    Raw Size:   50000.8 MB (estimated)
    File Size:  25000.8 MB (estimated)
    WARNING: Snappy compression is not enabled

This project supports [CMake](https://cmake.org/) out of the box.

### 写性能

leveldb
 
    fillseq      :       5.549 micros/op;   19.9 MB/s     
    fillrandom   :      11.597 micros/op;    9.5 MB/s     
    overwrite    :      14.144 micros/op;    7.8 MB/s
 
KVDB
 
    fillseq      :       2069.794 micros/op;   483.1 MB/s     
    fillrandom   :       2118.600 micros/op;   472.0 MB/s     
    overwrite    :       2123.194 micros/op;   471.0 MB/s

### 读性能

leveldb

  readrandom   :       6.219 micros/op; (1000000 of 1000000 found)   
  readrandom   :       5.026 micros/op; (1000000 of 1000000 found)     
  readseq      :       0.531 micros/op;  208.4 MB/s
  compact      : 2189861.000 micros/op;
  readrandom   :       3.718 micros/op; (1000000 of 1000000 found)
  readseq      :       0.477 micros/op;  231.7 MB/s
  fill100K     :    3313.683 micros/op;   28.8 MB/s (1000 ops)

KVDB

  readrandom   :       3926.204 micros/op; (43405 of 50000 found)     
  readrandom   :       4732.702 micros/op; (43374 of 50000 found)
  readseq      :       1.285 micros/op;   18.6 MB/s
  compact      :  218021.000 micros/op;
  readrandom   :       5023.581 micros/op; (43318 of 50000 found)
  readseq      :       0.653 micros/op;   36.4 MB/s
  fill100K     :     12432.440 micros/op;  7.7 MB/s (1000 ops)

### Build for POSIX

Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```

### Building for Windows

First generate the Visual Studio 2017 project/solution files:

```cmd
mkdir build
cd build
cmake -G "Visual Studio 15" ..
```
The default default will build for x86. For 64-bit run:

```cmd
cmake -G "Visual Studio 15 Win64" ..
```

To compile the Windows solution from the command-line:

```cmd
devenv /build Debug leveldb.sln
```

or open leveldb.sln in Visual Studio and build from within.

Please see the CMake documentation and `CMakeLists.txt` for more advanced usage.


## Repository contents

See [doc/index.md](doc/index.md) for more explanation. See
[doc/impl.md](doc/impl.md) for a brief overview of the implementation.

The public interface is in include/leveldb/*.h.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Guide to header files:

* **include/leveldb/db.h**: Main interface to the DB: Start here.

* **include/leveldb/options.h**: Control over the behavior of an entire database,
and also control over the behavior of individual reads and writes.

* **include/leveldb/comparator.h**: Abstraction for user-specified comparison function.
If you want just bytewise comparison of keys, you can use the default
comparator, but clients can write their own comparator implementations if they
want custom ordering (e.g. to handle different character encodings, etc.).

* **include/leveldb/iterator.h**: Interface for iterating over data. You can get
an iterator from a DB object.

* **include/leveldb/write_batch.h**: Interface for atomically applying multiple
updates to a database.

* **include/leveldb/slice.h**: A simple module for maintaining a pointer and a
length into some other byte array.

* **include/leveldb/status.h**: Status is returned from many of the public interfaces
and is used to report success and various kinds of errors.

* **include/leveldb/env.h**:
Abstraction of the OS environment.  A posix implementation of this interface is
in util/env_posix.cc.

* **include/leveldb/table.h, include/leveldb/table_builder.h**: Lower-level modules that most
clients probably won't use directly.
