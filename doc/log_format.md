leveldb Log format
==================
Each block consists of a sequence of records:

    block := record* trailer?
    record :=
      checksum: uint32     // crc32c of type and data[] ; little-endian
      length: uint16       // record的长度
      Sequence: uint64     // record 中的第一对kv所对应的sequence
      kv Number：uint16    // 这个record 中所具有的kv对的数量 
      data:               //  保存的数据

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