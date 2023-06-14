#include "record/column.h"
#include <iostream>

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  char *buf_p = buf;
  MACH_WRITE_UINT32(buf_p, COLUMN_MAGIC_NUM); // 魔数
  buf_p += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf_p, name_.length()); // 写入name长度
  buf_p += sizeof(uint32_t);
  MACH_WRITE_STRING(buf_p, name_);  // 写入name
  buf_p += name_.length();
  MACH_WRITE_TO(TypeId, buf_p, type_);  // 写入type
  buf_p += sizeof(TypeId);
  MACH_WRITE_UINT32(buf_p, len_); // 写入len
  buf_p += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf_p, table_ind_); // 写入table_ind
  buf_p += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf_p, nullable_);
  buf_p += sizeof(bool);
  MACH_WRITE_TO(bool, buf_p, unique_);
  buf_p += sizeof(bool);
  return buf_p - buf;
}

uint32_t Column::GetSerializedSize() const {
  uint32_t cnt = 0;
  cnt += 4 * sizeof(uint32_t);
  cnt += name_.length();
  cnt += sizeof(TypeId);
  cnt += 2 * sizeof(bool);
  return cnt;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  char *buf_p = buf;
  // 读魔数
  uint32_t magic = MACH_READ_UINT32(buf_p);
  buf_p += sizeof(uint32_t);
  if(magic != COLUMN_MAGIC_NUM){
    std::cerr<<"COLUMN_MAGIC_NUM error" << std::endl;
    return 0;
  }
  // 读name长度
  uint32_t name_len = MACH_READ_UINT32(buf_p);
  buf_p += sizeof(uint32_t);
  // 读name
  char name[name_len+1];
  memcpy(name, buf_p, name_len);
  name[name_len] = '\0';
  buf_p += name_len;
  // 读type
  TypeId type = MACH_READ_FROM(TypeId, buf_p);
  buf_p += sizeof(TypeId);
  // 读len
  uint32_t len = MACH_READ_UINT32(buf_p);
  buf_p += sizeof(uint32_t);
  // 读table_ind
  uint32_t table_ind = MACH_READ_UINT32(buf_p);
  buf_p += sizeof(uint32_t);
  // 读nullable
  bool nullable = MACH_READ_FROM(bool, buf_p);
  buf_p += sizeof(bool);
  // 读unique
  bool unique = MACH_READ_FROM(bool, buf_p);
  buf_p += sizeof(bool);
  if(type == TypeId::kTypeChar){
    column = new Column(name, type, len, table_ind, nullable, unique);
  }else{
    column = new Column(name, type, table_ind, nullable, unique);
  }
  return buf_p - buf;
}
