#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  char *buf_p = buf;
  MACH_WRITE_UINT32(buf_p, SCHEMA_MAGIC_NUM);
  buf_p += sizeof(uint32_t);
  // 写入column数
  MACH_WRITE_UINT32(buf_p, columns_.size());
  buf_p += sizeof(uint32_t);
  // 循环写入columns
  for(auto column : columns_){
    buf_p += column->SerializeTo(buf_p);
  }
  //写入is_manage
  MACH_WRITE_TO(bool, buf_p, is_manage_);
  buf_p += sizeof(bool);
  return buf_p - buf;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t cnt = 0;
  cnt += sizeof(uint32_t) * 2;
  cnt += sizeof(bool);
  for(auto column : columns_){
    cnt += column->GetSerializedSize();
  }
  return cnt;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  char *buf_p = buf;
  uint32_t magic = MACH_READ_UINT32(buf_p);
  buf_p += sizeof(uint32_t);
  if(magic != SCHEMA_MAGIC_NUM){
    std::cerr << "SCHEMA_MAGIC_NUM error." << std::endl;
  }
  uint32_t column_num = MACH_READ_UINT32(buf_p);
  buf_p += sizeof (uint32_t);
  // 读columns
  std::vector<Column *> columns(column_num);

  for(auto i = 0; i < column_num; i++){
    buf_p += Column::DeserializeFrom(buf_p, columns[i]);
  }
  uint32_t is_manage = MACH_READ_FROM(bool, buf_p);
  buf_p += sizeof(bool);

  if(schema == nullptr){
    schema = new Schema(columns, is_manage);
  }else{
    schema->columns_ = columns;
    schema->is_manage_ = is_manage;
  }
  return buf_p - buf;
}