#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  char *buf_p = buf;
  // 写入rid
  MACH_WRITE_TO(RowId, buf_p, rid_);
  buf_p += sizeof(RowId);
  // 写入fields数
  MACH_WRITE_UINT32(buf_p, fields_.size());
  buf_p += sizeof(uint32_t);
  // 写入fields
  for(auto field: fields_){
    buf_p += field->SerializeTo(buf_p);
  }
  return buf_p - buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  fields_.clear();
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  char *buf_p = buf;
  // 读rid
  rid_ = MACH_READ_FROM(RowId, buf_p);
  buf_p += sizeof(RowId);
  // 读field数
  uint32_t field_num = MACH_READ_UINT32(buf_p);
  buf_p += sizeof(uint32_t);
  // 读fields
  while(fields_.size() < schema->GetColumnCount()){
    fields_.push_back(nullptr);
  }
  int i=0;
  for(auto column : schema->GetColumns()){
    fields_[i] = new Field(column->GetType());
    buf_p += Field::DeserializeFrom(buf_p, column->GetType(), &fields_[i++], false);
  }

  return buf_p - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t cnt = sizeof(RowId) + sizeof(uint32_t);
  for(auto field: fields_){
    cnt += field->GetSerializedSize();
  }
  return cnt;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
