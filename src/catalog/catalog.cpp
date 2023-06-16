#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}


uint32_t CatalogMeta::GetSerializedSize() const {
    int cnt = 12;
    for (auto iter : table_meta_pages_) {
        cnt += 8;
    }
    for (auto iter : index_meta_pages_) {
        cnt += 8;
    }
  return cnt;
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    if(init){
        catalog_meta_ = CatalogMeta::NewInstance();
    }else{
        Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());
        auto table_cnt = catalog_meta_->GetTableMetaPages()->size();
        auto index_cnt = catalog_meta_->GetIndexMetaPages()->size();
        for(unsigned long i=0; i < table_cnt; i++){
            page_id_t page_id = catalog_meta_->GetTableMetaPages()->at(i);
            Page *page = buffer_pool_manager_->FetchPage(page_id);
            char *table_buf = page->GetData();
            TableMetadata *table_meta;
            TableMetadata::DeserializeFrom(table_buf, table_meta);
            TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId() + 1,
                                                      table_meta->GetSchema(), log_manager_, lock_manager_);
            TableInfo *table_info = TableInfo::Create();
            table_info->Init(table_meta, table_heap);
            table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
            tables_[table_meta->GetTableId()] = table_info;
            buffer_pool_manager_->UnpinPage(page_id, true);
        }
        for(unsigned long i = 0; i < index_cnt; i++){
            page_id_t page_id = catalog_meta_->GetIndexMetaPages()->at(i);
            Page *page = buffer_pool_manager_->FetchPage(page_id);
            char *index_buf = page->GetData();
            IndexMetadata *index_meta;
            IndexMetadata::DeserializeFrom(index_buf, index_meta);
            table_id_t table_id = index_meta->GetTableId();
            TableInfo *table_info = nullptr;
            assert(GetTable(table_id, table_info) == DB_SUCCESS);
            string table_name = table_info->GetTableName();
            IndexInfo *index_info = IndexInfo::Create();
            index_info->Init(index_meta, table_info, buffer_pool_manager_);
            string index_name = index_info->GetIndexName();
            indexes_[i] = index_info;
            index_names_[table_name][index_name] = i;
            buffer_pool_manager_->UnpinPage(page_id, true);
        }
    }
}

CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test**/
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }

}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if(table_names_.find(table_name) != table_names_.end()){
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t table_id = catalog_meta_->GetNextTableId();
  page_id_t page_id;
  buffer_pool_manager_->NewPage(page_id);
  page_id_t first_page_id;
  buffer_pool_manager_->NewPage(first_page_id);
  TableMetadata *table_meta = TableMetadata::Create(++table_id, table_name, first_page_id, schema);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  Page *table_page = buffer_pool_manager_->FetchPage(page_id);
  char *buf = table_page->GetData();
  table_meta->SerializeTo(buf);
  buffer_pool_manager_->FlushPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto find_table = table_names_.find(table_name);
  if(find_table == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }else{
    table_info = tables_[table_names_[table_name]];
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.clear();
  for(const auto &it : table_names_){
    tables.push_back(tables_.at(it.second));
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  if(table_names_.find(table_name) == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = nullptr;
  if(GetTable(table_name, table_info) != DB_SUCCESS) return DB_FAILED;
  Schema *schema = table_info->GetSchema();
  uint32_t col_index = 0;
  std::vector<uint32_t> key_attr;
  for(const auto& col_name : index_keys){
    if(schema->GetColumnIndex(col_name, col_index) == DB_COLUMN_NAME_NOT_EXIST){
      return DB_COLUMN_NAME_NOT_EXIST;
    }else{
      key_attr.push_back(col_index);
    }
  }
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end()){
    return DB_INDEX_ALREADY_EXIST;
  }

  page_id_t page_id;
  buffer_pool_manager_->NewPage(page_id);
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  IndexMetadata *index_meta = IndexMetadata::Create(++index_id, index_name, table_id, key_attr);
  IndexInfo *new_index = IndexInfo::Create();
  new_index->Init(index_meta, table_info, buffer_pool_manager_);
  index_info = new_index;
  indexes_[index_id] = new_index;
  index_names_[table_name][index_name] = index_id;
  catalog_meta_->index_meta_pages_[index_id] = page_id;

  Page* index_page = buffer_pool_manager_->FetchPage(page_id);
  char* buf = index_page->GetData();
  index_meta->SerializeTo(buf);
  buffer_pool_manager_->FlushPage(page_id);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(index_names_.find(table_name) == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto index_map = index_names_.at(table_name);
  if(index_map.find(index_name) != index_map.end()){
    auto index_id = index_map.at(index_name);
    index_info = indexes_.at(index_id);
    return DB_SUCCESS;
  }
  return DB_INDEX_NOT_FOUND;
}


dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  indexes.clear();
  if(index_names_.count(table_name) == 0){
    return DB_TABLE_NOT_EXIST;
  }
  auto map = index_names_.at(table_name);
  for(auto & iter : map){
    std::string index_name = iter.first;
    auto index_id = index_names_.at(table_name).at(index_name);
    indexes.push_back(indexes_.at(index_id));
  }
  return DB_SUCCESS;
}


dberr_t CatalogManager::DropTable(const string &table_name) {
  TableInfo* table_info = nullptr;
  if(GetTable(table_name, table_info) != DB_SUCCESS){
    return DB_TABLE_NOT_EXIST;
  }
  else{
    table_id_t table_id = table_info->GetTableId();
    page_id_t page_id = table_info->GetRootPageId();
    buffer_pool_manager_->DeletePage(page_id);
    table_names_.erase(table_name);
    tables_.erase(table_id);
    catalog_meta_->table_meta_pages_.erase(table_id);
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  IndexInfo* index_info = nullptr;
  TableInfo* table_info = nullptr;
  if(GetTable(table_name, table_info) != DB_SUCCESS ){
    return DB_TABLE_NOT_EXIST;
  }
  if(GetIndex(table_name, index_name, index_info) != DB_SUCCESS){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = index_names_[table_name][index_name];
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  buffer_pool_manager_->DeletePage(page_id);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* meta = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char* buf = reinterpret_cast<char *>(meta);
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID); // 暂时注释掉，可能导致重复写入的问题
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement 这是干啥的
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement 这是干啥的
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id) == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }else{
    table_info = tables_[table_id];
    return DB_SUCCESS;
  }
}