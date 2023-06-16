#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  uint32_t row_size = row.GetSerializedSize(schema_);
  if(row_size + 32 > PAGE_SIZE) return false;
  auto cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if(cur_page == nullptr) return false;
  cur_page->WLatch();
  //RowId rid(cur_page->GetPageId(), i);
  //row.SetRowId(rid);
  while(!cur_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
    auto next_page_id = cur_page->GetNextPageId();
    if(next_page_id != INVALID_PAGE_ID){
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
      cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      cur_page->WLatch();
    }else{
      auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page == nullptr){
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
        return false;
      }else{
        new_page->WLatch();
        cur_page->SetNextPageId(next_page_id);
        new_page->Init(next_page_id, cur_page->GetPageId(), log_manager_, txn);
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
        cur_page = new_page;
      }
    }
  }
  cur_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page == nullptr) return false;
  page->WLatch();
  Row pre_row(rid);
  int flag = page->UpdateTuple(row, &pre_row, schema_, txn, lock_manager_, log_manager_);
  switch(flag){
    case 0:
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      break;
    case 1: // slotID越界，返回错误，不更新
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return false;
    case 2: // 标记删除/物理删除，不更新
      page->ApplyDelete(rid, txn, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      break;
    case 3:
      page->ApplyDelete(rid, txn, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      InsertTuple(row, txn);
      break;
  }
  return true;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr, "page is null!");
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page == nullptr) return false;
  page->RLatch();
  bool success = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), success);
  return success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->RLatch();
  RowId rid;
  page->GetFirstTupleRid(&rid);
  Row row(rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, row, txn);
}

TableIterator TableHeap::End() {
  Row row;
  return TableIterator(this, row, static_cast<Transaction * >(nullptr));
}
