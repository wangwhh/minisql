#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {

}

TableIterator::TableIterator(TableHeap *table_heap, Row row_, Transaction *txn_) {
  tableHeap = table_heap;
  row = row_;
  txn = txn_;
  if(row.GetRowId().GetPageId() != INVALID_PAGE_ID)
    table_heap->GetTuple(&row, txn);
}

TableIterator::TableIterator(const TableIterator &other) {
  tableHeap = other.tableHeap;
  row = other.row;
  txn = other.txn;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (this->row.GetRowId() == itr.row.GetRowId()) && (this->tableHeap == itr.tableHeap) && (this->txn == itr.txn);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  assert(*this != tableHeap->End());
  return row;
}

Row *TableIterator::operator->() {
  assert(*this != tableHeap->End());
  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->tableHeap = itr.tableHeap;
  this->row = itr.row;
  this->txn = itr.txn;
}

// ++iter
TableIterator &TableIterator::operator++() {
  BufferPoolManager *bufferPoolManager = tableHeap->buffer_pool_manager_;
  auto cur_page = static_cast<TablePage *>(bufferPoolManager->FetchPage(row.GetRowId().GetPageId()));
  assert(cur_page != nullptr);
  cur_page->RLatch();

  RowId next_row_id;
  if(!cur_page->GetNextTupleRid(row.GetRowId(), &next_row_id)){
    while(cur_page->GetNextPageId() != INVALID_PAGE_ID){
      auto next_page = static_cast<TablePage *>(bufferPoolManager->FetchPage(cur_page->GetNextPageId()));
      cur_page->RUnlatch();
      bufferPoolManager->UnpinPage(cur_page->GetPageId(), false);
      cur_page = next_page;
      cur_page->RLatch();
      if(cur_page->GetFirstTupleRid(&next_row_id)) break;
    }
  }
  row.SetRowId(next_row_id);
  if(*this != tableHeap->End()){
    tableHeap->GetTuple(&row, txn);
  }
  cur_page->RUnlatch();
  bufferPoolManager->UnpinPage(cur_page->GetPageId(), false);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableHeap *ret_table_heap = tableHeap;
  Row ret_row = row;
  Transaction *ret_txn = txn;
  ++(*this);
  return TableIterator(ret_table_heap, ret_row, ret_txn);
}
