#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  if(item_index == page->GetSize()-1){  // 最后一个
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id != INVALID_PAGE_ID){  // 下一页非空
      item_index = 0;
      Page *next_page = buffer_pool_manager->FetchPage(next_page_id);
      page = reinterpret_cast<LeafPage *>(next_page->GetData());
    }else{  // 下一页为空
      item_index++;
    }
  }else{
    item_index++;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}