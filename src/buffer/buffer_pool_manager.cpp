#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  frame_id_t frame_id;
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  if(page_table_.count(page_id) != 0){
    frame_id = page_table_[page_id];
    Page *p = pages_ + frame_id;
    p->pin_count_++;
    replacer_->Pin(frame_id);
    return p;
  }else{
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    if(free_list_.empty()){
      if(!replacer_->Victim(&frame_id)) return nullptr ;
    }else{
      frame_id = free_list_.front();
      free_list_.pop_front();
    }

    // 2.     If R is dirty, write it back to the disk.
    Page *r = pages_ + frame_id;
    if(r->is_dirty_){
      disk_manager_->WritePage(r->page_id_, r->data_);
    }

    // 3.     Delete R from the page table and insert P.
    page_table_.erase(page_id);
    page_table_[page_id] = frame_id;

    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    disk_manager_->ReadPage(page_id, r->data_);
    r->page_id_ = page_id;
    r->pin_count_++;
    r->is_dirty_ = false;
    replacer_->Pin(frame_id);
    return r;
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!


  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  size_t i;
  for(i=0; i<pool_size_; i++){
    if(pages_[i].pin_count_ == 0){
      break;
    }
  }
  if(i == pool_size_) return nullptr ;

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t victim_frame_id;
  if(!free_list_.empty()){
    victim_frame_id = free_list_.front();
    free_list_.pop_front();
  }else{
    if(!replacer_->Victim(&victim_frame_id)) return nullptr;
  }

  page_id_t new_page = AllocatePage();
  Page *p = pages_ + victim_frame_id;
  if(p->is_dirty_){
    p->is_dirty_ = false;
    disk_manager_->WritePage(p->page_id_, p->data_);
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  p->ResetMemory();
  page_table_.erase(p->page_id_);
  page_table_[new_page] = victim_frame_id;
  p->page_id_ = new_page;
  p->pin_count_++;
  p->is_dirty_ = false;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = new_page;
  return p;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  disk_manager_->DeAllocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if(page_table_.count(page_id) == 0){
    return true;
  }else {
    frame_id_t frame_id = page_table_[page_id];
    Page *p = pages_ + frame_id;
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if (p->pin_count_) return false;
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    page_table_.erase(page_id);
    p->pin_count_ = 0;
    p->page_id_ = INVALID_PAGE_ID;
    p->is_dirty_ = false;
    free_list_.push_back(frame_id);
    return true;
  }
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.count(page_id) == 0) return true;
  frame_id_t frame_id = page_table_[page_id];
  Page *p = pages_ + frame_id;
  // synchronize dirty for p
  p->is_dirty_ |= is_dirty;
  if(p->pin_count_ == 0){
    return false ;
  }else if(--p->pin_count_ == 0){
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.count(page_id) == 0) return false;
  frame_id_t frame_id = page_table_[page_id];
  Page *p = pages_ + frame_id;
  p->is_dirty_ = false;
  disk_manager_->WritePage(p->page_id_, p->data_);
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}