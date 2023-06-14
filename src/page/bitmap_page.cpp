#include "page/bitmap_page.h"

#include "glog/logging.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  uint32_t byte_num = page_offset / 8;
  uint32_t bit_num = page_offset % 8;
  auto page_num = GetMaxSupportedSize();
  if(page_offset == GetMaxSupportedSize() || (bytes[byte_num] >> bit_num) & 1){
    for(uint32_t i = 0; i < page_num; i++){
      if(IsPageFree(i)){
        page_offset = i;
        bytes[i / 8] |= 1 << (i % 8);
        page_allocated_++;
        return true;
      }
    }
    return false;
  }
  bytes[byte_num] |= 1 << bit_num;
  page_allocated_++;
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  int byte_num = page_offset / 8;
  int bit_num = page_offset % 8;
  if((bytes[byte_num] >> bit_num) & 1){
    bytes[byte_num] ^= 1 << bit_num;
    page_allocated_--;
    return true;
  }else return false;
}


template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  int byte_num = page_offset / 8;
  int bit_num = page_offset % 8;
  return !((bytes[byte_num] >> bit_num) & 1);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] >> bit_index) & 1;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;