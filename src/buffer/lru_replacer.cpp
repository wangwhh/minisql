#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){
  lru_max_size = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_map.empty()){
    frame_id = nullptr;
    return false ;
  }else{
    frame_id_t del = lru_frame_id.back();
    lru_map.erase(del);
    lru_frame_id.pop_back();
    *frame_id = del;
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_map.count(frame_id) != 0) {
    lru_frame_id.erase(lru_map[frame_id]);
    lru_map.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(lru_map.count(frame_id) == 0){
    while(lru_map.size() >= lru_max_size){
      frame_id_t del = lru_frame_id.back();
      lru_map.erase(del);
      lru_frame_id.pop_back();
    }
    lru_frame_id.push_front(frame_id);
    lru_map[frame_id] = lru_frame_id.begin();
  }
}

size_t LRUReplacer::Size() {
  return lru_map.size();
}