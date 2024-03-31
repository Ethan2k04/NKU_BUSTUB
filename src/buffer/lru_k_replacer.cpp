//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // Lock the thread
  latch_.lock();

  // Process of eviction
  bool victim_found = false;

  // Try to find victim in history list
  if (!hist_list_.empty()) {
    for (auto rit = hist_list_.rbegin(); rit != hist_list_.rend(); ++rit) {
      if (node_store_[*rit].is_evictable_) {
        *frame_id = *rit;
        hist_list_.erase(std::next(rit).base());
        victim_found = true;
        break;
      }
    }
  }

  // Try to find victim in cache list
  if (!victim_found && !cache_list_.empty()) {
    for (auto rit = cache_list_.rbegin(); rit != cache_list_.rend(); ++rit) {
      if (node_store_[*rit].is_evictable_) {
        *frame_id = *rit;
        cache_list_.erase(std::next(rit).base());
        victim_found = true;
        break;
      }
    }
  }

  // Unlock the thread
  latch_.unlock();

  // Return the result of eviction
  if (victim_found) {
    node_store_.erase(*frame_id);
    --curr_size_;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // Lock the thread
  latch_.lock();

  // Check if frame_id is valid
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalid frame_id") + std::to_string(frame_id));
  }

  // Implementation of LRU-K logic
  size_t new_count = ++node_store_[frame_id].k_;
  if (new_count == 1) {
    // ++curr_size_;
    hist_list_.emplace_front(frame_id);
    node_store_[frame_id].pos_ = hist_list_.begin();
  } else {
    if (new_count == k_) {
      hist_list_.erase(node_store_[frame_id].pos_);
      cache_list_.emplace_front(frame_id);
      node_store_[frame_id].pos_ = cache_list_.begin();
    } else if (new_count > k_) {
      cache_list_.erase(node_store_[frame_id].pos_);
      cache_list_.emplace_front(frame_id);
      node_store_[frame_id].pos_ = cache_list_.begin();
    }
  }

  // Unlock the thread
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // Lock the thread
  latch_.lock();

  // Check if frame_id is valid
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalid frame_id") + std::to_string(frame_id));
  }

  // Decide whether to set a frame evictable
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  if (set_evictable && !node_store_[frame_id].is_evictable_) {
    ++curr_size_;
  } else if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    --curr_size_;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;

  // Unlock the thread
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // Lock the thread
  latch_.lock();

  // Find the frame first, if not exist then return
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  // If not evictable, throw an exception
  if (!node_store_[frame_id].is_evictable_) {
    throw std::logic_error(std::string("Can't remove a inevictable frame ") + std::to_string(frame_id));
  }

  // Decide where to remove a frame
  if (node_store_[frame_id].k_ < k_) {
    hist_list_.erase(node_store_[frame_id].pos_);
  } else {
    cache_list_.erase(node_store_[frame_id].pos_);
  }

  // Remove the record from the node_store_
  --curr_size_;
  node_store_.erase(frame_id);

  // Unlock the thread
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
