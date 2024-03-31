//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // We allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  Page *page;
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    // Get frame id from free list
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else {
    // Get frame id from replacer
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    page = pages_ + frame_id;
  }
  // Write back dirty page
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    // Clean
    page->is_dirty_ = false;
  }
  // Alloc a page id
  *page_id = AllocatePage();
  // Delete old map
  page_table_.erase(page->GetPageId());
  // Add new map
  page_table_.emplace(*page_id, frame_id);
  // Set page id
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  // Update replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  latch_.unlock();
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  // If exist
  if (page_table_.find(page_id) != page_table_.end()) {
    // Get page
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // Uodate replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // Update pin count
    page->pin_count_ += 1;
    return page;
  }
  // If not exist
  Page *page;
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    // Get frame id from free list
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else {
    // Get frame id from replacer
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    page = pages_ + frame_id;
  }
  // Write back dirty page
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    // Clean
    page->is_dirty_ = false;
  }
  // Erase old map
  page_table_.erase(page->GetPageId());
  // Add new map
  page_table_.emplace(page_id, frame_id);
  // Update page
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  // Update replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  // Read page from disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  // If exist
  if (page_table_.find(page_id) != page_table_.end()) {
    // Get page
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // Set dirty
    if (is_dirty) {
      page->is_dirty_ = is_dirty;
    }
    // If pin count is 0
    if (page->GetPinCount() == 0) {
      return false;
    }
    // Decrement pin count
    page->pin_count_ -= 1;
    if (page->GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    latch_.unlock();
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // Get page
  auto page = pages_ + page_table_[page_id];
  // Write back
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  // Clean
  page->is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    auto page = pages_ + i;
    if (page->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    // Write back
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  // Page exist
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    // Get page
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // If can not delete
    if (page->GetPinCount() > 0) {
      return false;
    }
    // Delete page
    page_table_.erase(page_id);
    free_list_.push_back(frame_id);
    replacer_->Remove(frame_id);
    // Reset page memory
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }
  DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
