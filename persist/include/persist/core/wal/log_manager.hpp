/**
 * log/log_manager.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef PERSIST_CORE_LOG_MANAGER_HPP
#define PERSIST_CORE_LOG_MANAGER_HPP

#include <atomic>
#include <memory>

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/page/log_page/page.hpp>
#include <persist/core/wal/log_record.hpp>

#include <persist/utility/mutex.hpp>

namespace persist {

/**
 * @brief Log Manager Class
 *
 * The log manager handles collection of log records for all transactions.
 */
class LogManager {
  PERSIST_PRIVATE
  /**
   * @brief Recursive lock for thread safety
   *
   */
  // TODO: Need granular locking
  typedef typename persist::Mutex<std::recursive_mutex> Mutex;
  Mutex lock; //<- lock for achieving thread safety via mutual exclusion
  typedef typename persist::LockGuard<Mutex> LockGuard;

  std::atomic<SeqNumber>
      seq_number; //<- Sequence number of the latest log record. This is used to
  // set sequence number of the next log record.
  PageId last_page_id; //<- Identifer of the last page in the backend storage of
                       // the log.
  Storage<LogPage> *
      storage PT_GUARDED_BY(lock);       //<- Pointer to backend log storage
  BufferManager<LogPage> buffer_manager; //<- Log record buffer manager
  bool started GUARDED_BY(lock); //<- flag indicating log manager started

public:
  /**
   * @brief Construct a new log manager object.
   *
   * @param storage pointer to backend log sotrage
   * @param cacheSize log buffer cache size
   */
  LogManager(Storage<LogPage> *storage,
             uint64_t cacheSize = DEFAULT_LOG_BUFFER_SIZE)
      : seq_number(0), started(false), storage(storage),
        buffer_manager(storage, cacheSize) {}

  /**
   * @brief Start log manager.
   *
   * @thread_unsafe The method is not thread safe as it is expected that the
   * user starts the log manager before spawning any threads.
   *
   */
  void Start() NO_THREAD_SAFETY_ANALYSIS {
    LockGuard guard(lock);

    if (!started) {
      // Start buffer manager
      buffer_manager.Start();
      // Load last page in buffer
      last_page_id = storage->GetPageCount();
      // Get last sequence number if last page ID is not 0
      if (last_page_id) {
        auto page = buffer_manager.Get(last_page_id);
        seq_number = page->GetLastSeqNumber();
      }
      // Set state to started
      started = true;
    }
  }

  /**
   * @brief Stop log manager.
   *
   * @thread_unsafe The method is not thread safe as it is expected that the
   * user stops the log manager after joining all the threads.
   *
   */
  void Stop() NO_THREAD_SAFETY_ANALYSIS {
    LockGuard guard(lock);

    if (started) {
      // Stop buffer manager
      buffer_manager.Stop();
      // Set state to stopped
      started = false;
    }
  }

  /**
   * @brief Add log record to transaction logs
   *
   * @thread_safe
   *
   * @param log_record reference to the log record object to add
   * @returns sequence number of the added log record
   */
  LogRecord::Location Add(LogRecord &log_record) {
    LockGuard guard(lock);

    // Set log record sequence number
    log_record.SetSeqNumber(++seq_number);
    // Dump log record bytes
    ByteBuffer data(log_record.GetStorageSize());
    log_record.Dump(data);

    // Check free space in page and size of log record. If the log record is
    // larger than the free space, split and store it into multiple page slots.
    // Else store it in a single page slot. Link the slots and insert it into
    // the log page.

    // Null page slot representing a virtual previous from first slot
    LogPageSlot null_slot;
    // Bookkeeping variables
    size_t to_write_size = data.size(), written_size = 0;
    // Pointer to previous page slot in the linked list. Begins with
    // pointing to the null slot.
    LogPageSlot *prev_slot = &null_slot;
    // Start loop to write content in linked record blocks
    while (to_write_size > 0) {
      // Get a free page
      auto page = buffer_manager.GetFreeOrNew<LogPage>();
      PageId page_id = page->GetId();

      // Create slot to add to page
      LogPageSlot slot(log_record.GetSeqNumber());
      // Compute availble space to write data in page. Here the greedy approach
      // is utilized where all the available free space can be used to store the
      // data. The amount of data that can be stored in the page is the
      // (freeSpace of page) - (fixedSize of page slot).
      size_t write_space = page->GetFreeSpaceSize(Operation::INSERT) -
                           (slot.GetStorageSize() - slot.data.size());
      if (to_write_size < write_space) {
        write_space = to_write_size;
      }
      // Write data to slot and add to page
      slot.data.resize(write_space);
      for (int i = 0; i < write_space; i++) {
        slot.data[i] = data[written_size + i];
      }
      auto inserted = page->InsertPageSlot(slot);

      // Create linkage between slots
      LogPageSlot::Location next_location(page_id, log_record.GetSeqNumber());
      // TODO: This will cause data race unless a slot level lock is used.
      prev_slot->SetNextLocation(next_location);

      // Update previous record block and location pointers
      prev_slot = inserted;

      // Update counters
      written_size += write_space;
      to_write_size -= write_space;
    }

    return null_slot.GetNextLocation();
  }

  /**
   * @brief Get Log record of given sequence number.
   *
   * @thread_safe
   *
   * @param seq_number sequence number of the log record to get
   * @returns unique pointer to the loaded log record
   */
  std::unique_ptr<LogRecord> Get(LogRecord::Location location) {
    LockGuard guard(lock);

    // Get the first page slot from the given location and create the log record
    // by joining all related slots.

    // Byte buffer to read
    ByteBuffer read;
    // Start reading record blocks
    LogPageSlot::Location read_location = location;
    while (!read_location.IsNull()) {
      // Get page
      auto page = buffer_manager.Get(read_location.page_id);
      // Get page slot
      const LogPageSlot &slot = page->GetPageSlot(read_location.seq_number);
      // Append data stored in slot to output buffer
      read.insert(read.end(), slot.data.begin(), slot.data.end());
      // Update read location to next block
      read_location = slot.GetNextLocation();
    }
    // Return value
    std::unique_ptr<LogRecord> log_record = std::make_unique<LogRecord>();
    // Load and return log record
    log_record->Load(read);

    return log_record;
  }

  /**
   * @brief Flush all log records to storage. This method is used by transaction
   * manager when a transaction is committed.
   *
   * @thread_safe
   */
  void Flush() {
    LockGuard guard(lock);

    buffer_manager.FlushAll();
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Get the latest sequence number
   *
   * @return Current sequence number
   */
  SeqNumber GetSeqNumber() const { return seq_number; }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_LOG_MANAGER_HPP */
