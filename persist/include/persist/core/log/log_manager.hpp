/**
 * recovery/log_manager.hpp - Persist
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

#ifndef LOG_MANAGER_HPP
#define LOG_MANAGER_HPP

#include <atomic>
#include <memory>

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/log/log_record.hpp>
#include <persist/core/page/log_page/log_page.hpp>
#include <persist/core/storage/base.hpp>
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
      seqNumber; //<- Sequence number of the latest log record. This is used to
                 // set sequence number of the next log record.
  Storage<LogPage> *
      storage PT_GUARDED_BY(lock);      //<- Pointer to backend log storage
  BufferManager<LogPage> bufferManager; //<- Log record buffer manager
  bool started GUARDED_BY(lock);        //<- flag indicating log manager started

public:
  /**
   * @brief Construct a new log manager object.
   *
   * @param storage pointer to backend log sotrage
   * @param cacheSize log buffer cache size
   */
  LogManager(Storage<LogPage> *storage,
             uint64_t cacheSize = DEFAULT_LOG_BUFFER_SIZE)
      : seqNumber(0), started(false), storage(storage),
        bufferManager(storage, cacheSize) {}

  /**
   * @brief Start log manager.
   *
   */
  void start() {
    LockGuard guard(lock);

    if (!started) {
      // Start buffer manager
      bufferManager.start();
      // Load last page in buffer
      PageId lastPageId = storage->getPageCount();
      // Get last sequence number if last page ID is not 0
      if (lastPageId) {
        auto page = bufferManager.get(lastPageId);
        seqNumber = page->getLastSeqNumber();
      }
      // Set state to started
      started = true;
    }
  }

  /**
   * @brief Stop log manager.
   *
   */
  void stop() {
    LockGuard guard(lock);

    if (started) {
      // Stop buffer manager
      bufferManager.stop();
      // Set state to stopped
      started = false;
    }
  }

  /**
   * @brief Add log record to transaction logs
   *
   * @param logRecord reference to the log record object to add
   * @returns sequence number of the added log record
   */
  LogRecord::Location add(LogRecord &logRecord) {
    LockGuard guard(lock);

    // Set log record sequence number
    logRecord.setSeqNumber(++seqNumber);
    // Dump log record bytes
    ByteBuffer data(logRecord.size());
    logRecord.dump(data);

    // Check free space in page and size of log record. If log record larger
    // than free space, then split and store it into multiple page slots. Else
    // store is into a single page slot. Link the slots and insert it into log
    // page.

    // Null page slot representing a virtual previous from first slot
    LogPageSlot nullSlot;
    // Bookkeeping variables
    uint64_t toWriteSize = data.size(), writtenSize = 0;
    // Pointer to previous page slot in the linked list. Begins with
    // pointing to the null slot.
    LogPageSlot *prevSlot = &nullSlot;
    // Start loop to write content in linked record blocks
    while (toWriteSize > 0) {
      // Get a free page
      auto page = bufferManager.getFreeOrNew();
      PageId pageId = page->getId();

      // Create slot to add to page
      LogPageSlot slot(logRecord.getSeqNumber());
      // Compute availble space to write data in page. Here he greedy approach
      // is utilized where all the available free space can be used to store the
      // data. The amount of data that can be stored in the page is the
      // (freeSpace of page) - (fixedSize of page slot).
      uint64_t writeSpace =
          page->freeSpace(LogPage::Operation::INSERT) - slot.fixedSize;
      if (toWriteSize < writeSpace) {
        writeSpace = toWriteSize;
      }
      // Write data to slot and add to page
      slot.data.resize(writeSpace);
      for (int i = 0; i < writeSpace; i++) {
        slot.data[i] = data[writtenSize + i];
      }
      auto inserted = page->insertPageSlot(slot);

      // Create linkage between slots
      LogPageSlot::Location nextLocation(pageId, logRecord.getSeqNumber());
      // TODO: This will cause data race unless a slot level lock is used.
      prevSlot->setNextLocation(nextLocation);

      // Update previous record block and location pointers
      prevSlot = inserted;

      // Update counters
      writtenSize += writeSpace;
      toWriteSize -= writeSpace;
    }

    return nullSlot.getNextLocation();
  }

  /**
   * @brief Get Log record of given sequence number.
   *
   * @param seqNumber sequence number of the log record to get
   * @returns unique pointer to the loaded log record
   */
  std::unique_ptr<LogRecord> get(LogRecord::Location location) {
    LockGuard guard(lock);

    // Get the first page slot from the given location and create the log record
    // by joining all related slots.

    // Byte buffer to read
    ByteBuffer read;
    // Start reading record blocks
    LogPageSlot::Location readLocation = location;
    while (!readLocation.isNull()) {
      // Get page
      auto page = bufferManager.get(readLocation.pageId);
      // Get page slot
      const LogPageSlot &slot = page->getPageSlot(readLocation.seqNumber);
      // Append data stored in slot to output buffer
      read.insert(read.end(), slot.data.begin(), slot.data.end());
      // Update read location to next block
      readLocation = slot.getNextLocation();
    }
    // Return value
    std::unique_ptr<LogRecord> logRecord = std::make_unique<LogRecord>();
    // Load and return log record
    logRecord->load(read);

    return logRecord;
  }

  /**
   * @brief Flush all log records to storage. This method is used by transaction
   * manager when a transaction is committed.
   */
  void flush() {
    LockGuard guard(lock);

    bufferManager.flushAll();
  }
};

} // namespace persist

#endif /* LOG_MANAGER_HPP */
