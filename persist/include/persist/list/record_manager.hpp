/**
 * list/record_manager.hpp - Persist
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

#ifndef PERSIST_LIST_RECORD_MANAGER_HPP
#define PERSIST_LIST_RECORD_MANAGER_HPP

#include <persist/core/common.hpp>
#include <persist/core/exceptions/record.hpp>
#include <persist/core/record_manager.hpp>

namespace persist {

/**
 * @brief List collection record manager.
 *
 * @tparam RecordType Data record type.
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 */
template <class RecordType, class ReplacerType, class FreeSpaceManagerType>
class ListRecordManager
    : public RecordManager<RecordType, ReplacerType, FreeSpaceManagerType> {
  using RecordManager<RecordType, ReplacerType,
                      FreeSpaceManagerType>::page_manager;

  PERSIST_PRIVATE
  /**
   * @brief Insert bytes as doubly linked page slots in storage. This method is
   * used for inserting and in-place updating of records stored in backend
   * storage.
   *
   * @param txn Reference to transaction object.
   * @param span Constant reference to span of byte buffer to insert.
   * @param location Constant reference to previous linked page slot
   * location. By default this is set to the NULL location.
   * @returns Location of the first page slot containing complete or part of the
   * byte buffer.
   */
  RecordPageSlot::Location
  Insert(Transaction &txn, Span span,
         RecordPageSlot::Location location = RecordPageSlot::Location()) {
    // Null slot representing the previous from first slot
    RecordPageSlot null_slot;

    // Bookkeeping variables
    uint64_t to_write_size = span.size, written_size = 0;
    // Pointer to previous slot in the doubly linked list. Begins with
    // pointing to the null slot.
    RecordPageSlot *prev_slot = &null_slot;
    // Pointer to previous slot location.
    const RecordPageSlot::Location *prev_location = &location;
    // Start loop to write content in linked slots
    while (to_write_size > 0) {
      // Get a free page
      auto page = page_manager.GetFreeOrNewPage(to_write_size);
      PageId page_id = page->GetId();

      // Create slot to add to page
      RecordPageSlot slot;
      // Compute availble space to write data in page. Here the greedy approach
      // is utilized where all the available free space can be used to store the
      // data. The amount of data that can be stored in the page is the
      // (freeSpace of page) - (fixedSize of page slot).
      size_t write_space = page->GetFreeSpaceSize(Operation::INSERT) -
                           (slot.GetStorageSize() - slot.data.size());
      if (to_write_size < write_space) {
        write_space = to_write_size;
      }
      // Pointer to one past element already written
      Byte *pos = span.start + written_size;
      // Write data to slot and add to page
      slot.data.resize(write_space);
      for (int i = 0; i < write_space; i++) {
        slot.data[i] = *(pos + i);
      }
      auto inserted = page->InsertPageSlot(slot, txn);

      // Create double linkage between slots
      RecordPageSlot::Location next_location(page_id, inserted.first);
      prev_slot->SetNextLocation(next_location);
      inserted.second->SetPrevLocation(*prev_location);

      // Update previous slot and location pointers
      prev_location = &prev_slot->GetNextLocation();
      prev_slot = inserted.second;

      // Update counters
      written_size += write_space;
      to_write_size -= write_space;
    }

    return null_slot.GetNextLocation();
  }

  /**
   * @brief
   *
   * @param txn
   * @param location
   */
  void Remove(Transaction &txn, const RecordPageSlot::Location &location) {}

public:
  /**
   * @brief Get record stored at given location.
   *
   * @param record Reference to the record to get.
   * @param location Constant reference to location of the stored record.
   * @param txn Reference to an active transaction.
   */
  void Get(RecordType &record, const RecordLocation &location,
           Transaction &txn) override {
    // Check if provided location is null
    if (location.IsNull()) {
      throw RecordNotFoundError("Invalid location provided.");
    }

    // Get the first page slot at the given location and create the record
    // by joining all related slots.

    // Byte buffer to read
    ByteBuffer read_buffer;
    // Start reading slots
    RecordPageSlot::Location read_location = location;
    while (!read_location.IsNull()) {
      // Get page
      auto page = page_manager.GetPage(read_location.page_id);
      // Get page slot
      const RecordPageSlot &slot =
          page->GetPageSlot(read_location.slot_id, txn);
      // Append data stored in slot to output buffer
      read_buffer.insert(read_buffer.end(), slot.data.begin(), slot.data.end());
      // Update read location to next block
      read_location = slot.GetNextLocation();
    }
    // Load record from byte buffer
    record.Load(read_buffer);
  }

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * inserted location of the record.
   *
   * @param record Reference to the record to insert.
   * @param txn Reference to an active transaction.
   * @returns Location of the inserted record.
   */
  RecordLocation Insert(RecordType &record, Transaction &txn) override {
    // Byte buffer to write
    ByteBuffer write_buffer;
    // Dump record to byte buffer
    record.Dump(write_buffer);
    // Insert byte buffer.
    return Insert(txn, write_buffer);
  }

  /**
   * @brief Update record stored at given location.
   *
   * @param record Reference to the updated record.
   * @param location Constant reference to location of the record.
   * @param txn Reference to an active transaction.
   */
  void Update(RecordType &record, const RecordLocation &location,
              Transaction &txn) override {}

  /**
   * @brief Delete record stored at given location.
   *
   * @param location Constant reference to location of the record to delete.
   * @param txn Reference to an active transaction.
   */
  void Delete(const RecordLocation &location, Transaction &txn) override {}
};

} // namespace persist

#endif /* PERSIST_LIST_RECORD_MANAGER_HPP */
