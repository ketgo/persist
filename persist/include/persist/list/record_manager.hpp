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
  // Page manager defined in base class
  using RecordManager<RecordType, ReplacerType,
                      FreeSpaceManagerType>::page_manager;
  // Inherit constructor
  using RecordManager<RecordType, ReplacerType,
                      FreeSpaceManagerType>::RecordManager;

  PERSIST_PRIVATE
  /**
   * @brief Insert bytes as doubly linked page slots in storage. The method is
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
  RecordPageSlot::Location Insert(
      Transaction &txn, const Span &span,
      const RecordPageSlot::Location &location = RecordPageSlot::Location()) {
    // Null slot representing the previous from first slot
    RecordPageSlot null_slot;

    // Bookkeeping variables
    size_t to_write_size = span.size, written_size = 0;
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
      // data.
      size_t write_space = page->GetFreeSpaceSize(Operation::INSERT);
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

      std::cout << "Insert: " << next_location << "\n";

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
   * On-place updat5e bytes stored in doubly linked page slots in storage.
   *
   * @param txn Reference to transaction object.
   * @param span Constant reference to span of updated byte buffer.
   * @param location Constant reference to the location of the starting record
   * slot to update.
   */
  void Update(Transaction &txn, const Span &span,
              const RecordPageSlot::Location &location) {
    // Bookkeeping variables
    size_t to_write_size = span.size, written_size = 0;
    RecordPageSlot::Location update_location = location, updated_location;
    // Perform in-place update of existing linked slots
    while (to_write_size > 0 && !update_location.IsNull()) {
      std::cout << "Update: " << update_location << "\n";
      // Get handle to the record page
      auto page = page_manager.GetPage(update_location.page_id);
      // Get reference to the slot to update
      const RecordPageSlot &slot =
          page->GetPageSlot(update_location.slot_id, txn);

      // Compute availble space to write data in page. The available space is
      // the sum of the current size of the slot and the amount of free space in
      // the page. Here the greedy approach is utilized where all the available
      // free space can be used to store the data.
      size_t write_space =
          slot.data.size() + page->GetFreeSpaceSize(Operation::UPDATE);
      if (to_write_size < write_space) {
        write_space = to_write_size;
      }
      // Pointer to one past element already written
      Byte *pos = span.start + written_size;
      // Create updated slot
      RecordPageSlot updated_slot;
      updated_slot.data.resize(write_space);
      for (int i = 0; i < write_space; i++) {
        updated_slot.data[i] = *(pos + i);
      }

      // Set next update location
      updated_location = update_location;
      update_location = slot.GetNextLocation();

      // Update double linkage between slots
      updated_slot.SetNextLocation(update_location);
      updated_slot.SetPrevLocation(slot.GetPrevLocation());

      // Update page
      page->UpdatePageSlot(updated_location.slot_id, updated_slot, txn);

      // Update counters
      written_size += write_space;
      to_write_size -= write_space;
    }

    // Remove any remaining linked slots in case present.
    if (!update_location.IsNull()) {
      Remove(txn, update_location);
    }

    // Insert remaining byte buffer if present since no more slots exist to
    // update in-place.
    if (to_write_size > 0) {
      Span insert_span(span.start + written_size, to_write_size);
      update_location = Insert(txn, insert_span, updated_location);

      // Get handle to the record page
      auto page = page_manager.GetPage(updated_location.page_id);
      // Get reference to the slot to update
      const RecordPageSlot &slot =
          page->GetPageSlot(updated_location.slot_id, txn);

      // Create updated slot
      RecordPageSlot updated_slot;
      updated_slot.data = slot.data;
      updated_slot.SetNextLocation(update_location);
      updated_slot.SetPrevLocation(slot.GetPrevLocation());
      page->UpdatePageSlot(updated_location.slot_id, updated_slot, txn);
    }
  }

  /**
   * @brief Remove doubly linked slots from backend storage.
   *
   * @param txn Reference to transaction object.
   * @param location Constant reference to the location of the starting record
   * slot to remove.
   */
  void Remove(Transaction &txn, const RecordPageSlot::Location &location) {
    // Location of the slot to remove
    RecordPageSlot::Location remove_location = location;
    while (!remove_location.IsNull()) {
      std::cout << "Remove: " << remove_location << "\n";
      // Current slot ID
      PageSlotId slot_id = remove_location.slot_id;
      // Get handle to the record page
      auto page = page_manager.GetPage(remove_location.page_id);
      // Get reference to the slot to remove
      const RecordPageSlot &slot = page->GetPageSlot(slot_id, txn);
      // Set next remove location
      remove_location = slot.GetNextLocation();
      // Remove record block
      page->RemovePageSlot(slot_id, txn);
    }
  }

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
    try {
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
        read_buffer.insert(read_buffer.end(), slot.data.begin(),
                           slot.data.end());
        // Update read location to next block
        read_location = slot.GetNextLocation();
      }
      // Load record from byte buffer
      record.Load(read_buffer);
    } catch (NotFoundException &) {
      throw RecordNotFoundError(location.page_id, location.slot_id);
    }
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
    ByteBuffer write_buffer(record.GetStorageSize());
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
              Transaction &txn) override {
    // Check if provided location is null
    if (location.IsNull()) {
      throw RecordNotFoundError("Invalid location provided.");
    }

    // Byte buffer to write
    ByteBuffer write_buffer(record.GetStorageSize());
    // Dump record to byte buffer
    record.Dump(write_buffer);
    // Update persisted byte buffer in backend
    Update(txn, write_buffer, location);
  }

  /**
   * @brief Delete record stored at given location.
   *
   * @param location Constant reference to location of the record to delete.
   * @param txn Reference to an active transaction.
   */
  void Delete(const RecordLocation &location, Transaction &txn) override {
    // Check if provided location is null
    if (location.IsNull()) {
      throw RecordNotFoundError("Invalid location provided.");
    }
    // Rmove record slots
    Remove(txn, location);
  }
};

} // namespace persist

#endif /* PERSIST_LIST_RECORD_MANAGER_HPP */
