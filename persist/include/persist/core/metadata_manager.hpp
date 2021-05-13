/**
 * metadata_manager.hpp - Persist
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

#ifndef PERSIST__CORE__METADATA_MANAGER_HPP
#define PERSIST__CORE__METADATA_MANAGER_HPP

#include <persist/core/metadata.hpp>
#include <persist/core/page/record_page/page.hpp>
#include <persist/core/page_manager.hpp>
#include <persist/core/transaction/transaction.hpp>

namespace persist {

/**
 * @brief Collection metadata manager. It is responsible for storing collection
 * metadata, consisting of size, location of first element, and location of last
 * element.
 *
 * TODO: Use direrectly the page manger as template parameter insted of replacer
 * and fsm.
 *
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 */
template <class ReplacerType, class FreeSpaceManagerType>
class MetadataManager {
  PERSIST_PRIVATE
  /**
   * @brief Metadata storage location. It is always stored as the first
   * record in the collection.
   *
   */
  const MetadataLocation location = {1, 1};

  /**
   * @brief Reference to the collections page manager.
   *
   */
  PageManager<RecordPage, ReplacerType, FreeSpaceManagerType> &page_manager;

  /**
   * @brief Flag indicating manager started.
   *
   */
  bool started;

public:
  /**
   * @brief Construct a new Metadata Manager object.
   *
   * @param page_manager Reference to page manager
   *
   */
  MetadataManager(
      PageManager<RecordPage, ReplacerType, FreeSpaceManagerType> &page_manager)
      : page_manager(page_manager), started(false) {}

  /**
   * @brief Start metadata manager.
   *
   * TODO: Take transaction manager an input argument and insert an empty
   * metadata object.
   */
  void Start() {
    if (!started) {
      // Start page manager
      page_manager.Start();
      started = true;
    }
  }

  /**
   * @brief Stop metadata manager.
   *
   */
  void Stop() {
    if (started) {
      // Start page manager
      page_manager.Stop();
      started = false;
    }
  }

  /**
   * @brief Read metadata from backend storage.
   *
   * @param metadata Reference to the metadata object to read
   * @param txn Reference to an active transaction
   */
  void Read(Metadata &metadata, Transaction &txn) {
    try {
      // Get page handle
      auto page = page_manager.GetPage(location.page_id);
      // Get page slot
      const RecordPageSlot &slot = page->GetPageSlot(location.slot_id, txn);
      // Byte buffer to read
      ByteBuffer read_buffer(slot.data);
      // Load metadata
      metadata.Load(read_buffer);
    } catch (NotFoundException &err) {
      throw MetadataNotFoundError();
    }
  }

  /**
   * @brief Insert metadata to backend storage.
   *
   * @param metadata Reference to the metadata object to insert
   * @param txn Reference to an active transaction
   */
  MetadataLocation Insert(Metadata &metadata, Transaction &txn) {
    // Get new page handle
    auto page = page_manager.GetNewPage();
    // Create page slot
    RecordPageSlot slot;
    // Dump metadata to slot
    slot.data.resize(metadata.GetStorageSize());
    metadata.Dump(slot.data);
    // Insert page slot
    auto inserted = page->InsertPageSlot(slot, txn);

    // NOTE: Since the setup happens during collection startup, it is assumed
    // that the above operation results in the metadata to be stored at
    // location [1, 1].
    // TODO: Store metadata location as part of storage header in case the
    // storage location in not [1, 1].

    // Throw setup exception if metadata inserted at an invalid location.
    MetadataLocation inserted_location(page->GetId(), inserted.first);
    if (inserted_location != location) {
      throw MetadataSetupError();
    }

    return inserted_location;
  }

  /**
   * @brief Update metadata in backend storage.
   *
   * @param metadata Reference to the metadata object to update
   * @param txn Reference to an active transaction
   */
  void Update(Metadata &metadata, Transaction &txn) {
    // Create updated slot
    RecordPageSlot slot;
    // Dump metadata to slot
    slot.data.resize(metadata.GetStorageSize());
    metadata.Dump(slot.data);
    // Get page handle
    auto page = page_manager.GetPage(location.page_id);
    // Update page
    page->UpdatePageSlot(location.slot_id, slot, txn);
  }
};

} // namespace persist

#endif /* PERSIST__CORE__METADATA_MANAGER_HPP */
