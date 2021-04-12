/**
 * page.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_RECORDPAGE_PAGE_HPP
#define PERSIST_CORE_PAGE_RECORDPAGE_PAGE_HPP

#include <map>
#include <unordered_map>

#include <persist/core/exceptions/page.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/page/record_page/slot.hpp>
#include <persist/core/transaction/transaction.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {

// TODO :
// 1. Implement PageSlotHandle used for accessing slots in a thread safe
// way. The handle locks and unlocks read-write lock of the associated slot
// during construction and destruction of the handle. The insert interface
// is not thread safe as it exposes a loophole for accessing slots by a
// pointer. Refactor that to use PageSlotHandle.
// 2. Add a read-write mutex to the PageSlot class or create a wrapper
// class.
// 3. Use move operations for writing page slots in page.

/**
 * @brief Record Page
 *
 * Slotted page stores data records in slots of variable lengths where each
 * slot may contain a full or partial record. The page can be used in
 * collections which store variable length records. It comprises of a
 * header, free space, and stored slots. The page header comprises of the
 * page unique identifier, called PageId, along with the next and previous
 * page identifiers in case the page is linked. It also contains entries of
 * offset values indicating where each slot is located within the page.
 *
 * Every slot in a page has a unique identifier called SlotId. The SlotId is
 * only unique within the context of a page. For a globally unique
 * identifier the (PageId, SlotId) tuple is used. The tuple acts as an
 * abstract address of the slot in a backend storage.
 *
 * A data record spanning accross multiple slots is usually stored as a
 * doubly-linked list of page slots. Thus, a slot contains the location,
 * i.e. the (PageId, SlotId) tuple of the next and previous page slots to
 * which it is linked. This information along with its own SlotId is stored
 * in its header. The rest of the slot stores the record data.
 *
 */
class RecordPage : public Page {
public:
  /**
   * Page Header Class
   *
   * The page header contains the page unique identifier along with the next
   * and previous page identifiers in case the page is linked. It also contains
   * entries of offset values indicating where each record-block in the page is
   * located.
   */
  class Header : public Storable {
  public:
    /**
     * @brief Page unique identifer
     */
    PageId page_id;
    /**
     * @brief Linked next page unique identifer. This is set to 0 by default and
     * is only used for handling page overflow in Map collections.
     */
    PageId next_page_id;
    /**
     * @brief Linked previous page unique identifer. This is set to 0 by default
     * and is only used for handling page overflow in Map collections.
     */
    PageId prev_page_id;

    /**
     * @brief Storage size of the page.
     */
    size_t page_size;

    /**
     * SlotSpan Class
     *
     * Contains offset and size of stored slots in the page.
     */
    struct SlotSpan {
      size_t offset; //<- location offset from end of block
      size_t size;   //<- size of stored data
    };
    /**
     * @brief SlotSpanMap type to map slot ID to its span.
     *
     */
    typedef typename std::map<PageSlotId, SlotSpan> SlotSpanMap;
    SlotSpanMap slots;

    /**
     * Constructors
     */
    Header(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
        : page_id(page_id), next_page_id(0), prev_page_id(0),
          page_size(page_size) {}

    /**
     * Get storage size of header. The size comprises of:
     *
     * NOTE: Page size is not stored as its part of the metadata.
     */
    size_t GetStorageSize() const override {
      return 3 * sizeof(PageId) + sizeof(size_t) +
             (sizeof(SlotSpan) + sizeof(PageSlotId)) * slots.size();
    }

    /**
     * Ending offset of the free space in the page.
     *
     * @returns free space ending offset
     */
    size_t GetTail() const {
      if (slots.empty()) {
        return page_size;
      }
      return slots.rbegin()->second.offset;
    }

    /**
     * Use up chunk of space of given size from the available free space in the
     * page. This operation allocates storage slot.
     *
     * @param size amount of space in bytes to occupy
     * @returns ID of the new slot
     */
    PageSlotId CreateSlot(size_t size) {
      // Get ID of the last slot
      PageSlotId last_id, new_id;
      if (slots.empty()) {
        last_id = 0;
      } else {
        last_id = slots.rbegin()->first;
      }
      // Create a new slot by adding it to the list of slots
      new_id = last_id + 1;
      slots.emplace(new_id, SlotSpan{GetTail() - size, size});

      return new_id;
    }

    /**
     * Use up chunk of space of given size from the available free space in the
     * page. This operation allocates storage slot with given ID. In case a slot
     * with given ID already exists then no operation is performed.
     *
     * @param slot_id the slot ID to use when creating. By default set to 0 in
     * which case an ID is generated.
     * @param size amount of space in bytes to occupy
     */
    void CreateSlot(PageSlotId slot_id, size_t size) {
      size_t prev_offset;
      if (slot_id == 1) {
        prev_offset = page_size;
      } else {
        prev_offset = slots.at(slot_id - 1).offset;
      }
      slots.emplace(slot_id, SlotSpan{prev_offset - size, 0});
      UpdateSlot(slot_id, size);
    }

    /**
     * Update size of used chunk of space occupied by slot of given ID.
     *
     * @param slot_id identifier of slot to update
     * @param size new size of the slot
     */
    void UpdateSlot(PageSlotId slot_id, size_t size) {
      // Change in size
      size_t delta = slots.at(slot_id).size - size;
      // Update targeted slot size
      slots.at(slot_id).size = size;
      // Adjsut offsets of rest of the slots
      SlotSpanMap::iterator it = slots.find(slot_id);
      while (it != slots.end()) {
        it->second.offset += delta;
        ++it;
      }
    }

    /**
     * Free up used chunk of space occupied by slot of given ID.
     *
     * @param slot_id identifier of slot to free
     */
    void FreeSlot(PageSlotId slot_id) {
      // Adjust slot offsets
      size_t size = slots.at(slot_id).size;
      SlotSpanMap::iterator it = std::next(slots.find(slot_id));
      while (it != slots.end()) {
        it->second.offset += size;
        ++it;
      }
      // Remove slot from the list of slots
      slots.erase(slot_id);
    }

    /**
     * Load object from byte string
     *
     * @param input input buffer span to load
     */
    void Load(Span input) override {
      if (input.size < GetStorageSize()) {
        throw PageParseError();
      }
      slots.clear(); //<- clears slots in case they are loaded
      // Load bytes
      persist::load(input, page_id, next_page_id, prev_page_id, slots);
    }

    /**
     * Dump object as byte string
     *
     * @param output output buffer span to dump
     */
    void Dump(Span output) override {
      if (output.size < GetStorageSize()) {
        throw PageParseError();
      }
      // Dump bytes
      persist::dump(output, page_id, next_page_id, prev_page_id, slots);
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write page header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "------- Header -------\n";
      os << "id: " << header.page_id << "\n";
      os << "next: " << header.next_page_id << "\n";
      os << "prev: " << header.prev_page_id << "\n";
      os << "_size: " << header.page_size << "\n";
      os << "slots: \n";
      for (auto slot : header.slots) {
        os << "\tid: " << slot.first << ", offset: " << slot.second.offset
           << ", size: " << slot.second.size << "\n";
      }
      os << "----------------------";
      return os;
    }
#endif
  };

  PERSIST_PRIVATE
  /**
   * @brief Page header
   */
  Header header;

  /**
   * @brief Collection of page slots mapped to their slot_ids in page header
   */
  typedef typename std::unordered_map<PageSlotId, RecordPageSlot> PageSlotMap;
  PageSlotMap page_slots;

public:
  /**
   * @brief Construct a new RecordPage object
   *
   * @param page_id page identifer
   * @param page_size page storage size
   */
  RecordPage(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
      : header(page_id, page_size) {}

  /**
   * Get page ID.
   *
   * @returns block identifier
   */
  const PageId &GetId() const override { return header.page_id; }

  /**
   * Get free space in bytes available in the page. The method takes the log
   * page slot fixed size into account when calculating free space.
   *
   * @param operation The type of page operation for which free space is
   * requested.
   * @returns free space available in page
   */
  size_t GetFreeSpaceSize(Operation operation) const override {
    size_t size = header.GetTail() - header.GetStorageSize();
    // Compute size for INSERT operation
    if (operation == Operation::INSERT) {
      // Amount of space occupied along with correction due to header of
      // SlotSpan and page slot fixed size during insert operation.
      const size_t occupied = sizeof(Header::SlotSpan) + sizeof(PageSlotId) +
                              RecordPageSlot::GetFixedStorageSize();
      // Check if free space size is greater than header slot size
      if (size > occupied) {
        size -= occupied;
      } else {
        size = 0;
      }
    }
    // Returns free size for all types of operations
    return size;
  }

  /**
   * Get next page ID. This is the ID of the next linked page when there is data
   * overflow. A value of `0` means there is no next page.
   *
   * @returns next page identifier
   */
  const PageId &GetNextPageId() const { return header.next_page_id; }

  /**
   * Set next page ID. This is the ID for the next linked page when there is
   * data overflow. A value of `0` means there is no next page.
   *
   * @param page_id Constant reference to the next page ID value to set
   */
  void SetNextPageId(const PageId &page_id) {
    header.next_page_id = page_id;
    // Notify observers of modification
    NotifyObservers();
  }

  /**
   * Get previous page ID. This is the ID for the previous linked page when
   * there is data overflow. A value of 0 means there is no previous page.
   *
   * @returns previous page identifier
   */
  const PageId &GetPrevPageId() const { return header.prev_page_id; }

  /**
   * Set previous page ID. This is the ID for the previous linked page when
   * there is data overflow. A value of 0 means there is no previous page.
   *
   * @param page_id Constant reference to the previous page ID value to set
   */
  void SetPrevPageId(const PageId &page_id) {
    header.prev_page_id = page_id;
    // Notify observers of modification
    NotifyObservers();
  }

  /**
   * Get page slot of given identifier within the page.
   *
   * @param slot_id Slot identifier
   * @param txn Reference to active transaction
   * @returns Constant reference to the PageSlot object if found
   * @throws PageSlotNotFoundError
   */
  const RecordPageSlot &GetPageSlot(PageSlotId slot_id,
                                    Transaction &txn) const {
    // Check if slot exists
    PageSlotMap::const_iterator it = page_slots.find(slot_id);
    if (it == page_slots.end()) {
      throw PageSlotNotFoundError(header.page_id, slot_id);
    }
    return it->second;
  }

  /**
   * Insert page slot to the page.
   *
   * @param page_slot Reference to the PageSlot object to insert
   * @param txn Reference to active transaction
   * @returns SlotId and pointer to the inserted PageSlot
   */
  std::pair<PageSlotId, RecordPageSlot *>
  InsertPageSlot(RecordPageSlot &page_slot, Transaction &txn) {
    // Create slot for record block
    PageSlotId slot_id = header.CreateSlot(page_slot.GetStorageSize());

    // Log insert operation
    RecordPageSlot::Location location(header.page_id, slot_id);
    txn.LogInsertOp(location, page_slot);

    // Insert record block at slot
    auto inserted = page_slots.emplace(slot_id, page_slot);

    // Notify observers of modification
    NotifyObservers();

    return std::pair<PageSlotId, RecordPageSlot *>(slot_id,
                                                   &inserted.first->second);
  }

  /**
   * Update page slot in the page.
   *
   * @param slot_id Identifier of the slot to update
   * @param page_slot Reference to updated page slot
   * @param txn Reference to active transaction
   * @throws PageSlotNotFoundError
   */
  virtual void UpdatePageSlot(PageSlotId slot_id, RecordPageSlot &page_slot,
                              Transaction &txn) {
    // Check if slot exists
    PageSlotMap::iterator it = page_slots.find(slot_id);
    if (it == page_slots.end()) {
      throw PageSlotNotFoundError(header.page_id, slot_id);
    }

    // Log update operation
    RecordPageSlot::Location location(header.page_id, slot_id);
    txn.LogUpdateOp(location, it->second, page_slot);

    // Update slot for record block
    header.UpdateSlot(slot_id, page_slot.GetStorageSize());
    // Update record block at slot
    it->second = std::move(page_slot);

    // Notify observers of modification
    NotifyObservers();
  }

  /**
   * Remove page slot of given identifier within page.
   *
   * @param slot_id Identifier of the slot to remove
   * @param txn Reference to active transaction
   * @throws PageSlotNotFoundError
   */
  void RemovePageSlot(PageSlotId slot_id, Transaction &txn) {
    // Check if slot exists
    PageSlotMap::iterator it = page_slots.find(slot_id);
    if (it == page_slots.end()) {
      throw PageSlotNotFoundError(header.page_id, slot_id);
    }

    // Log delete operation
    RecordPageSlot::Location location(header.page_id, slot_id);
    txn.LogDeleteOp(location, page_slots.at(slot_id));

    // Adjusting header
    header.FreeSlot(slot_id);
    // Removing record block from cache
    page_slots.erase(it);

    // Notify observers of modification
    NotifyObservers();
  }

  /**
   * Undo remove page slot of given identifier within page.
   *
   * @param slot_id Identifier of the slot to insert back
   * @param page_slot Reference to the slot to insert back
   * @param txn Reference to active transaction
   */
  void UndoRemovePageSlot(PageSlotId slot_id, RecordPageSlot &page_slot,
                          Transaction &txn) {
    // Log insert operation
    RecordPageSlot::Location location(header.page_id, slot_id);
    txn.LogInsertOp(location, page_slot);

    // Update slot for record block
    header.CreateSlot(slot_id, page_slot.GetStorageSize());
    // Update record block at slot
    page_slots.emplace(slot_id, page_slot);

    // Notify observers of modification
    NotifyObservers();
  }

  /**
   * @brief Get the storage size of the page.
   *
   * @returns Storage size.
   */
  size_t GetStorageSize() const override { return header.page_size; }

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < header.page_size) {
      throw PageParseError();
    }
    page_slots.clear(); //<- clears record blocks in case they are loaded

    // Load Page header
    header.Load(input);
    // Load record blocks
    for (auto &element : header.slots) {
      Header::SlotSpan &slot = element.second;
      Span span(input.start + slot.offset, slot.size);
      RecordPageSlot page_slot;
      page_slot.Load(span);
      page_slots.emplace(element.first, page_slot);
    }
  }

  /**
   * Dump Block object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < header.page_size) {
      throw PageParseError();
    }
    // Dump header
    header.Dump(output);
    // Dump free space
    Span span(output.start, header.GetTail());
    span += header.GetStorageSize();
    std::memset((void *)span.start, 0, span.size);
    // Dump record blocks
    for (auto &element : header.slots) {
      Header::SlotSpan &slot = element.second;
      RecordPageSlot &page_slot = page_slots.at(element.first);
      Span span(output.start + slot.offset, slot.size);
      page_slot.Dump(span);
    }
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write page to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const RecordPage &page) {
    os << "--------- Page " << page.header.page_id << " ---------\n";
    os << page.header << "\n";
    for (auto element : page.page_slots) {
      os << ":--> [" << page.header.page_id << ", " << element.first << "]\n";
      os << element.second << "\n";
    }
    os << "-----------------------------";

    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_RECORDPAGE_PAGE_HPP */
