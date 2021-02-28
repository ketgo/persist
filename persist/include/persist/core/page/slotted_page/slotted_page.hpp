/**
 * vls_slotted_page.hpp - Persist
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

#ifndef SLOTTED_PAGE_HPP
#define SLOTTED_PAGE_HPP

#include <map>
#include <unordered_map>

#include <persist/core/exceptions.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/page/slotted_page/page_slot.hpp>
#include <persist/core/transaction/transaction.hpp>

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
 * @brief Slotted Page
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
class SlottedPage : public Page {
public:
  /**
   * Page Header Class
   *
   * The page header contains the page unique identifier along with the next
   * and previous page identifiers in case the page is linked. It also contains
   * entries of offset values indicating where each record-block in the page is
   * located.
   */
  class Header {
    PERSIST_PRIVATE
    /**
     * @brief Computes checksum for record block.
     */
    Checksum _checksum() {

      // Implemented hash function based on comment in
      // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

      Checksum seed = size();

      seed =
          std::hash<PageId>()(pageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<PageId>()(nextPageId) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      seed ^= std::hash<PageId>()(prevPageId) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      seed ^= std::hash<size_t>()(slots.size()) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      for (auto element : slots) {
        HeaderSlot &slot = element.second;
        seed ^= std::hash<PageSlotId>()(slot.id) + 0x9e3779b9 + (seed << 6) +
                (seed >> 2);
        seed ^= std::hash<uint64_t>()(slot.offset) + 0x9e3779b9 + (seed << 6) +
                (seed >> 2);
        seed ^= std::hash<uint64_t>()(slot.size) + 0x9e3779b9 + (seed << 6) +
                (seed >> 2);
      }

      return seed;
    }

  public:
    /**
     * @brief Page unique identifer
     */
    PageId pageId;
    /**
     * @brief Linked next page unique identifer. This is set to 0 by default and
     * is only used for handling page overflow in Map collections.
     */
    PageId nextPageId;
    /**
     * @brief Linked previous page unique identifer. This is set to 0 by default
     * and is only used for handling page overflow in Map collections.
     */
    PageId prevPageId;

    /**
     * @brief Storage size of the page.
     */
    uint64_t pageSize;

    /**
     * Page Header Slot Class
     *
     * Contains offset, size and ID of storage slots in the page.
     */
    struct HeaderSlot {
      PageSlotId id;   //<- slot identifier
      uint64_t offset; //<- location offset from end of block
      uint64_t size;   //<- size of stored data
    };
    typedef typename std::map<PageSlotId, HeaderSlot> HeaderSlotMap;
    /**
     * @brief Page slots
     */
    HeaderSlotMap slots;

    /**
     * @brief Checksum to detect page corruption
     */
    Checksum checksum;

    /**
     * @brief Total size in bytes of fixed length data members of the header.
     * This variable is used for size calculation. The value includes:
     * - sizeof(pageId)
     * - sizeof(nextPageId)
     * - sizeof(prevPageId)
     * - sizeof(slots.size())
     * - sizeof(Checksome)
     */
    static const size_t fixedSize =
        3 * sizeof(PageId) + sizeof(size_t) + sizeof(Checksum);

    /**
     * Constructors
     */
    Header(PageId pageId = 0, uint64_t pageSize = DEFAULT_PAGE_SIZE)
        : pageId(pageId), nextPageId(0), prevPageId(0), pageSize(pageSize),
          checksum(0) {}

    /**
     * Get storage size of header. The size comprises of:
     * - sizeof(pageId)
     * - sizeof(nextPageId)
     * - sizeof(prevPageId)
     * - sizeof(slots.size())
     * - slots.size() * sizeof(Slot)
     * - sizeof(Checksome)
     *
     * NOTE: Page size is not stored as its part of the metadata.
     */
    uint64_t size() { return fixedSize + sizeof(HeaderSlot) * slots.size(); }

    /**
     * Ending offset of the free space in the page.
     *
     * @returns free space ending offset
     */
    uint64_t tail() {
      if (slots.empty()) {
        return pageSize;
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
    PageSlotId createSlot(uint64_t size) {
      // Get ID of the last slot
      PageSlotId lastId, newId;
      if (slots.empty()) {
        lastId = 0;
      } else {
        lastId = slots.rbegin()->second.id;
      }
      // Create slot and add it to the list of slots
      newId = lastId + 1;

      slots.insert(std::pair<PageSlotId, HeaderSlot>(
          newId, {newId, tail() - size, size}));

      return newId;
    }

    /**
     * Use up chunk of space of given size from the available free space in the
     * page. This operation allocates storage slot with given ID. In case a slot
     * with given ID already exists then no operation is performed.
     *
     * @param slotId the slot ID to use when creating. By default set to 0 in
     * which case an ID is generated.
     * @param size amount of space in bytes to occupy
     */
    void createSlot(PageSlotId slotId, uint64_t size) {
      uint64_t prevOffset;
      if (slotId == 1) {
        prevOffset = pageSize;
      } else {
        prevOffset = slots.at(slotId - 1).offset;
      }
      auto emplaced =
          slots.emplace(slotId, HeaderSlot{slotId, prevOffset - size, 0});
      updateSlot(slotId, size);
    }

    /**
     * Update size of used chunk of space occupied by slot of given ID.
     *
     * @param slotId identifier of slot to update
     * @param size new size of the slot
     */
    void updateSlot(PageSlotId slotId, uint64_t size) {
      // Change in size
      int64_t delta = slots.at(slotId).size - size;
      // Update targeted slot size
      slots.at(slotId).size = size;
      // Adjsut offsets of rest of the slots
      HeaderSlotMap::iterator it = slots.find(slotId);
      while (it != slots.end()) {
        it->second.offset += delta;
        ++it;
      }
    }

    /**
     * Free up used chunk of space occupied by slot of given ID.
     *
     * @param slotId identifier of slot to free
     */
    void freeSlot(PageSlotId slotId) {
      // Adjust slot offsets
      uint64_t size = slots.at(slotId).size;
      HeaderSlotMap::iterator it = std::next(slots.find(slotId));
      while (it != slots.end()) {
        it->second.offset += size;
        ++it;
      }
      // Remove slot from the list of slots
      slots.erase(slotId);
    }

    /**
     * Load object from byte string
     *
     * @param input input buffer span to load
     */
    void load(Span input) {
      if (input.size < fixedSize) {
        throw PageParseError();
      }
      slots.clear(); //<- clears slots in case they are loaded

      // Load bytes
      Byte *pos = input.start;
      std::memcpy((void *)&pageId, (const void *)pos, sizeof(PageId));
      pos += sizeof(PageId);
      std::memcpy((void *)&nextPageId, (const void *)pos, sizeof(PageId));
      pos += sizeof(PageId);
      std::memcpy((void *)&prevPageId, (const void *)pos, sizeof(PageId));
      pos += sizeof(PageId);
      size_t slotsCount;
      std::memcpy((void *)&slotsCount, (const void *)pos, sizeof(size_t));
      // Check if slots count value is valid
      size_t maxSlotsCount = (input.size - fixedSize) / sizeof(HeaderSlot);
      if (slotsCount > maxSlotsCount) {
        throw PageCorruptError();
      }
      pos += sizeof(size_t);
      while (slotsCount > 0) {
        HeaderSlot slot;
        std::memcpy((void *)&slot, (const void *)pos, sizeof(HeaderSlot));
        pos += sizeof(HeaderSlot);
        slots[slot.id] = slot;
        --slotsCount;
      }
      std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));

      // Check for corruption by matching checksum
      if (_checksum() != checksum) {
        throw PageCorruptError();
      }
    }

    /**
     * Dump object as byte string
     *
     * @param output output buffer span to dump
     */
    void dump(Span output) {
      if (output.size < size()) {
        throw PageParseError();
      }

      // Compute and set checksum
      checksum = _checksum();

      // Dump bytes
      Byte *pos = output.start;
      std::memcpy((void *)pos, (const void *)&pageId, sizeof(PageId));
      pos += sizeof(PageId);
      std::memcpy((void *)pos, (const void *)&nextPageId, sizeof(PageId));
      pos += sizeof(PageId);
      std::memcpy((void *)pos, (const void *)&prevPageId, sizeof(PageId));
      pos += sizeof(PageId);
      size_t slotsCount = slots.size();
      std::memcpy((void *)pos, (const void *)&slotsCount, sizeof(size_t));
      pos += sizeof(size_t);
      for (auto &element : slots) {
        HeaderSlot &slot = element.second;
        std::memcpy((void *)pos, (const void *)&slot, sizeof(HeaderSlot));
        pos += sizeof(HeaderSlot);
      }
      std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write page header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "------- Header -------\n";
      os << "id: " << header.pageId << "\n";
      os << "next: " << header.nextPageId << "\n";
      os << "prev: " << header.prevPageId << "\n";
      os << "_size: " << header.pageSize << "\n";
      os << "slots: \n";
      for (auto slot : header.slots) {
        os << "\tid: " << slot.second.id << ", offset: " << slot.second.offset
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
   * @brief Collection of page slots mapped to their slotIds in page header
   */
  typedef typename std::unordered_map<PageSlotId, PageSlot> PageSlotMap;
  PageSlotMap pageSlots;

public:
  /**
   * @brief Construct a new SlottedPage object
   *
   * @param pageId page identifer
   * @param pageSize page storage size
   */
  SlottedPage(PageId pageId = 0, uint64_t pageSize = DEFAULT_PAGE_SIZE)
      : header(pageId, pageSize) {
    // Check page size greater than minimum size
    if (pageSize < MINIMUM_PAGE_SIZE) {
      throw PageSizeError(pageSize);
    }
  }

  /**
   * @brief Get the page type identifer.
   *
   * @returns The page type identifier
   */
  PageTypeId getTypeId() const override { return 2; }

  /**
   * Get page ID.
   *
   * @returns block identifier
   */
  const PageId &getId() const override { return header.pageId; }

  /**
   * Get free space in bytes available in the page.
   *
   * @param operation The type of page operation for which free space is
   * requested.
   * @returns free space available in page
   */
  uint64_t freeSpace(Operation operation) override {
    uint64_t size = header.tail() - header.size();
    // Compute size for INSERT operation
    if (operation == Operation::INSERT) {
      // Check if free space size is greater than header slot size
      if (size > sizeof(Header::HeaderSlot)) {
        size -= sizeof(Header::HeaderSlot);
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
  const PageId &getNextPageId() const { return header.nextPageId; }

  /**
   * Set next page ID. This is the ID for the next linked page when there is
   * data overflow. A value of `0` means there is no next page.
   *
   * @param pageId next page ID value to set
   */
  void setNextPageId(PageId pageId) {
    header.nextPageId = pageId;
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Get previous page ID. This is the ID for the previous linked page when
   * there is data overflow. A value of 0 means there is no previous page.
   *
   * @returns previous page identifier
   */
  const PageId &getPrevPageId() const { return header.prevPageId; }

  /**
   * Set previous page ID. This is the ID for the previous linked page when
   * there is data overflow. A value of 0 means there is no previous page.
   *
   * @param pageId previous page ID value to set
   */
  void setPrevPageId(PageId pageId) {
    header.prevPageId = pageId;
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Get page slot of given identifier within the page.
   *
   * @param slotId Slot identifier
   * @param txn Reference to active transaction
   * @returns Constant reference to the PageSlot object if found
   * @throws PageSlotNotFoundError
   */
  const PageSlot &getPageSlot(PageSlotId slotId, Transaction &txn) const {
    // Check if slot exists
    PageSlotMap::const_iterator it = pageSlots.find(slotId);
    if (it == pageSlots.end()) {
      throw PageSlotNotFoundError(header.pageId, slotId);
    }
    return it->second;
  }

  /**
   * Insert page slot to the page.
   *
   * @param pageSlot Reference to the PageSlot object to insert
   * @param txn Reference to active transaction
   * @returns SlotId and pointer to the inserted PageSlot
   */
  std::pair<PageSlotId, PageSlot *> insertPageSlot(PageSlot &pageSlot,
                                                   Transaction &txn) {
    // Create slot for record block
    PageSlotId slotId = header.createSlot(pageSlot.size());

    // Log insert operation
    PageSlot::Location location(header.pageId, slotId);
    txn.logInsertOp(location, pageSlot);

    // Insert record block at slot
    auto inserted = pageSlots.emplace(slotId, pageSlot);

    // Notify observers of modification
    notifyObservers();

    return std::pair<PageSlotId, PageSlot *>(slotId, &inserted.first->second);
  }

  /**
   * Update page slot in the page.
   *
   * @param slotId Identifier of the slot to update
   * @param pageSlot Reference to updated page slot
   * @param txn Reference to active transaction
   * @throws PageSlotNotFoundError
   */
  virtual void updatePageSlot(PageSlotId slotId, PageSlot &pageSlot,
                              Transaction &txn) {
    // Check if slot exists
    PageSlotMap::iterator it = pageSlots.find(slotId);
    if (it == pageSlots.end()) {
      throw PageSlotNotFoundError(header.pageId, slotId);
    }

    // Log update operation
    PageSlot::Location location(header.pageId, slotId);
    txn.logUpdateOp(location, it->second, pageSlot);

    // Update slot for record block
    header.updateSlot(slotId, pageSlot.size());
    // Update record block at slot
    it->second = std::move(pageSlot);

    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Remove page slot of given identifier within page.
   *
   * @param slotId Identifier of the slot to remove
   * @param txn Reference to active transaction
   * @throws PageSlotNotFoundError
   */
  void removePageSlot(PageSlotId slotId, Transaction &txn) {
    // Check if slot exists
    PageSlotMap::iterator it = pageSlots.find(slotId);
    if (it == pageSlots.end()) {
      throw PageSlotNotFoundError(header.pageId, slotId);
    }

    // Log delete operation
    PageSlot::Location location(header.pageId, slotId);
    txn.logDeleteOp(location, pageSlots.at(slotId));

    // Adjusting header
    header.freeSlot(slotId);
    // Removing record block from cache
    pageSlots.erase(it);

    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Undo remove page slot of given identifier within page.
   *
   * @param slotId Identifier of the slot to insert back
   * @param pageSlot Reference to the slot to insert back
   * @param txn Reference to active transaction
   */
  void undoRemovePageSlot(PageSlotId slotId, PageSlot &pageSlot,
                          Transaction &txn) {
    // Log insert operation
    PageSlot::Location location(header.pageId, slotId);
    txn.logInsertOp(location, pageSlot);

    // Update slot for record block
    header.createSlot(slotId, pageSlot.size());
    // Update record block at slot
    pageSlots.emplace(slotId, pageSlot);

    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) override {
    if (input.size < header.pageSize) {
      throw PageParseError();
    }
    pageSlots.clear(); //<- clears record blocks in case they are loaded

    // Load Page header
    header.load(input);
    // Load record blocks
    for (auto &element : header.slots) {
      Header::HeaderSlot &slot = element.second;
      Span span(input.start + slot.offset, slot.size);
      PageSlot pageSlot;
      pageSlot.load(span);
      pageSlots.insert(std::pair<PageSlotId, PageSlot>(slot.id, pageSlot));
    }
  }

  /**
   * Dump Block object as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) override {
    if (output.size < header.pageSize) {
      throw PageParseError();
    }

    Span span(output.start, header.size());
    // Dump header
    header.dump(span);
    // Dump free space
    span.start += span.size;
    span.size = header.tail() - header.size();
    std::memset((void *)span.start, 0, span.size);
    // Dump record blocks
    for (auto &element : header.slots) {
      Header::HeaderSlot &slot = element.second;
      PageSlot &pageSlot = pageSlots.at(slot.id);
      span.start = output.start + slot.offset;
      span.size = slot.size;
      pageSlot.dump(span);
    }
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write page to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const VLSSlottedPage &page) {
    os << "--------- Page " << page.header.pageId << " ---------\n";
    os << page.header << "\n";
    for (auto element : page.pageSlots) {
      os << ":--> [" << page.header.pageId << ", " << element.first << "]\n";
      os << element.second << "\n";
    }
    os << "-----------------------------";

    return os;
  }
#endif
};

} // namespace persist

#endif /* SLOTTED_PAGE_HPP */
