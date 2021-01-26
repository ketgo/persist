/**
 * slotted_page.hpp - Persist
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
#include <persist/core/record_block.hpp>
#include <persist/core/transaction.hpp>

namespace persist {

/**
 * @brief SlottedPage Class
 *
 * Each slotted page comprises of a header, free space, and stored RecordBlocks.
 * The page header contains the page unique identifier along with the next and
 * previous page identifiers in case the page is linked. It also contains
 * entries of offset values indicating where each record-block in the page is
 * located.
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
        Slot &slot = element.second;
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
     * Page Slots
     *
     * Contains offset, size and ID of storage slots in the page.
     */
    struct Slot {
      PageSlotId id;   //<- slot identifier
      uint64_t offset; //<- location offset from end of block
      uint64_t size;   //<- size of stored data
    };
    typedef std::map<PageSlotId, Slot> Slots;
    /**
     * @brief Page slots
     */
    Slots slots;

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
    uint64_t size() { return fixedSize + sizeof(Slot) * slots.size(); }

    /**
     * Ending offset of the free space in the block.
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
     * Page. This operation allocates storage slot.
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

      slots.insert(
          std::pair<PageSlotId, Slot>(newId, {newId, tail() - size, size}));

      return newId;
    }

    /**
     * Use up chunk of space of given size from the available free space in the
     * Page. This operation allocates storage slot with given ID. In case a slot
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
      auto emplaced = slots.emplace(slotId, Slot{slotId, prevOffset - size, 0});
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
      Slots::iterator it = slots.find(slotId);
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
      Slots::iterator it = std::next(slots.find(slotId));
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
      size_t maxSlotsCount = (input.size - fixedSize) / sizeof(Slot);
      if (slotsCount > maxSlotsCount) {
        throw PageCorruptError();
      }
      pos += sizeof(size_t);
      while (slotsCount > 0) {
        Slot slot;
        std::memcpy((void *)&slot, (const void *)pos, sizeof(Slot));
        pos += sizeof(Slot);
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
      for (auto element : slots) {
        Slot &slot = element.second;
        std::memcpy((void *)pos, (const void *)&slot, sizeof(Slot));
        pos += sizeof(Slot);
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
   * @brief Collection of record blocks mapped to their slots in page header
   */
  typedef std::unordered_map<PageSlotId, RecordBlock> RecordBlockMap;
  RecordBlockMap recordBlocks;

public:
  /**
   * Constructors
   */
  SlottedPage(PageId pageId = 0, uint64_t pageSize = DEFAULT_PAGE_SIZE)
      : header(pageId, pageSize) {
    // Check page size greater than minimum size
    if (pageSize < MINIMUM_PAGE_SIZE) {
      throw PageSizeError(pageSize);
    }
  }

  /**
   * Get block ID.
   *
   * @returns block identifier
   */
  const PageId &getId() const override { return header.pageId; }

  /**
   * Get free space in bytes available in the block.
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
      if (size > sizeof(Header::Slot)) {
        size -= sizeof(Header::Slot);
      } else {
        size = 0;
      }
    }
    // Returns free size for all types of operations
    return size;
  }

  /**
   * Get next page ID. This is the ID for the page block when there is data
   * overflow. A value of `0` means there is no next page.
   *
   * @returns next page identifier
   */
  const PageId &getNextPageId() const { return header.nextPageId; }

  /**
   * Set next page ID. This is the ID for the next page when there is data
   * overflow. A value of `0` means there is no next page.
   *
   * @param pageId next page ID value to set
   */
  void setNextPageId(PageId pageId) {
    header.nextPageId = pageId;
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Get previous page ID. This is the ID for the previous page when there is
   * data overflow. A value of 0 means there is no previous page.
   *
   * @returns previous page identifier
   */
  const PageId &getPrevPageId() const { return header.prevPageId; }

  /**
   * Set previous page ID. This is the ID for the previous page when there is
   * data overflow. A value of 0 means there is no previous page.
   *
   * @param pageId previous page ID value to set
   */
  void setPrevPageId(PageId pageId) {
    header.prevPageId = pageId;
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Get RecordBlock object at a given slot.
   *
   * @param txn reference to active transaction
   * @param slotId slot identifier
   * @returns reference to RecordBlock object
   */
  const RecordBlock &getRecordBlock(Transaction &txn, PageSlotId slotId) const {
    // Check if slot exists
    RecordBlockMap::const_iterator it = recordBlocks.find(slotId);
    if (it == recordBlocks.end()) {
      throw RecordBlockNotFoundError(header.pageId, slotId);
    }
    return it->second;
  }

  /**
   * Add RecordBlock object to the page.
   *
   * @param txn reference to active transaction
   * @param recordBlock RecordBlock object to be added
   * @param pageSlotId the slot ID where to insert the record block. If set to 0
   * then the record block is inserted at the back of all the slots.
   * @returns page slot ID where record block is stored and pointer to stored
   * record block
   */
  std::pair<PageSlotId, RecordBlock *>
  addRecordBlock(Transaction &txn, RecordBlock &recordBlock) {
    // Create slot for record block
    PageSlotId slotId = header.createSlot(recordBlock.size());

    // Log insert operation
    RecordBlock::Location location(header.pageId, slotId);
    txn.logInsertOp(location, recordBlock);

    // Insert record block at slot
    auto inserted = recordBlocks.emplace(slotId, recordBlock);
    // Stage current page
    txn.stage(header.pageId);
    // Notify observers of modification
    notifyObservers();

    return std::pair<PageSlotId, RecordBlock *>(slotId,
                                                &inserted.first->second);
  }

  /**
   * Update record block in the page.
   *
   * @param txn reference to active transaction
   * @param slotId slot ID of the record being updated
   * @param recordBlock updated record block
   */
  void updateRecordBlock(Transaction &txn, PageSlotId slotId,
                         RecordBlock &recordBlock) {
    // Log update operation
    RecordBlock::Location location(header.pageId, slotId);
    txn.logUpdateOp(location, recordBlocks.at(slotId), recordBlock);

    // Update slot for record block
    header.updateSlot(slotId, recordBlock.size());
    // Update record block at slot
    recordBlocks.at(slotId) = std::move(recordBlock);
    // Stage current page
    txn.stage(header.pageId);
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Remove RecordBlock object at given slot.
   *
   * @param txn reference to active transaction
   * @param slotId slot identifier
   */
  void removeRecordBlock(Transaction &txn, PageSlotId slotId) {
    // Check if slot exists in the Page
    RecordBlockMap::iterator it = recordBlocks.find(slotId);
    if (it == recordBlocks.end()) {
      throw RecordBlockNotFoundError(header.pageId, slotId);
    }
    // Log delete operation
    RecordBlock::Location location(header.pageId, slotId);
    txn.logDeleteOp(location, recordBlocks.at(slotId));

    // Adjusting header
    header.freeSlot(slotId);
    // Removing record block from cache
    recordBlocks.erase(it);
    // Stage current page
    txn.stage(header.pageId);
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Undo remove RecordBlock object at given slot.
   *
   * @param txn reference to active transaction
   * @param slotId slot identifier
   * @param recordBlock removed record block to add back to page
   */
  void undoRemoveRecordBlock(Transaction &txn, PageSlotId slotId,
                             RecordBlock &recordBlock) {
    // Log insert operation
    RecordBlock::Location location(header.pageId, slotId);
    txn.logInsertOp(location, recordBlock);

    // Update slot for record block
    header.createSlot(slotId, recordBlock.size());
    // Update record block at slot
    recordBlocks.emplace(slotId, recordBlock);
    // Stage current page
    txn.stage(header.pageId);
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
    recordBlocks.clear(); //<- clears record blocks in case they are loaded

    // Load Page header
    header.load(input);
    // Load record blocks
    for (auto element : header.slots) {
      Header::Slot &slot = element.second;
      Span span(input.start + slot.offset, slot.size);
      RecordBlock recordBlock;
      recordBlock.load(span);
      recordBlocks.insert(
          std::pair<PageSlotId, RecordBlock>(slot.id, recordBlock));
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
    for (auto element : header.slots) {
      Header::Slot &slot = element.second;
      RecordBlock &recordBlock = recordBlocks.at(slot.id);
      span.start = output.start + slot.offset;
      span.size = slot.size;
      recordBlock.dump(span);
    }
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write page to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const SlottedPage &page) {
    os << "--------- Page " << page.header.pageId << " ---------\n";
    os << page.header << "\n";
    for (auto element : page.recordBlocks) {
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
