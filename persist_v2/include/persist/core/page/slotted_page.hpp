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

#include <cstdint>
#include <map>
#include <unordered_map>

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
    Checksum _checksum();

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
    PageSlotId createSlot(uint64_t size);

    /**
     * Use up chunk of space of given size from the available free space in the
     * Page. This operation allocates storage slot with given ID. In case a slot
     * with given ID already exists then no operation is performed.
     *
     * @param slotId the slot ID to use when creating. By default set to 0 in
     * which case an ID is generated.
     * @param size amount of space in bytes to occupy
     */
    void createSlot(PageSlotId slotId, uint64_t size);

    /**
     * Update size of used chunk of space occupied by slot of given ID.
     *
     * @param slotId identifier of slot to update
     * @param size new size of the slot
     */
    void updateSlot(PageSlotId slotId, uint64_t size);

    /**
     * Free up used chunk of space occupied by slot of given ID.
     *
     * @param slotId identifier of slot to free
     */
    void freeSlot(PageSlotId slotId);

    /**
     * Load object from byte string
     *
     * @param input input buffer span to load
     */
    void load(Span input);

    /**
     * Dump object as byte string
     *
     * @param output output buffer span to dump
     */
    void dump(Span output);

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
  SlottedPage() {}
  SlottedPage(PageId pageId, uint64_t pageSize = DEFAULT_PAGE_SIZE);

  /**
   * Get block ID.
   *
   * @returns block identifier
   */
  PageId &getId() override { return header.pageId; }

  /**
   * Get free space in bytes available in the block.
   *
   * @param operation The type of page operation for which free space is
   * requested.
   * @returns free space available in page
   */
  uint64_t freeSpace(Operation operation) override;

  /**
   * Get next page ID. This is the ID for the page block when there is data
   * overflow. A value of `0` means there is no next page.
   *
   * @returns next page identifier
   */
  PageId &getNextPageId() { return header.nextPageId; }

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
  PageId &getPrevPageId() { return header.prevPageId; }

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
  RecordBlock &getRecordBlock(Transaction &txn, PageSlotId slotId);

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
  std::pair<PageSlotId, RecordBlock *> addRecordBlock(Transaction &txn,
                                                      RecordBlock &recordBlock);

  /**
   * Update record block in the page.
   *
   * @param txn reference to active transaction
   * @param slotId slot ID of the record being updated
   * @param recordBlock updated record block
   */
  void updateRecordBlock(Transaction &txn, PageSlotId slotId,
                         RecordBlock &recordBlock);

  /**
   * Remove RecordBlock object at given slot.
   *
   * @param txn reference to active transaction
   * @param slotId slot identifier
   */
  void removeRecordBlock(Transaction &txn, PageSlotId slotId);

  /**
   * Undo remove RecordBlock object at given slot.
   *
   * @param txn reference to active transaction
   * @param slotId slot identifier
   * @param recordBlock removed record block to add back to page
   */
  void undoRemoveRecordBlock(Transaction &txn, PageSlotId slotId,
                             RecordBlock &recordBlock);

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) override;

  /**
   * Dump Block object as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) override;

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