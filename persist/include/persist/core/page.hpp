/**
 * data_block.hpp - Persist
 *
 * Copyright 2020 Ketan Goyal
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

#ifndef PAGE_HPP
#define PAGE_HPP

#include <cstdint>
#include <list>
#include <unordered_map>

#include <persist/core/common.hpp>
#include <persist/core/record_block.hpp>

#define MINIMUM_PAGE_SIZE 256
#define DEFAULT_PAGE_SIZE 1024

namespace persist {

/**
 * Page Class
 *
 * A Page is a logical chunk of space on a backend storage. Each page comprises
 * of a header, free space, and stored RecordBlocks. The page header contains
 * the page unique identifier along with the next and previous page identifiers
 * in case the page is linked. It also contains entries of offset values
 * indicating where each record-block in the page is located.
 */
class Page : public Serializable {
public:
  /**
   * Page Header Class
   *
   * The page header contains the page unique identifier along with the next
   * and previous page identifiers in case the page is linked. It also contains
   * entries of offset values indicating where each record-block in the page is
   * located.
   */
  class Header : public Serializable {
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
    typedef std::list<Slot> Slots;
    /**
     * @brief Page slots
     */
    Slots slots;

    /**
     * Constructors
     */
    Header();
    Header(PageId pageId);
    Header(PageId pageId, uint64_t pageSize);

    /**
     * Get storage size of header.
     */
    uint64_t size();

    /**
     * Ending offset of the free space in the block.
     *
     * @returns free space ending offset
     */
    uint64_t tail();

    /**
     * Use up chunk of space of given size from the available free space in the
     * Page. This operation allocates storage slot.
     *
     * @param size amount of space in bytes to occupy
     * @returns pointer to the new slot
     */
    Slot *createSlot(uint64_t size);

    /**
     * Free up used chunk of space occupied by given slot.
     *
     * @param slot poiter to slot to free
     */
    void freeSlot(Slot *slot);

    /**
     * Load object from byte string
     *
     * @param input input buffer to load
     */
    void load(ByteBuffer &input) override;

    /**
     * Dump object as byte string
     *
     * @returns reference to the buffer with results
     */
    ByteBuffer &dump() override;
  };

private:
  /**
   * @brief Page header
   */
  Header header;

  typedef std::unordered_map<PageSlotId, std::pair<RecordBlock, Header::Slot *>>
      RecordBlockCache;
  /**
   * @brief Cached collection of records stored in page slots
   */
  RecordBlockCache cache;

public:
  /**
   * Constructors
   */
  Page();
  Page(PageId pageId);
  Page(PageId pageId, uint64_t pageSize);

  /**
   * Get block ID.
   *
   * @returns block identifier
   */
  PageId &getId() { return header.pageId; }

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
  void setNextPageId(PageId pageId) { header.nextPageId = pageId; }

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
  void setPrevPageId(PageId pageId) { header.prevPageId = pageId; }

  /**
   * Get free space in bytes available in the block.
   *
   * @returns free space available in data block
   */
  uint64_t freeSpace();

  /**
   * Get RecordBlock object at a given slot.
   *
   * @param slotId slot identifier
   * @returns reference to RecordBlock object
   */
  RecordBlock &getRecordBlock(PageSlotId slotId);

  /**
   * Add RecordBlock object to the page.
   *
   * @param recordBlock recRecordBlock object to be added
   * @returns page slot ID where record block is stored
   */
  PageSlotId addRecordBlock(RecordBlock &recordBlock);

  /**
   * Remove RecordBlock object at given slot.
   *
   * @param slotId slot identifier
   */
  void removeRecordBlock(PageSlotId slotId);

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer to load
   */
  void load(ByteBuffer &input) override;

  /**
   * Dump Block object as byte string.
   *
   * @returns reference to the buffer with results
   */
  ByteBuffer &dump() override;
};

} // namespace persist

#endif /* PAGE_HPP */
