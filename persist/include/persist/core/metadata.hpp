/**
 * metadata.hpp - Persist
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

/**
 * MetaData Headers
 *
 * The file contains definitions for backend storage metadata. This data is used
 * for managing Pages in backend storage.
 */

#ifndef METADATA_HPP
#define METADATA_HPP

#include <set>
#include <unordered_map>

#include <persist/core/common.hpp>
#include <persist/core/page.hpp>

namespace persist {

/**
 * Storage MetaData
 *
 * The class contains backend storage metadata information. This information
 * is utilized by the page table for efficient handling of page lifecycle.
 *
 */
class MetaData : public Serializable {
public:
  /**
   * @brief Size of each page
   */
  uint64_t pageSize;
  /**
   * @brief Number of pages in the storage.
   *
   */
  uint64_t numPages;
  /**
   * @brief List of free pages in the storage. These are pages containing free
   * space.
   *
   */
  std::set<PageId> freePages;

  /**
   * Constructor
   */
  MetaData() : pageSize(DEFAULT_PAGE_SIZE), numPages(0) {}

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

/**
 * MetaData Delta Class
 *
 * Contains changes to be applied on metadata object.
 */
class MetaDataDelta : public Serializable {
private:
  /**
   * Variable to indicate the number of pages increased or decreased. If the
   * value is set to `-1` the page count is reduced by one, if set to '1' then
   * its increased by once else if '0' then no change is made.
   */
  int8_t numPages;
  /**
   * Variable stored the changes in the list of free space pages. If a page ID
   * is mapped to '-1' then that ID is removed from the list of free space pages
   * in metadata. In case the page is not present then no operation is
   * performed. Similarly, if the ID is mapped to '1' then it is added to the
   * free space list in metadata. If value is '0' then no operation is
   * performed.
   *
   */
  std::unordered_map<PageId, int8_t> freePages;

public:
  /**
   * Construct
   */
  MetaDataDelta() : numPages(0) {}

  /**
   * Increase number of pages.
   */
  void numPagesUp();

  /**
   * Increase number of pages.
   */
  void numPagesDown();

  /**
   * Add page ID to free space list.
   *
   * @param pageId page ID to add
   */
  void addFreePage(PageId pageId);

  /**
   * Remove page ID from free space list.
   *
   * @param pageId page ID to remove
   */
  void removeFreePage(PageId pageId);

  /**
   * Apply change on metadata.
   *
   * @param metadata metadata object on which to apply changes
   */
  void apply(MetaData &metadata);

  /**
   * Undo changed on metadata.
   *
   * @param metadata metadata object on which to undo changes
   */
  void undo(MetaData &metadata);

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

} // namespace persist

#endif /* METADATA_HPP */
