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

#include <persist/core/defs.hpp>
#include <persist/core/page.hpp>

namespace persist {

/**
 * Storage MetaData
 *
 * The class contains backend storage metadata information. This information
 * is utilized by the page table for efficient handling of page lifecycle.
 *
 */
class MetaData {
  PERSIST_PRIVATE
  /**
   * @brief Computes checksum for record block.
   */
  Checksum _checksum();

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
   * @brief Checksum to detect metadata corruption
   */
  Checksum checksum;

  /**
   * @brief Total size in bytes of fixed length data members of the metadata.
   * The value includes:
   * - sizeof(pageSize)
   * - sizeof(numPages)
   * - sizeof(freePages.size())
   * - sizeof(Checksome)
   */
  static const size_t fixedSize =
      2 * sizeof(uint64_t) + sizeof(size_t) + sizeof(Checksum);

  /**
   * Constructor
   */
  MetaData() : pageSize(DEFAULT_PAGE_SIZE), numPages(0) {}

  /**
   * Storage size of metadata. The size comprises of:
   * - sizeof(pageSize)
   * - sizeof(numPages)
   * - sizeof(freePages.size())
   * - freePages.size() * sizeof(PageId)
   * - sizeof(Checksome)
   */
  uint64_t size() { return fixedSize + sizeof(PageId) * freePages.size(); }

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
};

/**
 * MetaData Delta Class
 *
 * Contains changes to be applied on metadata object.
 */
class MetaDataDelta {
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
  void load(ByteBuffer &input);

  /**
   * Dump object as byte string
   *
   * @param output output buffer to dump
   */
  void dump(ByteBuffer &output);
};

} // namespace persist

#endif /* METADATA_HPP */
