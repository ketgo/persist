/**
 * log_page.hpp - Persist
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

#ifndef LOG_PAGE_HPP
#define LOG_PAGE_HPP

#include <unordered_map>

#include <persist/core/exceptions.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/page/log_page/page_slot.hpp>

namespace persist {

/**
 * @brief LogPage Class
 *
 * The log page is a page to store log records. Log records are persisted to
 * backend storage by the log manager in pages for efficiency.
 */
class LogPage : public Page {
public:
  /**
   * @brief Log Page Header
   *
   * The header contains page ID information.
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

      seed ^=
          std::hash<PageId>()(pageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<SeqNumber>()(lastSeqNumber) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      seed ^= std::hash<uint64_t>()(slotCount) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);

      return seed;
    }

  public:
    /**
     * @brief Page unique identifer
     */
    PageId pageId;

    /**
     * @brief Sequence number of the last log record in page.
     *
     * Note: A value of 0 indicates no complete or starting part of the log
     * record found in page.
     */
    SeqNumber lastSeqNumber;

    /**
     * @brief Number of slots in the page
     *
     */
    uint64_t slotCount;

    /**
     * @brief Storage size of the page.
     */
    uint64_t pageSize;

    /**
     * @brief Checksum to detect page corruption
     */
    Checksum checksum;

    /**
     * @brief Construct a new Header object
     *
     */
    Header(PageId pageId = 0, uint64_t pageSize = DEFAULT_PAGE_SIZE)
        : pageId(pageId), pageSize(pageSize), lastSeqNumber(0), slotCount(0) {}

    /**
     * @brief Get storage size of header
     *
     */
    uint64_t size() {
      return sizeof(PageId) + sizeof(SeqNumber) + sizeof(uint64_t) +
             sizeof(Checksum);
    }

    /**
     * Load object from byte string
     *
     * @param input input buffer span to load
     */
    void load(Span input) {
      if (input.size < size()) {
        throw PageParseError();
      }

      // Load bytes
      Byte *pos = input.start;
      std::memcpy((void *)&pageId, (const void *)pos, sizeof(PageId));
      pos += sizeof(PageId);
      std::memcpy((void *)&lastSeqNumber, (const void *)pos, sizeof(SeqNumber));
      pos += sizeof(SeqNumber);
      std::memcpy((void *)&slotCount, (const void *)pos, sizeof(uint64_t));
      pos += sizeof(uint64_t);
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
      std::memcpy((void *)pos, (const void *)&lastSeqNumber, sizeof(SeqNumber));
      pos += sizeof(SeqNumber);
      std::memcpy((void *)pos, (const void *)&slotCount, sizeof(uint64_t));
      pos += sizeof(uint64_t);
      std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write page header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "------- Header -------\n";
      os << "id: " << header.pageId << "\n";
      os << "lastSeqNumber: " << header.lastSeqNumber << "\n";
      os << "slotCount: " << header.slotCount << "\n";
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
   * @brief Mapping between sequence number and corresponding log record stored
   * as bytes in log page slots.
   */
  typedef typename std::unordered_map<PageSlotId, LogPageSlot> SlotMap;
  SlotMap slots;

  /**
   * @brief Size of free space available on the page.
   */
  uint64_t dataSize;

public:
  /**
   * @brief Construct a new Log Page object
   */
  LogPage(PageId pageId = 0, uint64_t pageSize = DEFAULT_LOG_PAGE_SIZE)
      : header(pageId, pageSize), dataSize(header.size()) {
    // Check page size greater than minimum size
    if (pageSize < MINIMUM_PAGE_SIZE) {
      throw PageSizeError(pageSize);
    }
  }

  /**
   * Get page ID.
   *
   * @returns page identifier
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
    // If stored data size greater than page size then return 0
    if (header.pageSize <= dataSize) {
      return 0;
    }

    return header.pageSize - dataSize;
  }

  /**
   * @brief Get the last sequence number in the page
   *
   * @returns Constant reference to last sequence number
   */
  const SeqNumber &getLastSeqNumber() const { return header.lastSeqNumber; }

  /**
   * @brief Set the last sequence number in the page
   *
   * @param seqNumber last sequence number in the page
   */
  void setLastSeqNumber(SeqNumber seqNumber) {
    header.lastSeqNumber = seqNumber;
    // Notify observers of modification
    notifyObservers();
  }

  /**
   * Get page slot of given identifier within the page.
   *
   * @param seqNumber sequence number of the log record seeked
   * @returns Constant reference to the LogPageSlot object if found
   * @throws PageSlotNotFoundError
   */
  const LogPageSlot &getPageSlot(SeqNumber seqNumber) const {
    // Check if slot exists
    SlotMap::const_iterator it = slots.find(seqNumber);
    if (it == slots.end()) {
      throw PageSlotNotFoundError(header.pageId, seqNumber);
    }
    return it->second;
  }

  // TODO: The insert is not thread safe as it exposes a loophole for
  // accessing protected slots by returning a pointer. We need a SlotHandle
  // which locks the access to slots from concurrent read and writes.
  /**
   * @brief Insert log page slot to page.
   *
   * @param pageSlot Reference to the LogPageSlot object to insert
   * @returns pointer to the inserted LogPageSlot
   */
  LogPageSlot *insertPageSlot(LogPageSlot &pageSlot) {
    // Update data size in page
    dataSize += pageSlot.size();
    // Insert record block at slot
    auto inserted = slots.emplace(pageSlot.getSeqNumber(), pageSlot);

    // Notify observers of modification
    notifyObservers();

    return &inserted.first->second;
  }

  /**
   * Load LogPage object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) override {
    if (input.size < header.pageSize) {
      throw PageParseError();
    }
    slots.clear(); //<- clears data in case it is loaded

    // Load Page header
    header.load(input);

    // Load bytes
    dataSize = header.size();
    Byte *pos = input.start + dataSize;
    // Load Slots
    for (int i = 0; i < header.slotCount; ++i) {
      // Load slot
      LogPageSlot slot;
      slot.load(Span(pos, input.size - dataSize));
      uint64_t slotSize = slot.size();
      dataSize += slotSize;
      slots.emplace(slot.getSeqNumber(), slot);
      pos += slotSize;
    }
  }

  /**
   * Dump LogPage object as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) override {
    if (output.size < header.pageSize) {
      throw PageParseError();
    }

    // Set slot count in header
    header.slotCount = slots.size();
    // Dump header
    header.dump(output);

    // Dump bytes
    Byte *pos = output.start + header.size();
    // Dump slots
    for (auto &element : slots) {
      LogPageSlot &slot = element.second;
      uint64_t slotSize = slot.size();
      slot.dump(Span(pos, slotSize));
      pos += slotSize;
    }
    // Dump free space
    std::memset((void *)pos, 0, freeSpace(Operation::INSERT));
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write page to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const LogPage &page) {
    os << "--------- Page " << page.header.pageId << " ---------\n";
    os << page.header << "\n";
    for (auto element : page.slots) {
      os << ":--> [" << page.header.pageId << ", " << element.first << "]\n";
      os << element.second << "\n";
    }
    os << "-----------------------------";

    return os;
  }
#endif
};

} // namespace persist

#endif /* LOG_PAGE_HPP */
