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

#ifndef PERSIST_CORE_PAGE_LOGPAGE_PAGE_HPP
#define PERSIST_CORE_PAGE_LOGPAGE_PAGE_HPP

#include <unordered_map>

#include <persist/core/exceptions/page.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/page/log_page/slot.hpp>

namespace persist {

// TODO:
// 1. The insert is not thread safe as it exposes a loophole for
// accessing protected slots by returning a pointer. We need a LogSlotHandle
// which locks the access to slots from concurrent read and writes.
// 2. Use move operations for writing page slots in page.

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
  class Header : public Storable {
  public:
    /**
     * @brief Page unique identifer
     */
    PageId page_id;

    /**
     * @brief Sequence number of the last log record in page.
     *
     * Note: A value of 0 indicates no complete or starting part of the log
     * record found in page.
     */
    SeqNumber last_seq_number;

    /**
     * @brief Number of slots in the page
     *
     */
    size_t slot_count;

    /**
     * @brief Storage size of the page.
     */
    size_t page_size;

    /**
     * @brief Construct a new Header object
     *
     */
    Header(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
        : page_id(page_id), page_size(page_size), last_seq_number(0),
          slot_count(0) {}

    /**
     * @brief Get storage size of header
     *
     */
    size_t GetStorageSize() const override {
      return sizeof(PageId) + sizeof(SeqNumber) + sizeof(uint64_t);
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
      // Load bytes
      persist::load(input, page_id, last_seq_number, slot_count);
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
      persist::dump(output, page_id, last_seq_number, slot_count);
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write page header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "------- Header -------\n";
      os << "id: " << header.page_id << "\n";
      os << "lastSeqNumber: " << header.last_seq_number << "\n";
      os << "slotCount: " << header.slot_count << "\n";
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
  size_t data_size;

public:
  /**
   * @brief Construct a new Log Page object
   */
  LogPage(PageId page_id = 0, size_t page_size = DEFAULT_LOG_PAGE_SIZE)
      : header(page_id, page_size), data_size(header.GetStorageSize()) {}

  /**
   * Get page identifier.
   *
   * @returns page identifier
   */
  const PageId &GetId() const override { return header.page_id; }

  /**
   * @brief Get the storage free space size in the page for specified operation.
   * The method takes the log page slot fixed size into account when calculating
   * free space.
   *
   * NOTE: Log pages only support INSERT operation
   *
   * @param operation Operaion to be performed
   * @returns Free space in bytes
   */
  size_t GetFreeSpaceSize(Operation operation) const override {
    // Amount of space occupied along with the fixed size of slot
    size_t occupied = data_size + LogPageSlot::GetFixedStorageSize();
    // If occupied data size is greater than equal to page size then return 0
    if (header.page_size <= occupied) {
      return 0;
    }
    return header.page_size - occupied;
  }

  /**
   * @brief Get the last sequence number in the page
   *
   * @returns Constant reference to last sequence number
   */
  const SeqNumber &GetLastSeqNumber() const { return header.last_seq_number; }

  /**
   * @brief Set the last sequence number in the page
   *
   * @param seq_number last sequence number in the page
   */
  void SetLastSeqNumber(SeqNumber seq_number) {
    header.last_seq_number = seq_number;
    // Notify observers of modification
    NotifyObservers();
  }

  /**
   * Get page slot of given identifier within the page.
   *
   * @param seq_number sequence number of the log record seeked
   * @returns Constant reference to the LogPageSlot object if found
   * @throws PageSlotNotFoundError
   */
  const LogPageSlot &GetPageSlot(SeqNumber seq_number) const {
    // Check if slot exists
    SlotMap::const_iterator it = slots.find(seq_number);
    if (it == slots.end()) {
      throw PageSlotNotFoundError(header.page_id, seq_number);
    }
    return it->second;
  }

  /**
   * @brief Insert log page slot to page.
   *
   * @param page_slot Reference to the LogPageSlot object to insert
   * @returns pointer to the inserted LogPageSlot
   */
  LogPageSlot *InsertPageSlot(LogPageSlot &page_slot) {
    // Update data size in page
    data_size += page_slot.GetStorageSize();
    // Insert record block at slot
    auto inserted = slots.emplace(page_slot.GetSeqNumber(), page_slot);

    // Notify observers of modification
    NotifyObservers();

    return &inserted.first->second;
  }

  /**
   * @brief Get the storage size of the page.
   *
   * @returns Storage size.
   */
  size_t GetStorageSize() const override { return header.page_size; }

  /**
   * Load LogPage object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < header.page_size) {
      throw PageParseError();
    }
    slots.clear(); //<- clears data in case it is loaded

    // Load Page header
    header.Load(input);
    data_size = header.GetStorageSize();
    input += data_size;
    // Load bytes
    for (size_t i = 0; i < header.slot_count; ++i) {
      // Load slot
      LogPageSlot slot;
      slot.Load(input);
      size_t slot_size = slot.GetStorageSize();
      data_size += slot_size;
      input += slot_size;
      slots.emplace(slot.GetSeqNumber(), slot);
    }
  }

  /**
   * Dump LogPage object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < header.page_size) {
      throw PageParseError();
    }
    // Dump header
    header.slot_count = slots.size();
    header.Dump(output);
    output += header.GetStorageSize();
    // Dump bytes
    for (auto &element : slots) {
      LogPageSlot &slot = element.second;
      slot.Dump(output);
      output += slot.GetStorageSize();
    }
    // Dump free space
    std::memset((void *)output.start, 0, output.size);
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write page to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const LogPage &page) {
    os << "--------- Page " << page.header.page_id << " ---------\n";
    os << page.header << "\n";
    for (auto element : page.slots) {
      os << ":--> [" << page.header.page_id << ", " << element.first << "]\n";
      os << element.second << "\n";
    }
    os << "-----------------------------";

    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_LOGPAGE_PAGE_HPP */
