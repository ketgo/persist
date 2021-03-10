/**
 * log_page/page_slot.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_LOG_PAGE_SLOT_HPP
#define PERSIST_CORE_PAGE_LOG_PAGE_SLOT_HPP

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {
/**
 * @brief LogPageSlot Class
 *
 * The class implements a page slot used by log pages. In case a log record
 * does not fit in a pge, it is split accross multiple slots.
 */
class LogPageSlot {
public:
  /**
   * LogPageSlot Location Class
   *
   * The class object represents the address location of a slot in storage. This
   * is simply the global unique identifier of the slot given by the tuple
   * (PageId, SeqNumber).
   */
  struct Location {
    /**
     * @brief Identifier of the page containing slot.
     */
    PageId page_id;
    /**
     * @brief Sequence number of the log record stored in the slot.
     */
    SeqNumber seq_number;

    /**
     * Constructor
     */
    Location() : page_id(0), seq_number(0) {}
    Location(PageId page_id, SeqNumber seq_number)
        : page_id(page_id), seq_number(seq_number) {}

    /**
     * @brief Check if location is NULL. A NULL location will have both page ID
     * and sequence number set to `0`.
     */
    bool IsNull() const { return page_id == 0 && seq_number == 0; }

    /**
     * @brief Set the location to NULL.
     */
    void SetNull() {
      page_id = 0;
      seq_number = 0;
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Location &other) const {
      return page_id == other.page_id && seq_number == other.seq_number;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Location &other) const {
      return page_id != other.page_id || seq_number != other.seq_number;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write slot location to output stream
     */
    friend std::ostream &operator<<(std::ostream &os,
                                    const Location &location) {
      os << "[" << location.page_id << ", " << location.seq_number << "]";
      return os;
    }
#endif
  };

  /**
   * LogPageSlot Header Class
   *
   * The class represents header of a plog age slot. It contains the metadata
   * information required for facilitating read write operations of log records.
   */
  class Header {
  public:
    /**
     * @brief Log record sequence number
     */
    SeqNumber seq_number;

    /**
     * @brief Next page slot location
     */
    Location next_location;

    /**
     * Constructors
     */
    Header() : seq_number(0) {}
    Header(SeqNumber seq_number) : seq_number(seq_number) {}
    Header(SeqNumber seq_number, Location next_location)
        : seq_number(seq_number), next_location(next_location) {}

    /**
     * @brief Get the storage size of header
     *
     */
    size_t GetSize() const { return sizeof(Header); }

    /**
     * Load slot header from byte string.
     *
     * @param input input buffer span to load
     */
    void Load(Span input) {
      if (input.size < GetSize()) {
        throw PageSlotParseError();
      }
      persist::load(input, seq_number, next_location);
    }

    /**
     * Dump slot header as byte string.
     *
     * @param output output buffer span to dump
     */
    void Dump(Span output) {
      if (output.size < GetSize()) {
        throw PageSlotParseError();
      }
      persist::dump(output, seq_number, next_location);
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Header &other) const {
      return seq_number == other.seq_number &&
             next_location == other.next_location;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Header &other) const {
      return seq_number != other.seq_number ||
             next_location != other.next_location;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write slot header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "---- Header ----\n";
      os << "seqNumber: " << header.seq_number << "\n";
      os << "next: " << header.next_location << "\n";
      os << "-----------------";
      return os;
    }
#endif
  };

  PERSIST_PRIVATE
  /**
   * @brief LogPage slot header
   *
   */
  Header header;

public:
  ByteBuffer data; //<- data contained in the record block

  /**
   * @brief Construct a new log page slot
   *
   */
  LogPageSlot() {}
  LogPageSlot(SeqNumber seq_number) : header(seq_number) {}
  LogPageSlot(SeqNumber seq_number, Location next_location)
      : header(seq_number, next_location) {}

  /**
   * Get storage size of page slot.
   * 
   */
  size_t GetSize() const {
    return header.GetSize() + sizeof(size_t) + sizeof(Byte) * data.size();
  }

  /**
   * @brief Get the sequence number of the stored log record
   *
   * @return sequence number of the stored log record
   */
  const SeqNumber &GetSeqNumber() const { return header.seq_number; }

  /**
   * @brief Set the sequence number of the stored log record
   *
   * @param seq_number sequence number of the stored log record
   */
  void SetSeqNumber(SeqNumber &seq_number) { header.seq_number = seq_number; }

  /**
   * @brief Get the next page slot location
   *
   * @return Location of the next linked page slot
   */
  const Location &GetNextLocation() const { return header.next_location; }

  /**
   * @brief Set the next page slot location
   *
   * @param location Location of the next linked page slot
   */
  void SetNextLocation(Location &location) { header.next_location = location; }

  /**
   * Load page slot object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) {
    if (input.size < GetSize()) {
      throw PageSlotParseError();
    }
    // Load header
    header.Load(input);
    input += header.GetSize();
    // Load bytes
    persist::load(input, data);
  }

  /**
   * Dump page slot object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) {
    if (output.size < GetSize()) {
      throw PageSlotParseError();
    }
    // Dump header
    header.Dump(output);
    output += header.GetSize();
    // Dump bytes
    persist::dump(output, data);
  }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const LogPageSlot &other) const {
    return header == other.header && data == other.data;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const LogPageSlot &other) const {
    return header != other.header || data != other.data;
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write record block to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const LogPageSlot &slot) {
    os << "------ LogPageSlot ------\n";
    os << slot.header << "\n";
    os << "data: " << slot.data << "\n";
    os << "--------------------------";
    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_LOG_PAGE_SLOT_HPP */
