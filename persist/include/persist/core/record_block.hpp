/**
 * record.hpp - Persist
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

#ifndef RECORD_HPP
#define RECORD_HPP

#include <cstdint>
#include <string>

#include <persist/core/defs.hpp>

namespace persist {

/**
 * Record Block Class
 *
 * The class represents a single chunk of a data record stored in backend
 * storage. A data record can consist of one or more RecordBlock objects
 * with same identifier.
 */
class RecordBlock {
public:
  /**
   * Record Block Location Class
   *
   * The class contains location information of a record block.
   */
  struct Location {
    /**
     * @brief ID of page containing record block.
     */
    PageId pageId;
    /**
     * @brief ID of slot inside the above page containing record block.
     */
    PageSlotId slotId;

    /**
     * Constructor
     */
    Location() : pageId(0), slotId(0) {}
    Location(PageId pageId, PageSlotId slotId)
        : pageId(pageId), slotId(slotId) {}

    /**
     * @brief Check if location is NULL
     */
    bool isNull() { return pageId == 0; }

    /**
     * @brief Set the location to NULL
     */
    void setNull() {
      pageId = 0;
      slotId = 0;
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Location &other) const {
      return pageId == other.pageId && slotId == other.slotId;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Location &other) const {
      return pageId != other.pageId || slotId != other.slotId;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write record block location to output stream
     */
    friend std::ostream &operator<<(std::ostream &os,
                                    const Location &location) {
      os << "[" << location.pageId << ", " << location.slotId << "]";
      return os;
    }
#endif
  };

  /**
   * Record Block Header Class
   *
   * Header data type for Record Block. It contains the metadata information for
   * facilitating read write operations of records. Since a record is stored as
   * linked list of record blocks, the header contains this linking information.
   */
  class Header {
  public:
    /**
     * @brief Next RecordBlock location
     */
    Location nextLocation;
    /**
     * @brief Previous RecordBlock location
     */
    Location prevLocation;

    /**
     * @brief Checksum to detect record block corruption
     */
    Checksum checksum;

    /**
     * Constructors
     */
    Header() : checksum(0) {}

    /**
     * Get storage size of header.
     */
    uint64_t size() { return sizeof(Header); }

    /**
     * Load record block header from byte string.
     *
     * @param input input buffer span to load
     */
    void load(Span input);

    /**
     * Dump record block header as byte string.
     *
     * @param output output buffer span to dump
     */
    void dump(Span output);

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write record block header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "---- Header ----\n";
      os << "next: " << header.nextLocation << "\n";
      os << "prev: " << header.prevLocation << "\n";
      os << "-----------------";
      return os;
    }
#endif
  };

  PERSIST_PRIVATE
  /**
   * @brief Record block header
   *
   */
  Header header;

  /**
   * @brief Computes checksum for record block.
   */
  Checksum _checksum();

public:
  ByteBuffer data; //<- data contained in the record block

  /**
   * Constructors
   */
  RecordBlock() {}
  RecordBlock(RecordBlock::Header &header) : header(header) {}

  /**
   * Get storage size of record block.
   */
  uint64_t size() { return header.size() + sizeof(Byte) * data.size(); }

  /**
   * @brief Get the next RecordBlock location object
   *
   * @return Location reference to the next record block
   */
  Location &nextLocation();

  /**
   * @brief Get the previous RecordBlock location object
   *
   * @return Location reference to the previous record block
   */
  Location &prevLocation();

  /**
   * Load RecordBlock object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input);

  /**
   * Dump RecordBlock object as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output);

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write record block to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const RecordBlock &recordBlock) {
    os << "------ Record Block ------\n";
    os << recordBlock.header << "\n";
    os << "data: " << recordBlock.data << "\n";
    os << "--------------------------";
    return os;
  }
#endif
};

} // namespace persist

#endif /* RECORD_HPP */
