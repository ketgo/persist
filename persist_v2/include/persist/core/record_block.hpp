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

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

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
    bool isNull() const { return pageId == 0; }

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
    void load(Span input) {
      if (input.size < size()) {
        throw RecordBlockParseError();
      }

      // Load bytes
      std::memcpy((void *)this, (const void *)input.start, size());
    }

    /**
     * Dump record block header as byte string.
     *
     * @param output output buffer span to dump
     */
    void dump(Span output) {
      if (output.size < size()) {
        throw RecordBlockParseError();
      }
      // Dump bytes
      std::memcpy((void *)output.start, (const void *)this, size());
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Header &other) const {
      return nextLocation == other.nextLocation &&
             prevLocation == other.prevLocation;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Header &other) const {
      return nextLocation != other.nextLocation ||
             prevLocation != other.prevLocation;
    }

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
  Checksum _checksum() {

    // Implemented hash function based on comment in
    // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

    Checksum seed = size();

    seed = std::hash<PageId>()(header.nextLocation.pageId) + 0x9e3779b9 +
           (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageSlotId>()(header.nextLocation.slotId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageId>()(header.prevLocation.slotId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageSlotId>()(header.prevLocation.slotId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    for (auto &i : data) {
      seed ^= i + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }

    return seed;
  }

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
  const Location &getNextLocation() const { return header.nextLocation; }

  /**
   * @brief Set the next RecordBlock location object
   *
   * @param location reference to the location of the next record block
   */
  void setNextLocation(Location &location) { header.nextLocation = location; }

  /**
   * @brief Get the previous RecordBlock location object
   *
   * @return Location reference to the previous record block
   */
  const Location &getPrevLocation() const { return header.prevLocation; }

  /**
   * @brief Set the previous RecordBlock location object
   *
   * @param location reference to the location of the previous record block
   */
  void setPrevLocation(Location &location) { header.prevLocation = location; }

  /**
   * Load RecordBlock object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    if (input.size < size()) {
      throw RecordBlockParseError();
    }
    // Load header
    header.load(input);
    // Load data
    size_t dataSize = input.size - header.size();
    data.resize(dataSize);
    std::memcpy((void *)data.data(),
                (const void *)(input.start + header.size()), dataSize);

    // Check for corruption by matching checksum
    if (_checksum() != header.checksum) {
      throw RecordBlockCorruptError();
    }
  }

  /**
   * Dump RecordBlock object as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) {
    if (output.size < size()) {
      throw RecordBlockParseError();
    }

    // Compute and set checksum
    header.checksum = _checksum();

    // Dump header
    header.dump(output);
    // Dump data
    std::memcpy((void *)(output.start + header.size()),
                (const void *)data.data(), data.size());
  }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const RecordBlock &other) const {
    return header == other.header && data == other.data;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const RecordBlock &other) const {
    return header != other.header || data != other.data;
  }

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
