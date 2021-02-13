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

#ifndef LOG_PAGE_SLOT_HPP
#define LOG_PAGE_SLOT_HPP

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

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
     * @brief ID of page containing slot.
     */
    PageId pageId;
    /**
     * @brief Sequence number of the log record stored in the slot.
     */
    SeqNumber seqNumber;

    /**
     * Constructor
     */
    Location() : pageId(0), seqNumber(0) {}
    Location(PageId pageId, SeqNumber seqNumber)
        : pageId(pageId), seqNumber(seqNumber) {}

    /**
     * @brief Check if location is NULL. A NULL location will have both page ID
     * and sequence number set to `0`.
     */
    bool isNull() const { return pageId == 0 && seqNumber == 0; }

    /**
     * @brief Set the location to NULL.
     */
    void setNull() {
      pageId = 0;
      seqNumber = 0;
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Location &other) const {
      return pageId == other.pageId && seqNumber == other.seqNumber;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Location &other) const {
      return pageId != other.pageId || seqNumber != other.seqNumber;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write slot location to output stream
     */
    friend std::ostream &operator<<(std::ostream &os,
                                    const Location &location) {
      os << "[" << location.pageId << ", " << location.seqNumber << "]";
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
    SeqNumber seqNumber;

    /**
     * @brief Next page slot location
     */
    Location nextLocation;

    /**
     * @brief Checksum to detect slot corruption
     */
    Checksum checksum;

    /**
     * Constructors
     */
    Header() : seqNumber(0), checksum(0) {}
    Header(SeqNumber seqNumber) : seqNumber(seqNumber), checksum(0) {}
    Header(SeqNumber seqNumber, Location nextLocation)
        : seqNumber(seqNumber), nextLocation(nextLocation), checksum(0) {}

    /**
     * Load slot header from byte string.
     *
     * @param input input buffer span to load
     */
    void load(Span input) {
      if (input.size < sizeof(Header)) {
        throw PageSlotParseError();
      }

      // Load bytes
      std::memcpy((void *)this, (const void *)input.start, sizeof(Header));
    }

    /**
     * Dump slot header as byte string.
     *
     * @param output output buffer span to dump
     */
    void dump(Span output) {
      if (output.size < sizeof(Header)) {
        throw PageSlotParseError();
      }
      // Dump bytes
      std::memcpy((void *)output.start, (const void *)this, sizeof(Header));
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Header &other) const {
      return seqNumber == other.seqNumber && nextLocation == other.nextLocation;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Header &other) const {
      return seqNumber != other.seqNumber || nextLocation != other.nextLocation;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write slot header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "---- Header ----\n";
      os << "seqNumber: " << header.seqNumber << "\n";
      os << "next: " << header.nextLocation << "\n";
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

  /**
   * @brief Computes checksum for page slot.
   *
   * @param dataSize size of data in bytes
   */
  Checksum _checksum(size_t dataSize) {

    // Implemented hash function based on comment in
    // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

    Checksum seed = fixedSize;

    seed ^= std::hash<PageId>()(header.seqNumber) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    seed ^= std::hash<PageId>()(header.nextLocation.pageId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageSlotId>()(header.nextLocation.seqNumber) +
            0x9e3779b9 + (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageSlotId>()(dataSize) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);

    return seed;
  }

public:
  ByteBuffer data; //<- data contained in the record block

  /**
   * @brief Construct a new log page slot
   *
   */
  LogPageSlot() {}
  LogPageSlot(SeqNumber seqNumber) : header(seqNumber) {}
  LogPageSlot(SeqNumber seqNumber, Location nextLocation)
      : header(seqNumber, nextLocation) {}

  /**
   * @brief Size of fixed length members of slot
   *
   */
  static const uint64_t fixedSize = sizeof(Header) + sizeof(size_t);

  /**
   * Get storage size of page slot.
   */
  uint64_t size() { return fixedSize + sizeof(Byte) * data.size(); }

  /**
   * @brief Get the sequence number of the stored log record
   *
   * @return sequence number of the stored log record
   */
  const SeqNumber &getSeqNumber() const { return header.seqNumber; }

  /**
   * @brief Set the sequence number of the stored log record
   *
   * @param seqNumber sequence number of the stored log record
   */
  void setSeqNumber(SeqNumber &seqNumber) { header.seqNumber = seqNumber; }

  /**
   * @brief Get the next page slot location
   *
   * @return Location of the next linked page slot
   */
  const Location &getNextLocation() const { return header.nextLocation; }

  /**
   * @brief Set the next page slot location
   *
   * @param location Location of the next linked page slot
   */
  void setNextLocation(Location &location) { header.nextLocation = location; }

  /**
   * Load page slot object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    // Load header
    header.load(input);

    // Load bytes
    Byte *pos = input.start + sizeof(Header);
    // Load data size
    size_t dataSize;
    std::memcpy((void *)&dataSize, (const void *)pos, sizeof(size_t));
    pos += sizeof(size_t);
    // Check for corruption by matching checksum
    if (_checksum(dataSize) != header.checksum) {
      throw PageSlotCorruptError();
    }
    // Load data
    data.resize(dataSize);
    if (input.size < size()) {
      throw PageSlotParseError();
    }
    std::memcpy((void *)data.data(), (const void *)pos, dataSize);
  }

  /**
   * Dump page slot object as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) {
    if (output.size < size()) {
      throw PageSlotParseError();
    }

    // Get data size
    size_t dataSize = data.size();

    // Compute and set checksum
    header.checksum = _checksum(dataSize);

    // Dump header
    header.dump(output);

    // Dump bytes
    Byte *pos = output.start + sizeof(Header);
    // Dump data size
    std::memcpy((void *)pos, (const void *)&dataSize, sizeof(size_t));
    pos += sizeof(size_t);
    // Dump data
    std::memcpy((void *)pos, (const void *)data.data(), dataSize);
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

#endif /* LOG_PAGE_SLOT_HPP */
