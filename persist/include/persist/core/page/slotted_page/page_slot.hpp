/**
 * slotted_page/page_slot.hpp - Persist
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

#ifndef SLOTTED_PAGE_SLOT_HPP
#define SLOTTED_PAGE_SLOT_HPP

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

namespace persist {

/**
 * @brief PageSlot Class
 *
 * The class implements a page slot used by slotted pages. It stores a full or
 * partial data record which is to be stored in a slotted page. Each page slot
 * belonging to a page has a unique identifier called SlotId. The SlotId is only
 * unique within the context of a page. For a globally unique identifier the
 * (PageId, SlotId) tuple is used. This tuple acts as an abstract address of the
 * slot within a backend storage.
 *
 * A data record spanning accross multiple slots is stored as a doubly-linked
 * list of page slots. Thus, a slot contains the location, i.e. the (PageId,
 * SlotId) tuple, of the next and previous page slots to which it is linked.
 * This information along with its own SlotId is stored in its header. The rest
 * of the slot stores the data record.
 *
 */
class PageSlot {
public:
  /**
   * PageSlot Location Class
   *
   * The class object represents the address location of a page slot. This is
   * simply the global unique identifier of the slot given by the (PageId,
   * SlotId) tuple.
   */
  struct Location {
    /**
     * @brief ID of page containing slot.
     */
    PageId pageId;
    /**
     * @brief ID of the slot inside the above page.
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
     * @brief Write slot location to output stream
     */
    friend std::ostream &operator<<(std::ostream &os,
                                    const Location &location) {
      os << "[" << location.pageId << ", " << location.slotId << "]";
      return os;
    }
#endif
  };

  /**
   * PageSlot Header Class
   *
   * The class represents header of a page slot. It contains the metadata
   * information required for facilitating read write operations of records.
   */
  class Header {
  public:
    /**
     * @brief Next page slot location
     */
    Location nextLocation;
    /**
     * @brief Previous page slot location
     */
    Location prevLocation;

    /**
     * @brief Checksum to detect slot corruption
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
     * Load slot header from byte string.
     *
     * @param input input buffer span to load
     */
    void load(Span input) {
      if (input.size < size()) {
        throw PageSlotParseError();
      }

      // Load bytes
      std::memcpy((void *)this, (const void *)input.start, size());
    }

    /**
     * Dump slot header as byte string.
     *
     * @param output output buffer span to dump
     */
    void dump(Span output) {
      if (output.size < size()) {
        throw PageSlotParseError();
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
     * @brief Write slot header to output stream
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
   * @brief Page slot header
   *
   */
  Header header;

  /**
   * @brief Computes checksum for page slot.
   */
  Checksum _checksum() {

    // Implemented hash function based on comment in
    // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

    Checksum seed = size();

    seed = std::hash<PageId>()(header.nextLocation.pageId) + 0x9e3779b9 +
           (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageSlotId>()(header.nextLocation.slotId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageId>()(header.prevLocation.pageId) + 0x9e3779b9 +
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
   * @brief Construct a new Page Slot object
   *
   */
  PageSlot() {}
  PageSlot(PageSlot::Header header) : header(header) {}
  PageSlot(ByteBuffer data) : data(data) {}
  PageSlot(ByteBuffer data, PageSlot::Header header)
      : data(data), header(header) {}

  /**
   * Get storage size of page slot.
   */
  uint64_t size() { return header.size() + sizeof(Byte) * data.size(); }

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
   * @brief Get the previous page slot location
   *
   * @return Location of the previous linked page slot
   */
  const Location &getPrevLocation() const { return header.prevLocation; }

  /**
   * @brief Set the previous page slot location
   *
   * @param location Location of the previous linked page slot
   */
  void setPrevLocation(Location &location) { header.prevLocation = location; }

  /**
   * Load page slot object from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    if (input.size < size()) {
      throw PageSlotParseError();
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
      throw PageSlotCorruptError();
    }
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
  bool operator==(const PageSlot &other) const {
    return header == other.header && data == other.data;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const PageSlot &other) const {
    return header != other.header || data != other.data;
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write record block to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const PageSlot &slot) {
    os << "------ PageSlot ------\n";
    os << slot.header << "\n";
    os << "data: " << slot.data << "\n";
    os << "--------------------------";
    return os;
  }
#endif
};

} // namespace persist

#endif /* SLOTTED_PAGE_SLOT_HPP */
