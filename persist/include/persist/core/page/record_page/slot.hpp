/**
 * slot.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_RECORDPAGE_SLOT_HPP
#define PERSIST_CORE_PAGE_RECORDPAGE_SLOT_HPP

#include <persist/core/common.hpp>
#include <persist/core/exceptions/page.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {

/**
 * @brief The class implements a page slot used by slotted pages. It stores a
 * full or partial data record which is to be stored in a slotted page. Each
 * page slot belonging to a page has a unique identifier called SlotId. The
 * SlotId is only unique within the context of a page. For a globally unique
 * identifier the (PageId, SlotId) tuple is used. This tuple acts as an abstract
 * address of the slot within a backend storage.
 *
 * A data record spanning accross multiple slots is stored as a doubly-linked
 * list of page slots. Thus, a slot contains the location, i.e. the (PageId,
 * SlotId) tuple, of the next and previous page slots to which it is linked.
 * This information along with its own SlotId is stored in its header. The rest
 * of the slot stores the data record.
 *
 */
class RecordPageSlot : public Storable {
public:
  /**
   * RecordPageSlot Location Class
   *
   * The class object represents the address location of a page slot. This is
   * simply the global unique identifier of the slot given by the (PageId,
   * SlotId) tuple.
   */
  struct Location {
    /**
     * @brief ID of page containing slot.
     */
    PageId page_id;
    /**
     * @brief ID of the slot inside the above page.
     */
    PageSlotId slot_id;

    /**
     * Constructor
     */
    Location() : page_id(0), slot_id(0) {}
    Location(PageId page_id, PageSlotId slot_id)
        : page_id(page_id), slot_id(slot_id) {}

    /**
     * @brief Check if location is NULL
     */
    bool IsNull() const { return page_id == 0; }

    /**
     * @brief Set the location to NULL
     */
    void SetNull() {
      page_id = 0;
      slot_id = 0;
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Location &other) const {
      return page_id == other.page_id && slot_id == other.slot_id;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Location &other) const {
      return page_id != other.page_id || slot_id != other.slot_id;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write slot location to output stream
     */
    friend std::ostream &operator<<(std::ostream &os,
                                    const Location &location) {
      os << "[" << location.page_id << ", " << location.slot_id << "]";
      return os;
    }
#endif
  };

  /**
   * RecordPageSlot Header Class
   *
   * The class represents header of a page slot. It contains the metadata
   * information required for facilitating read write operations of records.
   */
  class Header : public Storable {
  public:
    /**
     * @brief Next page slot location
     */
    Location next_location;
    /**
     * @brief Previous page slot location
     */
    Location prev_location;

    /**
     * Constructors
     */
    Header() = default;

    /**
     * Get storage size of header.
     */
    size_t GetStorageSize() const override { return sizeof(Header); }

    /**
     * Load slot header from byte string.
     *
     * @param input input buffer span to load
     */
    void Load(Span input) override {
      if (input.size < GetStorageSize()) {
        throw PageParseError();
      }
      // Load bytes
      persist::load(input, next_location, prev_location);
    }

    /**
     * Dump slot header as byte string.
     *
     * @param output output buffer span to dump
     */
    void Dump(Span output) override {
      if (output.size < GetStorageSize()) {
        throw PageParseError();
      }
      // Dump bytes
      persist::dump(output, next_location, prev_location);
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Header &other) const {
      return next_location == other.next_location &&
             prev_location == other.prev_location;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Header &other) const {
      return next_location != other.next_location ||
             prev_location != other.prev_location;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write slot header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "---- Header ----\n";
      os << "next: " << header.next_location << "\n";
      os << "prev: " << header.prev_location << "\n";
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

public:
  ByteBuffer data; //<- data contained in the record block

  /**
   * @brief Construct a new Page Slot object
   *
   */
  RecordPageSlot() {}
  RecordPageSlot(RecordPageSlot::Header header) : header(header) {}
  RecordPageSlot(ByteBuffer data) : data(data) {}
  RecordPageSlot(ByteBuffer data, RecordPageSlot::Header header)
      : data(data), header(header) {}

  /**
   * Get storage size of page slot.
   *
   */
  size_t GetStorageSize() const override {
    return header.GetStorageSize() + sizeof(size_t) + data.size();
  }

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
   * @brief Get the previous page slot location
   *
   * @return Location of the previous linked page slot
   */
  const Location &GetPrevLocation() const { return header.prev_location; }

  /**
   * @brief Set the previous page slot location
   *
   * @param location Location of the previous linked page slot
   */
  void SetPrevLocation(Location &location) { header.prev_location = location; }

  /**
   * Load page slot object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < GetStorageSize()) {
      throw PageParseError();
    }
    // Load header
    header.Load(input);
    input += header.GetStorageSize();
    // Load data
    persist::load(input, data);
  }

  /**
   * Dump page slot object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < GetStorageSize()) {
      throw PageParseError();
    }
    // Dump header
    header.Dump(output);
    output += header.GetStorageSize();
    // Dump data
    persist::dump(output, data);
  }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const RecordPageSlot &other) const {
    return header == other.header && data == other.data;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const RecordPageSlot &other) const {
    return header != other.header || data != other.data;
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write record block to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const RecordPageSlot &slot) {
    os << "------ PageSlot ------\n";
    os << slot.header << "\n";
    os << "data: " << slot.data << "\n";
    os << "--------------------------";
    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_RECORDPAGE_SLOT_HPP */
