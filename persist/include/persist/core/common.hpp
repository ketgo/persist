/**
 * common.hpp - Persist
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

#ifndef PERSIST_CORE_COMMON_HPP
#define PERSIST_CORE_COMMON_HPP

#include <vector>

#include <persist/core/defs.hpp>

namespace persist {

/**
 * @brief Byte data type.
 */
typedef uint8_t Byte;

/**
 * @brief Byte buffer data type.
 *
 */
typedef std::vector<Byte> ByteBuffer;

/**
 * @brief A span object contains the starting memory location and size of a
 * byte buffer.
 *
 */
typedef struct Span {
  /**
   * @brief Starting address of the byte buffer.
   *
   */
  Byte *start;

  /**
   * @brief Size of the byte buffer.
   *
   */
  size_t size;

  /**
   * @brief Construct a new Span object.
   *
   * @param start Starting memory address of byte buffer.
   * @param size Size of the byte buffer.
   * @param buffer Reference to ByteBuffer object.
   */
  Span() {}
  Span(Byte *start, size_t size) : start(start), size(size) {}
  Span(ByteBuffer &buffer) : start(buffer.data()), size(buffer.size()) {}

  /**
   * @brief Assignment operator
   *
   */
  inline void operator=(const Span &span) {
    this->start = span.start;
    this->size = span.size;
  }

  /**
   * @brief Shift the span by specifed size. It is left to the user to make sure
   * the specified shift is valid. An invalid size would be one which cases the
   * span to point at invalid memory address.
   *
   * @param size The size by which to shift the span.
   */
  inline void operator+=(const size_t &size) {
    this->start += size;
    this->size -= size;
  }

  /**
   * @brief Shift the span by specifed size. It is left to the user to make
   * sure the specified shift is valid. An invalid size would be one which
   * cases the span to point at invalid memory address.
   *
   * @param size The size by which to shift the span.
   */
  inline Span operator+(const size_t &size) {
    Span span;
    span.start = this->start + size;
    span.size = this->size - size;
    return span;
  }
} Span;

/**
 * @brief ByteBuffer literal `_bb`
 */
inline ByteBuffer operator"" _bb(const char *string, size_t size) {
  ByteBuffer buffer(size);
  for (int i = 0; i < size; i++) {
    buffer[i] = static_cast<Byte>(string[i]);
  }
  return buffer;
}

#ifdef __PERSIST_DEBUG__
/**
 * @brief Write byte buffer to output stream
 */
inline std::ostream &operator<<(std::ostream &os, const ByteBuffer &buffer) {
  for (auto c : buffer) {
    os << c;
  }
  return os;
}
#endif

/**
 * @brief Storable Abstract Base Class.
 *
 */
class Storable {
public:
  /**
   * @brief Get the storage size of the storable object.
   *
   * @returns storage size in bytes.
   */
  virtual size_t GetStorageSize() const = 0;

  /**
   * Load page object from byte string.
   *
   * @param input input buffer span to load
   */
  virtual void Load(Span input) = 0;

  /**
   * Dump page object as byte string.
   *
   * @param output output buffer span to dump
   */
  virtual void Dump(Span output) = 0;
};

/**
 * @brief Record location object represents abstract address of a record in
 * storage. It consists of page and slot identifer.
 *
 */
struct RecordLocation {
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
  RecordLocation() : page_id(0), slot_id(0) {}
  RecordLocation(PageId page_id, PageSlotId slot_id)
      : page_id(page_id), slot_id(slot_id) {}

  /**
   * @brief Check if location is NULL
   */
  bool IsNull() const { return page_id == 0; }

  /**
   * @brief Set the record location to NULL
   */
  void SetNull() {
    page_id = 0;
    slot_id = 0;
  }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const RecordLocation &other) const {
    return page_id == other.page_id && slot_id == other.slot_id;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const RecordLocation &other) const {
    return page_id != other.page_id || slot_id != other.slot_id;
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write record location to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const RecordLocation &location) {
    os << "[" << location.page_id << ", " << location.slot_id << "]";
    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_COMMON_HPP */
