/**
 * simple_page.hpp - Persist
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

#ifndef PERSIST_TEST_SIMPLE_PAGE_HPP
#define PERSIST_TEST_SIMPLE_PAGE_HPP

#include <unordered_map>

#include <persist/core/exceptions.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

namespace test {

/**
 * @brief Simple Page Class
 *
 * The class represents a simple page storing a single record in bytes and
 * a header consisting of just page ID. The class can be used for unit testing
 * page dependent componenets.
 */
class SimplePage : public Page {
public:
  /**
   * @brief Simple Page Header
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

      Checksum seed = GetSize();

      seed =
          std::hash<PageId>()(page_id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);

      return seed;
    }

  public:
    /**
     * @brief Page unique identifer
     */
    PageId page_id;

    /**
     * @brief Storage size of the page.
     */
    size_t page_size;

    /**
     * @brief Checksum to detect page corruption
     */
    Checksum checksum;

    /**
     * @brief Construct a new Header object
     *
     */
    Header(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
        : page_id(page_id), page_size(page_size) {}

    /**
     * @brief Get storage size of header
     *
     */
    size_t GetSize() const { return sizeof(PageId) + sizeof(Checksum); }

    /**
     * Load object from byte string
     *
     * @param input input buffer span to load
     */
    void Load(Span input) {
      if (input.size < GetSize()) {
        throw PageParseError();
      }

      // Load bytes
      Byte *pos = input.start;
      std::memcpy((void *)&page_id, (const void *)pos, sizeof(PageId));
      pos += sizeof(PageId);
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
    void Dump(Span output) {
      if (output.size < GetSize()) {
        throw PageParseError();
      }

      // Compute and set checksum
      checksum = _checksum();

      // Dump bytes
      Byte *pos = output.start;
      std::memcpy((void *)pos, (const void *)&page_id, sizeof(PageId));
      pos += sizeof(PageId);
      std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write page header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "------- Header -------\n";
      os << "id: " << header.page_id << "\n";
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
   * @brief Record stored as bytes in page.
   *
   */
  ByteBuffer record;

public:
  /**
   * Constructors
   */
  SimplePage(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
      : header(page_id, page_size) {
    // Check page size greater than minimum size
    if (page_size < MINIMUM_PAGE_SIZE) {
      throw PageSizeError(page_size);
    }
  }

  /**
   * @brief Get the page type identifer.
   *
   * @returns The page type identifier
   */
  PageTypeId GetTypeId() const override { return 0; }

  /**
   * Get page ID.
   *
   * @returns page identifier
   */
  const PageId &GetId() const override { return header.page_id; }

  /**
   * @brief Get the storage free space size in the page for specified operation.
   *
   * @param operation Operaion to be performed
   * @returns Free space in bytes
   */
  size_t GetFreeSpaceSize(Operation operation) const override {
    size_t data_size = header.GetSize() + record.size() +
                       sizeof(size_t); // The record size is stored in the
                                       // backend storage thus added here
    // If stored data size greater than page size then return 0
    if (header.page_size <= data_size) {
      return 0;
    }

    return header.page_size - data_size;
  }

  /**
   * @brief Get data record
   *
   * @returns reference to stored record
   */
  const ByteBuffer &GetRecord() const { return record; }

  /**
   * @brief Set data record
   *
   * @param record lvalue reference to data record to be stored
   */
  void SetRecord(ByteBuffer &&record) { this->record = record; }

  /**
   * @brief Set data record
   *
   * @param record reference to data record to be stored
   */
  void SetRecord(ByteBuffer &record) { this->record = record; }

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < header.page_size) {
      throw PageParseError();
    }
    record.clear(); //<- clears data in case it is loaded

    // Load Page header
    header.Load(input);

    Byte *pos = input.start + header.GetSize();
    // Load record size
    size_t recordSize = 0;
    std::memcpy((void *)&recordSize, (const void *)pos, sizeof(size_t));
    pos += sizeof(size_t);
    // Load data
    record.resize(recordSize);
    std::memcpy((void *)record.data(), (const void *)(pos), recordSize);
  }

  /**
   * Dump Block object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < header.page_size) {
      throw PageParseError();
    }

    // Dump header
    header.Dump(output);

    Byte *pos = output.start + header.GetSize();
    // Dump record size
    size_t recordSize = record.size();
    std::memcpy((void *)pos, (const void *)&recordSize, sizeof(size_t));
    pos += sizeof(size_t);
    // Dump record
    std::memcpy((void *)(pos), (const void *)record.data(), record.size());
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write page to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const SimplePage &page) {
    os << "--------- Page " << page.header.page_id << " ---------\n";
    os << page.header << "\n";
    os << page.record << "\n";
    os << "-----------------------------";

    return os;
  }
#endif
};

} // namespace test

} // namespace persist

#endif /* PERSIST_TEST_SIMPLE_PAGE_HPP */
