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

#include <memory>
#include <unordered_map>

#include <persist/core/exceptions/page.hpp>
#include <persist/core/page/base.hpp>

#include <persist/utility/serializer.hpp>

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
  class Header : public Storable {
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
     * @brief Construct a new Header object
     *
     */
    Header(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
        : page_id(page_id), page_size(page_size) {}

    /**
     * @brief Get storage size of header
     *
     */
    size_t GetStorageSize() const override { return sizeof(PageId); }

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
      persist::load(input, page_id);
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
      persist::dump(output, page_id);
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
      : header(page_id, page_size) {}

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
    size_t data_size = header.GetStorageSize() + record.size() +
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
  void SetRecord(ByteBuffer &&record) {
    this->record = record;
    NotifyObservers();
  }

  /**
   * @brief Set data record
   *
   * @param record Constant reference to data record to be stored
   */
  void SetRecord(const ByteBuffer &record) {
    this->record = record;
    NotifyObservers();
  }

  /**
   * @brief Append data record
   *
   * @param record Constant reference to data to be appended.
   */
  void AppendRecord(const ByteBuffer &record) {
    this->record.insert(this->record.end(), record.begin(), record.end());
    NotifyObservers();
  }

  /**
   * @brief Get the page storage size.
   *
   */
  size_t GetStorageSize() const override { return header.page_size; }

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < GetStorageSize()) {
      throw PageParseError();
    }
    record.clear(); //<- clears data in case it is loaded

    // Load Page header
    header.Load(input);
    input += header.GetStorageSize();
    persist::load(input, record);
  }

  /**
   * Dump Block object as byte string.
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
    // Dump record size
    persist::dump(output, record);
    // Dump free space
    std::memset((void *)output.start, 0, output.size);
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
