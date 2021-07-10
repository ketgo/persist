/**
 * node.hpp - Persist
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

#ifndef PERSIST__LIST__NODE_HPP
#define PERSIST__LIST__NODE_HPP

#include <persist/core/exceptions/record.hpp>
#include <persist/core/record.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {

/**
 * @brief Linked List Node
 *
 * The node stores the record data in bytes and the linkage between the next
 * and prior node.
 *
 * @tparam RecordType The type of record.
 */
template <class RecordType> struct ListNode : public Storable {
  /**
   * @brief Next linked node
   */
  RecordLocation next;

  /**
   * @brief Previous linked node
   */
  RecordLocation previous;

  /**
   * @brief Record data as byte buffer
   */
  RecordType record;

  /**
   * @brief Get the storage size of the storable object.
   *
   * @returns storage size in bytes.
   */
  size_t GetStorageSize() const override {
    return 2 * sizeof(RecordLocation) + record.GetStorageSize();
  }

  /**
   * Load page object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < GetStorageSize()) {
      throw RecordParseError();
    }
    // Load bytes
    persist::load(input, next, previous);
    record.Load(input);
  }

  /**
   * Dump page object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < GetStorageSize()) {
      throw RecordParseError();
    }
    // Load bytes
    persist::dump(output, next, previous);
    record.Dump(output);
  }
};

} // namespace persist

#endif /* PERSIST__LIST__NODE_HPP */
