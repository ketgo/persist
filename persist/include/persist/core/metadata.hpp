/**
 * metadata.hpp - Persist
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

#ifndef PERSIST__CORE__METADATA_HPP
#define PERSIST__CORE__METADATA_HPP

#include <persist/core/exceptions/metadata.hpp>
#include <persist/core/record.hpp>

namespace persist {

/**
 * @brief Metadata containing the number of elements in the collection and the
 * location of the first and last elemenet.
 *
 */
struct Metadata : public Storable {
  size_t count;         //<- Number of elements in the collection.
  RecordLocation first; //<- Location of the first node in the collection.
  RecordLocation last;  //<- Location of the last node in the collection.

  size_t GetStorageSize() const override {
    return sizeof(size_t) + 2 * sizeof(RecordLocation);
  }

  void Load(Span input) override {
    if (input.size < GetStorageSize()) {
      throw MetadataParseError();
    }
    persist::load(input, count, first, last);
  }

  void Dump(Span output) override {
    if (output.size < GetStorageSize()) {
      throw MetadataParseError();
    }
    persist::dump(output, count, first, last);
  }
};

} // namespace persist

#endif /* PERSIST__CORE__METADATA_HPP */
