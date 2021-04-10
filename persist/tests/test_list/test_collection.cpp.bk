/**
 * test_collection.cpp - Persist
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

/**
 * @brief Uint Test List Collection
 *
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <persist/list/collection.hpp>
#include <persist/utility/serializer.hpp>

using namespace persist;

class ListCollectionTestFixture : public ::testing::Test {
protected:
  /**
   * @brief Record stored by the collection.
   *
   */
  struct Record : public Storable {
    std::string data;

    size_t GetStorageSize() const override { return data.size(); }
    void Load(Span input) override { load(input, data); }
    void Dump(Span output) override { dump(output, data); }
  };
  std::unique_ptr<List<Record>> list;

  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(ListCollectionTestFixture, TestIterator) {}
