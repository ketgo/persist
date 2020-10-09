/**
 * test_manager.cpp - Persist
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

/**
 * Block Manager Cache Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <persist/core/exceptions.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/storage/base.hpp>

using namespace persist;

class MockStorage : public Storage {
public:
  void open() override {}
  bool is_open() override { return true; }
  void close() override {}
  std::unique_ptr<MetaData> read() override { return nullptr; }
  void write(MetaData &metadata) override {}
  std::unique_ptr<Page> read(PageId pageId) override { return nullptr; }
  void write(Page &page) override {}
};

class PageTableTestFixture : public ::testing::Test {
protected:
  const uint64_t maxSize = 100;
  std::unique_ptr<PageTable> manager;
  std::unique_ptr<MockStorage> storage;

  void SetUp() override {
    storage = std::make_unique<MockStorage>();
    manager = std::make_unique<PageTable>(*storage, maxSize);
  }
};

TEST_F(PageTableTestFixture, TestGet) {}

TEST_F(PageTableTestFixture, TestGetError) {}

TEST_F(PageTableTestFixture, TestMark) {}

TEST_F(PageTableTestFixture, TestFlush) {}
