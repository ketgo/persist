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
#include <persist/core/storage/memory_storage.hpp>

using namespace persist;

class PageTableTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  std::unique_ptr<Page> page_1, page_2, page_3;
  std::unique_ptr<MetaData> metadata;
  std::unique_ptr<PageTable> table;
  std::unique_ptr<MemoryStorage> storage;

  void SetUp() override {
    // setting up pages
    page_1 = std::make_unique<Page>(1, pageSize);
    page_2 = std::make_unique<Page>(2, pageSize);
    page_3 = std::make_unique<Page>(3, pageSize);

    // setting up metadata
    metadata = std::make_unique<MetaData>();
    metadata->pageSize = pageSize;
    metadata->numPages = 3;
    metadata->freePages.insert(1);
    metadata->freePages.insert(2);
    metadata->freePages.insert(3);

    // setting up storage
    storage = std::make_unique<MemoryStorage>(pageSize);
    storage->write(*page_1);
    storage->write(*page_2);
    storage->write(*page_3);
    storage->write(*metadata);

    table = std::make_unique<PageTable>(*storage, maxSize);
  }
};

TEST_F(PageTableTestFixture, TestGet) {}

TEST_F(PageTableTestFixture, TestGetError) {}

TEST_F(PageTableTestFixture, TestGetLRUPersist) {}

TEST_F(PageTableTestFixture, TestGetNew) {}

TEST_F(PageTableTestFixture, TestGetFree) {}

TEST_F(PageTableTestFixture, TestGetFreeNew) {}

TEST_F(PageTableTestFixture, TestSessionStage) {}

TEST_F(PageTableTestFixture, TestSessionCommit) {}
