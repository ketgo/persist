/**
 * test_memory_storage.cpp - Persist
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
 * Memory Storage Backend Test
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>
#include <persist/core/storage/memory_storage.hpp>

using namespace persist;

class MemoryStorageTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = 512;
  std::unique_ptr<MemoryStorage> storage;

  void SetUp() override {
    storage = std::make_unique<MemoryStorage>(pageSize);
    storage->open();
  }

  void TearDown() override { storage->close(); }
};

TEST_F(MemoryStorageTestFixture, TestReadPageError) {
  try {
    std::unique_ptr<Page> page = storage->read(1);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(MemoryStorageTestFixture, TestReadWritePage) {
  RecordBlock recordBlock;
  recordBlock.data = "testing"_bb;

  Page page(1, pageSize);
  PageSlotId slotId = page.addRecordBlock(recordBlock);
  storage->write(page);

  std::unique_ptr<Page> _page = storage->read(1);
  RecordBlock &_recordBlock = _page->getRecordBlock(slotId);

  ASSERT_EQ(page.getId(), _page->getId());
  ASSERT_EQ(recordBlock.data, _recordBlock.data);
}

TEST_F(MemoryStorageTestFixture, TestReadWriteMetaData) {
  MetaData metadata;
  metadata.pageSize = pageSize;
  metadata.freePages = {1, 2, 3};
  storage->write(metadata);

  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(metadata.freePages, _metadata->freePages);
  ASSERT_EQ(metadata.numPages, _metadata->numPages);
  ASSERT_EQ(metadata.pageSize, _metadata->pageSize);
}
