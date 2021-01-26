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
#include <persist/core/page/simple_page.hpp>
#include <persist/core/storage/memory_storage.hpp>

using namespace persist;

class MemoryStorageTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = 512;
  std::unique_ptr<MemoryStorage<SimplePage>> storage;

  void SetUp() override {
    storage = std::make_unique<MemoryStorage<SimplePage>>(pageSize);
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
  SimplePage page(1, pageSize);
  page.record = "testing"_bb;
  storage->write(page);
  std::unique_ptr<SimplePage> _page = storage->read(1);

  ASSERT_EQ(page.getId(), _page->getId());
  ASSERT_EQ(page.record, _page->record);
}

TEST_F(MemoryStorageTestFixture, TestAllocate) {
  ASSERT_EQ(storage->allocate(), 1);
}

TEST_F(MemoryStorageTestFixture, TestReadWriteFSL) {
  FSL fsl;
  fsl.freePages = {1, 2, 3};
  storage->write(fsl);

  std::unique_ptr<FSL> _fsl = storage->read();

  ASSERT_EQ(fsl.freePages, _fsl->freePages);
}
