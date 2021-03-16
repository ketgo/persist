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
#include <persist/core/page/creator.hpp>
#include <persist/core/storage/memory_storage.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class MemoryStorageTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = 512;
  std::unique_ptr<MemoryStorage> storage;

  void SetUp() override {
    storage = std::make_unique<MemoryStorage>(page_size);
    storage->Open();
  }

  void TearDown() override { storage->Close(); }
};

TEST_F(MemoryStorageTestFixture, TestReadPageError) {
  try {
    auto page = storage->Read(1);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(MemoryStorageTestFixture, TestReadWritePage) {
  auto page = CreatePage<SimplePage>(1, page_size);
  ByteBuffer record = "testing"_bb;
  page->SetRecord(record);

  storage->Write(*page);
  auto _page = storage->Read(1);

  ASSERT_EQ(page->GetId(), _page->GetId());
  ASSERT_EQ(page->GetRecord(),
            static_cast<SimplePage *>(_page.get())->GetRecord());
}

TEST_F(MemoryStorageTestFixture, TestAllocate) {
  ASSERT_EQ(storage->Allocate(), 1);
}

TEST_F(MemoryStorageTestFixture, TestReadWriteFSL) {
  FSL fsl;
  fsl.freePages = {1, 2, 3};
  storage->Write(fsl);

  std::unique_ptr<FSL> _fsl = storage->Read();

  ASSERT_EQ(fsl.freePages, _fsl->freePages);
}
