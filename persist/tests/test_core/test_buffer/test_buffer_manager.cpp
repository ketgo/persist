/**
 * test_buffer_manager.cpp - Persist
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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>

/**
 * @brief Enable debug mode if not already enabled
 *
 */
#ifndef __PERSIST_DEBUG__
#define __PERSIST_DEBUG__
#endif

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/page/creator.hpp>
#include <persist/core/storage/creator.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class BufferManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const std::string path = "test_buffer_manager";
  std::unique_ptr<SimplePage> page_1, page_2, page_3;
  std::unique_ptr<BufferManager<SimplePage>> buffer_manager;
  std::unique_ptr<Storage<SimplePage>> storage;

  void SetUp() override {
    // setting up pages
    page_1 = persist::CreatePage<SimplePage>(1, page_size);
    page_2 = persist::CreatePage<SimplePage>(2, page_size);
    page_3 = persist::CreatePage<SimplePage>(3, page_size);

    // setting up storage
    storage = persist::CreateStorage<SimplePage>("file://" + path);
    Insert();

    buffer_manager =
        std::make_unique<BufferManager<SimplePage>>(storage.get(), max_size);
    buffer_manager->Start();
  }

  void TearDown() override {
    storage->Remove();
    buffer_manager->Stop();
  }

private:
  /**
   * @brief Insert test data
   */
  void Insert() {
    storage->Open();
    storage->Write(*page_1);
    storage->Write(*page_2);
    storage->Write(*page_3);
    storage->Close();
  }
};

TEST_F(BufferManagerTestFixture, TestBufferManagerError) {
  ASSERT_THROW(BufferManager<SimplePage> manager(storage.get(), 1),
               BufferManagerError);
}

TEST_F(BufferManagerTestFixture, TestGet) {
  PageId page_id = page_1->GetId();
  auto page = buffer_manager->Get(page_id);

  ASSERT_EQ(page->GetId(), page_id);
  ASSERT_EQ(page->GetFreeSpaceSize(Operation::INSERT),
            page_1->GetFreeSpaceSize(Operation::INSERT));
}

TEST_F(BufferManagerTestFixture, TestGetError) {
  ASSERT_THROW(buffer_manager->Get(10), PageNotFoundError);
}

TEST_F(BufferManagerTestFixture, TestFlush) {
  ByteBuffer record;

  // Sub-block needed to release page handle
  {
    auto page = buffer_manager->Get(1);
    record = ByteBuffer(page->GetFreeSpaceSize(Operation::INSERT), 'A');
    page->SetRecord(record);
  }

  ASSERT_TRUE(buffer_manager->Flush(1));

  // Load page from storage using a new buffer manager instance
  auto _page = storage->Read(1);

  ASSERT_EQ(_page->GetId(), 1);
  ASSERT_EQ(_page->GetRecord(), record);
}

TEST_F(BufferManagerTestFixture, TestFlushAll) {
  std::vector<ByteBuffer> records(3);

  for (int i = 1; i <= 3; i++) {
    auto page = buffer_manager->Get(i);
    records[i - 1] = "testing"_bb;
    page->SetRecord(records[i - 1]);
  }

  buffer_manager->FlushAll();

  for (int i = 1; i <= 3; i++) {
    auto _page = storage->Read(i);
    ASSERT_EQ(_page->GetId(), i);
    ASSERT_EQ(_page->GetRecord(), records[i - 1]);
  }
}

TEST_F(BufferManagerTestFixture, TestPageReplacement) {
  ByteBuffer record = "testing"_bb;

  // Load and update pages in buffer
  for (int i = 1; i <= 3; i++) {
    auto page = buffer_manager->Get(i);
    page->SetRecord(record);
  }

  // Only replaced page is persisted
  auto _page = storage->Read(1);
  ASSERT_EQ(_page->GetId(), 1);
  ASSERT_EQ(_page->GetRecord(), record);
  // Other pages are not persisted
  for (int i = 2; i <= 3; i++) {
    auto _page = storage->Read(i);
    ASSERT_EQ(_page->GetId(), i);
    ASSERT_EQ(_page->GetRecord(), ""_bb);
  }
}
