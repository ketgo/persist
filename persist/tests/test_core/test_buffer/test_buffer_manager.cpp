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

/**
 * Buffer Manager Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

/**
 * @brief Enable debug mode if not already enabled
 *
 */
#ifndef __PERSIST_DEBUG__
#define __PERSIST_DEBUG__
#endif

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/buffer/replacer/lru_replacer.hpp>
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
  std::unique_ptr<FSL> fsl;
  std::unique_ptr<BufferManager> buffer_manager;
  std::unique_ptr<Storage> storage;

  void SetUp() override {
    // setting up pages
    page_1 = persist::CreatePage<SimplePage>(1, page_size);
    page_2 = persist::CreatePage<SimplePage>(2, page_size);
    page_3 = persist::CreatePage<SimplePage>(3, page_size);

    // setting up free space list
    fsl = std::make_unique<FSL>();
    fsl->freePages = {1, 2, 3};

    // setting up storage
    storage = persist::CreateStorage("file://" + path);
    Insert();

    buffer_manager = std::make_unique<BufferManager>(storage.get(), max_size,
                                                     ReplacerType::LRU);
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
    storage->Write(*fsl);
    storage->Close();
  }
};

TEST_F(BufferManagerTestFixture, TestBufferManagerError) {
  try {
    BufferManager manager(storage.get(), 1); //<- invalid max size value
    FAIL() << "Expected BufferManagerError Exception.";
  } catch (BufferManagerError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected BufferManagerError Exception.";
  }
}

TEST_F(BufferManagerTestFixture, TestGet) {
  PageId page_id = page_1->GetId();
  auto page = buffer_manager->Get<SimplePage>(page_id);

  ASSERT_EQ(page->GetId(), page_id);
  ASSERT_EQ(page->GetFreeSpaceSize(Operation::INSERT),
            page_1->GetFreeSpaceSize(Operation::INSERT));
}

TEST_F(BufferManagerTestFixture, TestGetError) {
  try {
    auto page = buffer_manager->Get<SimplePage>(10);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(BufferManagerTestFixture, TestGetNew) {
  auto page = buffer_manager->GetNew<SimplePage>();

  // A new page with ID 4 should be created
  ASSERT_EQ(page->GetId(), 4);
}

TEST_F(BufferManagerTestFixture, TestGetFree) {
  auto page = buffer_manager->GetFreeOrNew<SimplePage>();

  // Check if page has free space
  ASSERT_TRUE(page->GetFreeSpaceSize(Operation::INSERT) > 0);
}

TEST_F(BufferManagerTestFixture, TestGetFreeForNewPage) {
  // Fill up all pages
  for (int i = 1; i <= 3; i++) {
    auto page = buffer_manager->Get<SimplePage>(i);
    ByteBuffer record(page->GetFreeSpaceSize(Operation::INSERT), 'A');
    page->SetRecord(record);
  }

  auto page = buffer_manager->GetFreeOrNew<SimplePage>();

  // Check if page has free space
  ASSERT_TRUE(page->GetFreeSpaceSize(Operation::INSERT) > 0);
  // Check for new page
  ASSERT_EQ(page->GetId(), 4);
}

TEST_F(BufferManagerTestFixture, TestFlush) {
  ByteBuffer record;

  // Sub-block needed to release page handle
  {
    auto page = buffer_manager->Get<SimplePage>(1);
    record = ByteBuffer(page->GetFreeSpaceSize(Operation::INSERT), 'A');
    page->SetRecord(record);
  }

  ASSERT_TRUE(buffer_manager->Flush(1));

  // Load page from storage using a new buffer manager instance
  auto _page = storage->Read(1);

  ASSERT_EQ(_page->GetId(), 1);
  ASSERT_EQ(static_cast<SimplePage *>(_page.get())->GetRecord(), record);
}

TEST_F(BufferManagerTestFixture, TestFlushAll) {
  std::vector<ByteBuffer> records(3);

  for (int i = 1; i <= 3; i++) {
    auto page = buffer_manager->Get<SimplePage>(i);
    records[i - 1] = "testing"_bb;
    page->SetRecord(records[i - 1]);
  }

  buffer_manager->FlushAll();

  for (int i = 1; i <= 3; i++) {
    auto _page = storage->Read(i);
    ASSERT_EQ(_page->GetId(), i);
    ASSERT_EQ(static_cast<SimplePage *>(_page.get())->GetRecord(),
              records[i - 1]);
  }
}

TEST_F(BufferManagerTestFixture, TestPageReplacement) {
  ByteBuffer record = "testing"_bb;

  // Load and update pages in buffer
  for (int i = 1; i <= 3; i++) {
    auto page = buffer_manager->Get<SimplePage>(i);
    page->SetRecord(record);
  }

  // Only replaced page is persisted
  auto _page = storage->Read(1);
  ASSERT_EQ(_page->GetId(), 1);
  ASSERT_EQ(static_cast<SimplePage *>(_page.get())->GetRecord(), record);
  // Other pages are not persisted
  for (int i = 2; i <= 3; i++) {
    auto _page = storage->Read(i);
    ASSERT_EQ(_page->GetId(), i);
    ASSERT_EQ(static_cast<SimplePage *>(_page.get())->GetRecord(), ""_bb);
  }
}
