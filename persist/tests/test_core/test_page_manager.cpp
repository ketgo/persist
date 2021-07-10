/**
 * test_page_manager.cpp - Persist
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
 * @brief Unit Test PageManager
 *
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <persist/core/fsm/fsl.hpp>
#include <persist/core/page/creator.hpp>
#include <persist/core/page_manager.hpp>
#include <persist/core/storage/creator.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class PageManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const ByteBuffer data = "testing"_bb;
  const std::string connection_string = "file://test_page_manager";
  std::unique_ptr<SimplePage> data_page;
  std::unique_ptr<FSLPage> fsl_page;
  std::unique_ptr<Storage<SimplePage>> data_storage;
  std::unique_ptr<Storage<FSLPage>> fsl_storage;
  std::unique_ptr<BufferManager<SimplePage>> buffer_manager;
  std::unique_ptr<FSLManager> fsl_manager;
  std::unique_ptr<PageManager<SimplePage, LRUReplacer, FSLManager>>
      page_manager;

  void SetUp() override {
    // Setting up pages
    data_page = persist::CreatePage<SimplePage>(1, page_size);
    data_page->SetRecord(data);
    fsl_page = persist::CreatePage<FSLPage>(1, page_size);
    fsl_page->Insert(1);

    // Setup backend storage
    data_storage = persist::CreateStorage<SimplePage>(
        ConnectionString(connection_string, DATA_STORAGE_EXTENTION));
    data_storage->Open();
    fsl_storage = persist::CreateStorage<FSLPage>(
        ConnectionString(connection_string, FSM_STORAGE_EXTENTION));
    fsl_storage->Open();

    // Insert page
    Insert();

    // Setup buffer manager
    buffer_manager = std::make_unique<BufferManager<SimplePage>>(
        data_storage.get(), max_size);
    buffer_manager->Start();

    // Setup free space manager
    fsl_manager = std::make_unique<FSLManager>(connection_string, max_size);
    fsl_manager->Start();

    // Setup page manager
    page_manager =
        std::make_unique<PageManager<SimplePage, LRUReplacer, FSLManager>>(
            *buffer_manager.get(), *fsl_manager.get());
    page_manager->Start();
  }

  void TearDown() override {
    buffer_manager->Stop();
    fsl_manager->Stop();
    page_manager->Stop();

    data_storage->Remove();
    fsl_storage->Remove();
  }

private:
  void Insert() {
    data_storage->Write(*data_page);
    data_storage->ReOpen();

    fsl_storage->Write(*fsl_page);
    fsl_storage->ReOpen();
  }
};

TEST_F(PageManagerTestFixture, TestGetPage) {
  auto page = page_manager->GetPage(1);

  ASSERT_EQ(page->GetId(), 1);
  ASSERT_EQ(page->GetRecord(), data);
}

TEST_F(PageManagerTestFixture, TestGetNewPage) {
  auto page = page_manager->GetNewPage();

  ASSERT_EQ(page->GetId(), 2);
  // Assert fsl manager tracks new page.
  ASSERT_EQ(fsl_manager->GetPageId(0), 2);

  // Fill page
  ByteBuffer data(page->GetFreeSpaceSize(Operation::INSERT), 'A');
  page->SetRecord(data);
  // Assert fsl manager stops tracking new page as it is full.
  ASSERT_EQ(fsl_manager->GetPageId(0), 1);
}

TEST_F(PageManagerTestFixture, TestGetFreeOrNewPage) {
  auto page = page_manager->GetFreeOrNewPage(0);

  // Returns page with free space.
  ASSERT_EQ(page->GetId(), 1);
  // Fill page
  ByteBuffer data(page->GetFreeSpaceSize(Operation::UPDATE), 'A');
  page->AppendRecord(data);

  auto new_page = page_manager->GetFreeOrNewPage(0);
  // Assert page manager returned new page.
  ASSERT_EQ(new_page->GetId(), 2);
}
