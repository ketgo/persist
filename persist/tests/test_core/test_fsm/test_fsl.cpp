/**
 * test_fsl.cpp - Persist
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
 * Free space list unit tests.
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <persist/core/fsm/fsl.hpp>
#include <persist/core/page/creator.hpp>
#include <persist/core/storage/creator.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class FSLTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const size_t cache_size = 2;
  const std::set<PageId> free_pages = {1, 2, 3};
  const std::string path = "test_fsl_manager";
  const PageId fsl_page_id = 1;
  PageId full_page_id, empty_page_id_1, empty_page_id_2;
  std::unique_ptr<FSLManager> fsl_manager;
  std::unique_ptr<Storage<FSLPage>> storage;
  std::unique_ptr<FSLPage> fsl_page;
  std::unique_ptr<SimplePage> full_page, empty_page_1, empty_page_2;

  void SetUp() override {
    // Setup FSL Storage
    storage = persist::CreateStorage<FSLPage>("file://" + path);

    // Setup FSL page
    fsl_page = persist::CreatePage<FSLPage>(1, DEFAULT_PAGE_SIZE);
    fsl_page->free_pages = free_pages;
    Insert();

    // Setup FSL Manager
    fsl_manager = std::make_unique<FSLManager>(*storage, cache_size);
    fsl_manager->Start();

    // Setup empty page
    empty_page_id_1 = fsl_page->GetMaxPageId();
    empty_page_1 =
        persist::CreatePage<SimplePage>(empty_page_id_1, DEFAULT_PAGE_SIZE);
    empty_page_id_2 = fsl_page->GetMaxPageId() + 1;
    empty_page_2 =
        persist::CreatePage<SimplePage>(empty_page_id_2, DEFAULT_PAGE_SIZE);

    // Setup full page
    full_page_id = 3;
    full_page =
        persist::CreatePage<SimplePage>(full_page_id, DEFAULT_PAGE_SIZE);
    ByteBuffer record(full_page->GetFreeSpaceSize(Operation::INSERT), 'A');
    full_page->SetRecord(record);
  }

  void TearDown() override {
    storage->Remove();
    fsl_manager->Stop();
  }

private:
  /**
   * @brief Insert test data
   */
  void Insert() {
    storage->Open();
    storage->Write(*fsl_page);
    storage->Close();
  }
};

TEST_F(FSLTestFixture, TestManagerGetPageId) {
  ASSERT_EQ(fsl_manager->GetPageId(0), 3);
}

TEST_F(FSLTestFixture, TestManagerEmptyPage) {
  fsl_manager->Manage(*empty_page_1);
  ASSERT_EQ(storage->GetPageCount(), 1);
  ASSERT_EQ(fsl_manager->GetPageId(0), empty_page_id_1);

  // Test duplicate
  fsl_manager->Manage(*empty_page_1);
  ASSERT_EQ(storage->GetPageCount(), 1);
  ASSERT_EQ(fsl_manager->GetPageId(0), empty_page_id_1);

  // Test entry in new FSLPage
  fsl_manager->Manage(*empty_page_2);
  ASSERT_EQ(storage->GetPageCount(), 2);
  ASSERT_EQ(fsl_manager->GetPageId(0), empty_page_id_2);
}

TEST_F(FSLTestFixture, TestManagerFullPage) {
  fsl_manager->Manage(*full_page);

  ASSERT_EQ(storage->GetPageCount(), 1);
  ASSERT_EQ(fsl_manager->GetPageId(0), 2);
}
