/**
 * test_page_handle.cpp - Persist
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
 * Page Handle Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/buffer/page_handle.hpp>
#include <persist/core/buffer/replacer/lru_replacer.hpp>
#include <persist/core/page/factory.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class PageHandleTestFixture : public ::testing::Test {
protected:
  const PageId page_id_1 = 1, page_id_2 = 2;
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SimplePage> page_1, page_2;
  std::unique_ptr<LRUReplacer> replacer;
  typedef PageHandle<SimplePage> PageHandle;

  void SetUp() override {
    // setting up pages
    page_1 = persist::CreatePage<SimplePage>(page_id_1, page_size);
    page_2 = persist::CreatePage<SimplePage>(page_id_2, page_size);

    // setting up replacer
    replacer = std::make_unique<LRUReplacer>();
    replacer->Track(page_id_1);
    replacer->Track(page_id_2);
  }

  void TearDown() override {}

  /**
   * @brief Helper function to get page handle
   */
  PageHandle GetPageHandle(PageId page_id) {
    if (page_id == page_id_1) {
      return PageHandle(page_1.get(), replacer.get());
    }
    return PageHandle(page_2.get(), replacer.get());
  }
};

TEST_F(PageHandleTestFixture, TestLifeCycle) {
  ASSERT_TRUE(!replacer->IsPinned(page_id_1));

  {
    PageHandle pageHandle = GetPageHandle(page_id_1);
    ASSERT_TRUE(replacer->IsPinned(page_id_1));
    ASSERT_EQ(pageHandle->GetId(), page_id_1);
  }

  ASSERT_TRUE(!replacer->IsPinned(page_id_1));
}

TEST_F(PageHandleTestFixture, TestMoveConstructor) {
  ASSERT_TRUE(!replacer->IsPinned(page_id_1));

  {
    PageHandle pageHandle(std::move(GetPageHandle(page_id_1)));
    ASSERT_TRUE(replacer->IsPinned(page_id_1));
    ASSERT_EQ(pageHandle->GetId(), page_id_1);
  }

  ASSERT_TRUE(!replacer->IsPinned(page_id_1));
}

TEST_F(PageHandleTestFixture, TestMoveAssignment) {
  ASSERT_TRUE(!replacer->IsPinned(page_id_1));
  ASSERT_TRUE(!replacer->IsPinned(page_id_2));

  {
    PageHandle pageHandle = GetPageHandle(page_id_1);
    ASSERT_TRUE(replacer->IsPinned(page_id_1));
    ASSERT_EQ(pageHandle->GetId(), page_id_1);

    // Move assignment
    pageHandle = std::move(GetPageHandle(page_id_2));
    ASSERT_TRUE(!replacer->IsPinned(page_id_1));
    ASSERT_TRUE(replacer->IsPinned(page_id_2));
    ASSERT_EQ(pageHandle->GetId(), page_id_2);
  }

  ASSERT_TRUE(!replacer->IsPinned(page_id_1));
  ASSERT_TRUE(!replacer->IsPinned(page_id_2));
}
