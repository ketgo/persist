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
#include <persist/core/page/simple_page.hpp>

using namespace persist;

class PageHandleTestFixture : public ::testing::Test {
protected:
  const PageId pageId = 1;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SimplePage> page;
  std::unique_ptr<LRUReplacer> replacer;
  typedef PageHandle<SimplePage> PageHandle;

  void SetUp() override {
    // setting up pages
    page = std::make_unique<SimplePage>(pageId, pageSize);

    // setting up replacer
    replacer = std::make_unique<LRUReplacer>();
    replacer->track(pageId);
  }

  void TearDown() override {}
};

TEST_F(PageHandleTestFixture, TestLifeCycle) {
  ASSERT_TRUE(!replacer->isPinned(pageId));

  {
    PageHandle pageHandle(page.get(), replacer.get());
    ASSERT_TRUE(replacer->isPinned(pageId));
    ASSERT_EQ(pageHandle->getId(), pageId);
  }

  ASSERT_TRUE(!replacer->isPinned(pageId));
}
