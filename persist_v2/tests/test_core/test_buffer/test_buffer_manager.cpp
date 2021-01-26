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

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/buffer/replacer/lru_replacer.hpp>
#include <persist/core/page/simple_page.hpp>
#include <persist/core/storage/factory.hpp>

using namespace persist;

class BufferManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const char *file = "file://test_buffer_manager";
  std::unique_ptr<SimplePage> page_1, page_2, page_3;
  std::unique_ptr<FSL> fsl;
  typedef BufferManager<SimplePage, LRUReplacer> BufferManager;
  std::unique_ptr<BufferManager> bufferManager;
  std::unique_ptr<Storage<SimplePage>> storage;

  void SetUp() override {
    // setting up pages
    page_1 = std::make_unique<SimplePage>(1, pageSize);
    page_2 = std::make_unique<SimplePage>(2, pageSize);
    page_3 = std::make_unique<SimplePage>(3, pageSize);

    // setting up free space list
    fsl = std::make_unique<FSL>();
    fsl->freePages = {1, 2, 3};

    // setting up storage
    storage = createStorage<SimplePage>(file);
    insert();

    bufferManager = std::make_unique<BufferManager>(*storage, maxSize);
    bufferManager->start();
  }

  void TearDown() override {
    storage->remove();
    bufferManager->stop();
  }

private:
  /**
   * @brief Insert test data
   */
  void insert() {
    storage->open();
    storage->write(*page_1);
    storage->write(*page_2);
    storage->write(*page_3);
    storage->write(*fsl);
    storage->close();
  }
};

TEST_F(BufferManagerTestFixture, TestBufferManagerError) {
  try {
    BufferManager manager(*storage, 1); //<- invalid max size value
    FAIL() << "Expected BufferManagerError Exception.";
  } catch (BufferManagerError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected BufferManagerError Exception.";
  }
}

TEST_F(BufferManagerTestFixture, TestGet) {
  PageId pageId = page_1->getId();
  auto page = bufferManager->get(pageId);

  ASSERT_EQ(page->getId(), pageId);
  ASSERT_EQ(page->freeSpace(Page::Operation::INSERT),
            page_1->freeSpace(Page::Operation::INSERT));
}

TEST_F(BufferManagerTestFixture, TestGetError) {
  try {
    auto page = bufferManager->get(10);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}
