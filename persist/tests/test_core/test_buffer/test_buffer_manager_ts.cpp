/**
 * test_buffer_manager_ts.cpp - Persist
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
 * @brief Buffer Manager Thread Safety Tests
 *
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/buffer/replacer/lru_replacer.hpp>
#include <persist/core/page/simple_page.hpp>
#include <persist/core/storage/factory.hpp>

using namespace persist;

class BufferManagerThreadSafetyTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const std::string path = "test_buffer_manager_ts";
  std::unique_ptr<SimplePage> page_1, page_2, page_3;
  std::unique_ptr<FSL> fsl;
  typedef BufferManager<SimplePage> BufferManager;
  std::unique_ptr<BufferManager> bufferManager;
  std::unique_ptr<Storage<SimplePage>> storage;

  void SetUp() override {
    ByteBuffer buffer;

    // setting up pages
    page_1 = std::make_unique<SimplePage>(1, pageSize);
    page_1->setRecord("test_page_1"_bb);
    page_2 = std::make_unique<SimplePage>(2, pageSize);
    page_2->setRecord("test_page_2"_bb);
    page_3 = std::make_unique<SimplePage>(3, pageSize);
    page_3->setRecord("test_page_3"_bb);

    // setting up free space list
    fsl = std::make_unique<FSL>();
    fsl->freePages = {1, 2, 3};

    // setting up storage
    storage = createStorage<SimplePage>("file://" + path);
    insert();

    bufferManager = std::make_unique<BufferManager>(storage.get(), maxSize,
                                                    ReplacerType::LRU);
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

/**
 * @brief Test concurrent call to `get` method for same page when the internal
 * buffer is empty.
 *
 * One of the threads should load the page from the backend storage and return
 * its page handle. The other thread should wait for the page to be done loading
 * and then return the page handle.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIGetI) {
  // Assert buffer is empty
  ASSERT_TRUE(bufferManager->isEmpty());

  std::thread thread_i([this]() {
    auto page = bufferManager->get(1);

    ASSERT_EQ(page->getId(), page_1->getId());
    ASSERT_EQ(page->getRecord(), page_1->getRecord());
  });

  std::thread thread_j([this]() {
    auto page = bufferManager->get(1);

    ASSERT_EQ(page->getId(), page_1->getId());
    ASSERT_EQ(page->getRecord(), page_1->getRecord());
  });

  thread_i.join();
  thread_j.join();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(1));
  ASSERT_EQ(bufferManager->get(1)->getId(), page_1->getId());
  ASSERT_EQ(bufferManager->get(1)->getRecord(), page_1->getRecord());
}

/**
 * @brief Test concurrent call to `get` method for same page when the internal
 * buffer is full.
 *
 * One of the threads should load the page with replacement from the backend
 * storage and return its page handle. The other thread should wait for the page
 * to be done loading and then return the page handle.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids data race in the FSM, the backend storage, and the internal
 * buffer.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIGetI) {
  // Filling up buffer
  bufferManager->get(2);
  bufferManager->get(3);
  ASSERT_TRUE(bufferManager->isFull());

  std::thread thread_i([this]() {
    auto page = bufferManager->get(1);

    ASSERT_EQ(page->getId(), page_1->getId());
    ASSERT_EQ(page->getRecord(), page_1->getRecord());
  });

  std::thread thread_j([this]() {
    auto page = bufferManager->get(1);

    ASSERT_EQ(page->getId(), page_1->getId());
    ASSERT_EQ(page->getRecord(), page_1->getRecord());
  });

  thread_i.join();
  thread_j.join();

  // Assert the page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(1));
  ASSERT_EQ(bufferManager->get(1)->getId(), page_1->getId());
  ASSERT_EQ(bufferManager->get(1)->getRecord(), page_1->getRecord());

  // Assert only one victim page got replaced
  ASSERT_TRUE(bufferManager->isFull());
}

/**
 * @brief Test concurrent call to `get` method for different pages when the
 * internal buffer is empty.
 *
 * Both threads should load the pages from the backend storage. The loading of
 * the two pages should be done in a way that avoids a data race in the internal
 * buffer.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIGetJ) {
  // Assert buffer is empty
  ASSERT_TRUE(bufferManager->isEmpty());

  std::thread thread_i([this]() {
    auto page = bufferManager->get(1);

    ASSERT_EQ(page->getId(), page_1->getId());
    ASSERT_EQ(page->getRecord(), page_1->getRecord());
  });

  std::thread thread_j([this]() {
    auto page = bufferManager->get(2);

    ASSERT_EQ(page->getId(), page_2->getId());
    ASSERT_EQ(page->getRecord(), page_2->getRecord());
  });

  thread_i.join();
  thread_j.join();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(1));
  ASSERT_EQ(bufferManager->get(1)->getId(), page_1->getId());
  ASSERT_EQ(bufferManager->get(1)->getRecord(), page_1->getRecord());
  // Assert second page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(2));
  ASSERT_EQ(bufferManager->get(2)->getId(), page_2->getId());
  ASSERT_EQ(bufferManager->get(2)->getRecord(), page_2->getRecord());
}

/**
 * @brief Test concurrent call to `get` method for different pages when the
 * internal buffer is full.
 *
 * Both threads should load the pages with replacement from the backend storage.
 * The replacement of page involves finding a victim page from FSM, then
 * flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * storage.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIGetJ) {
  // Filling up buffer
  bufferManager->get(2);
  bufferManager->get(3);
  ASSERT_TRUE(bufferManager->isFull());

  std::thread thread_i([this]() {
    auto page = bufferManager->get(1);

    ASSERT_EQ(page->getId(), page_1->getId());
    ASSERT_EQ(page->getRecord(), page_1->getRecord());
  });

  std::thread thread_j([this]() {
    auto page = bufferManager->get(2);

    ASSERT_EQ(page->getId(), page_2->getId());
    ASSERT_EQ(page->getRecord(), page_2->getRecord());
  });

  thread_i.join();
  thread_j.join();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(1));
  ASSERT_EQ(bufferManager->get(1)->getId(), page_1->getId());
  ASSERT_EQ(bufferManager->get(1)->getRecord(), page_1->getRecord());
  // Assert second page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(2));
  ASSERT_EQ(bufferManager->get(2)->getId(), page_2->getId());
  ASSERT_EQ(bufferManager->get(2)->getRecord(), page_2->getRecord());
}

/**
 * @brief Test concurrent call to `get` and `flush` methods for same page when
 * the internal buffer is empty.
 *
 * While one thread calls the `get` method, the other calls `flush`. Since the
 * `get` call pins the page, the flush call should have no effect.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIFlushI) {
  // Assert buffer is empty
  ASSERT_TRUE(bufferManager->isEmpty());

  std::thread thread_i([this]() {
    auto page = bufferManager->get(1);

    page->setRecord("update_testing_1"_bb);
  });

  std::thread thread_j([this]() { bufferManager->flush(1); });

  thread_i.join();
  thread_j.join();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(1));
  ASSERT_EQ(bufferManager->get(1)->getId(), 1);
  ASSERT_EQ(bufferManager->get(1)->getRecord(), "update_testing_1"_bb);

  // Assert flush did not write page to backend storage
  auto page = storage->read(1);
  ASSERT_EQ(page->getId(), page_1->getId());
  ASSERT_EQ(page->getRecord(), page_1->getRecord());
}

/**
 * @brief Test concurrent call to `get` and `flush` methods for same page when
 * the internal buffer is full.
 *
 * While one thread calls the `get` method, loading a page by replacement from
 * backend storage, the other calls `flush`. Since the `get` call pins the page,
 * the flush call should have no effect.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * storage.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIFlushI) {
  // Filling up buffer
  bufferManager->get(2);
  bufferManager->get(3);
  ASSERT_TRUE(bufferManager->isFull());

  std::thread thread_i([this]() {
    auto page = bufferManager->get(1);

    page->setRecord("update_testing_1"_bb);
  });

  std::thread thread_j([this]() { bufferManager->flush(1); });

  thread_i.join();
  thread_j.join();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(bufferManager->isPageLoaded(1));
  ASSERT_EQ(bufferManager->get(1)->getId(), 1);
  ASSERT_EQ(bufferManager->get(1)->getRecord(), "update_testing_1"_bb);

  // Assert flush did not write page to backend storage
  auto page = storage->read(1);
  ASSERT_EQ(page->getId(), page_1->getId());
  ASSERT_EQ(page->getRecord(), page_1->getRecord());
}
