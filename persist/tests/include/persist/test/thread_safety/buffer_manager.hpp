/**
 * thread_safety/this->buffer_manager.hpp - Persist
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
 * @brief BufferManager thread safety tests. The interface provided in this
 * file can be used to test thread safety of custom BufferManager
 * implementations.
 *
 */

#ifndef PERSIST_TEST_THREADSAFETY_BUFFER_MANAGER_HPP
#define PERSIST_TEST_THREADSAFETY_BUFFER_MANAGER_HPP

#include <gtest/gtest.h>
#include <tstest/tstest.hpp>

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

#include <persist/core/buffer/replacer/lru_replacer.hpp>
#include <persist/core/storage/factory.hpp>

#include "persist/test/simple_page.hpp"

namespace persist {
namespace test {

/**
 * @brief Replacer thread safety test fixture.
 *
 * @tparam ReplacerType type of replacer
 */
template <class BufferManagerType>
class BufferManagerThreadSafetyTestFixture : public testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const std::string path = "test_buffer_manager_ts";
  std::unique_ptr<SimplePage> page_1, page_2, page_3;
  std::unique_ptr<FSL> fsl;
  std::unique_ptr<BufferManagerType> buffer_manager;
  std::unique_ptr<Storage> storage;

  // TSTest runner
  tstest::Runner runner;
  // TSTest assertor
  tstest::Assertor assertor;

  void SetUp() override {
    PageFactory::RegisterPage<SimplePage>();

    // Setting up pages
    ByteBuffer buffer;
    this->page_1 = persist::CreatePage<SimplePage>(1, this->page_size);
    this->page_1->SetRecord("test_this->page_1"_bb);
    this->page_2 = persist::CreatePage<SimplePage>(2, this->page_size);
    this->page_2->SetRecord("test_this->page_2"_bb);
    this->page_3 = persist::CreatePage<SimplePage>(3, this->page_size);
    this->page_3->SetRecord("test_this->page_3"_bb);

    // setting up free space list
    this->fsl = std::make_unique<FSL>();
    this->fsl->freePages = {1, 2, 3};

    // setting up this->storage
    this->storage = persist::CreateStorage("file://" + this->path);
    Insert();

    this->buffer_manager = std::make_unique<BufferManagerType>(
        this->storage.get(), this->max_size, ReplacerType::LRU);
    this->buffer_manager->Start();
  }

  void TearDown() override {
    this->storage->Remove();
    this->buffer_manager->Stop();
  }

private:
  /**
   * @brief Insert test data
   */
  void Insert() {
    this->storage->Open();
    this->storage->Write(*this->page_1);
    this->storage->Write(*this->page_2);
    this->storage->Write(*this->page_3);
    this->storage->Write(*this->fsl);
    this->storage->Close();
  }
};

TYPED_TEST_SUITE_P(BufferManagerThreadSafetyTestFixture);

/**
 * @brief Test concurrent call to `get` method for same page when the internal
 * buffer is empty.
 *
 * One of the threads should load the page from the backend this->storage and
 * return its page handle. The other thread should wait for the page to be done
 * loading and then return the page handle.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIGetI) {
  // Assert buffer is empty
  ASSERT_TRUE(this->buffer_manager->IsEmpty());

  THREAD(this->runner, "thread-a") {
    auto page = this->buffer_manager->Get<SimplePage>(1);

    ASSERT_EQ(page->GetId(), this->page_1->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_1->GetRecord());
  };

  THREAD(this->runner, "thread-b") {
    auto page = this->buffer_manager->Get<SimplePage>(1);

    ASSERT_EQ(page->GetId(), this->page_1->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_1->GetRecord());
  };

  this->runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(),
            this->page_1->GetId());
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            this->page_1->GetRecord());
}

/**
 * @brief Test concurrent call to `get` method for same page when the internal
 * buffer is full.
 *
 * One of the threads should load the page with replacement from the backend
 * this->storage and return its page handle. The other thread should wait for
 * the page to be done loading and then return the page handle.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids data race in the FSM, the backend this->storage, and the
 * internal buffer.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIGetI) {
  // Filling up buffer
  this->buffer_manager->Get<SimplePage>(2);
  this->buffer_manager->Get<SimplePage>(3);
  ASSERT_TRUE(this->buffer_manager->IsFull());

  THREAD(this->runner, "thread-a") {
    auto page = this->buffer_manager->Get<SimplePage>(1);

    ASSERT_EQ(page->GetId(), this->page_1->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_1->GetRecord());
  };

  THREAD(this->runner, "thread-b") {
    auto page = this->buffer_manager->Get<SimplePage>(1);

    ASSERT_EQ(page->GetId(), this->page_1->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_1->GetRecord());
  };

  this->runner.Run();

  // Assert the page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(),
            this->page_1->GetId());
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            this->page_1->GetRecord());

  // Assert only one victim page got replaced
  ASSERT_TRUE(this->buffer_manager->IsFull());
}

/**
 * @brief Test concurrent call to `get` method for different pages when the
 * internal buffer is empty.
 *
 * Both threads should load the pages from the backend this->storage. The
 * loading of the two pages should be done in a way that avoids a data race in
 * the internal buffer.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIGetJ) {
  // Assert buffer is empty
  ASSERT_TRUE(this->buffer_manager->IsEmpty());

  THREAD(this->runner, "thread-a") {
    auto page = this->buffer_manager->Get<SimplePage>(1);

    ASSERT_EQ(page->GetId(), this->page_1->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_1->GetRecord());
  };

  THREAD(this->runner, "thread-b") {
    auto page = this->buffer_manager->Get<SimplePage>(2);

    ASSERT_EQ(page->GetId(), this->page_2->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_2->GetRecord());
  };

  this->runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(),
            this->page_1->GetId());
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            this->page_1->GetRecord());
  // Assert second page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(2));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(2)->GetId(),
            this->page_2->GetId());
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(2)->GetRecord(),
            this->page_2->GetRecord());
}

/**
 * @brief Test concurrent call to `get` method for different pages when the
 * internal buffer is full.
 *
 * Both threads should load the pages with replacement from the backend
 * this->storage. The replacement of page involves finding a victim page from
 * FSM, then Flushing and removing it from the internal buffer. Thus it should
 * be done in a way that avoids a data race in the internal buffer, FSM and the
 * backend this->storage.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIGetJ) {
  // Filling up buffer
  this->buffer_manager->Get<SimplePage>(2);
  this->buffer_manager->Get<SimplePage>(3);
  ASSERT_TRUE(this->buffer_manager->IsFull());

  THREAD(this->runner, "thread-a") {
    auto page = this->buffer_manager->Get<SimplePage>(1);

    ASSERT_EQ(page->GetId(), this->page_1->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_1->GetRecord());
  };

  THREAD(this->runner, "thread-b") {
    auto page = this->buffer_manager->Get<SimplePage>(2);

    ASSERT_EQ(page->GetId(), this->page_2->GetId());
    ASSERT_EQ(page->GetRecord(), this->page_2->GetRecord());
  };

  this->runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(),
            this->page_1->GetId());
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            this->page_1->GetRecord());
  // Assert second page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(2));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(2)->GetId(),
            this->page_2->GetId());
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(2)->GetRecord(),
            this->page_2->GetRecord());
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for same page when
 * the internal buffer is empty.
 *
 * While one thread calls the `get` method, the other calls `Flush`. Since the
 * `get` call pins the page, the Flush call should have no effect.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIFlushI) {
  // Assert buffer is empty
  ASSERT_TRUE(this->buffer_manager->IsEmpty());

  THREAD(this->runner, "thread-a") {
    OPERATION("GetPage", auto page = this->buffer_manager->Get<SimplePage>(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(this->runner, "thread-b") {
    OPERATION("Flush", this->buffer_manager->Flush(1));
  };

  // Sequence of events which do not persist any changes to page
  this->assertor.InsertMany(
      {// Sequence 1
       {{"thread-b", "Flush", tstest::Event::Type::BEGIN},
        {"thread-b", "Flush", tstest::Event::Type::END},
        {"thread-a", "GetPage", tstest::Event::Type::BEGIN},
        {"thread-a", "GetPage", tstest::Event::Type::END},
        {"thread-a", "SetRecord", tstest::Event::Type::BEGIN},
        {"thread-a", "SetRecord", tstest::Event::Type::END}},
       // Sequence 2
       {{"thread-a", "GetPage", tstest::Event::Type::BEGIN},
        {"thread-a", "GetPage", tstest::Event::Type::END},
        {"thread-b", "Flush", tstest::Event::Type::BEGIN},
        {"thread-b", "Flush", tstest::Event::Type::END},
        {"thread-a", "SetRecord", tstest::Event::Type::BEGIN},
        {"thread-a", "SetRecord", tstest::Event::Type::END}}},
      [&]() {
        // Assert Flush did not write changes to backend this->storage
        auto page = this->storage->Read(1);
        ASSERT_EQ(page->GetId(), this->page_1->GetId());
        ASSERT_EQ(static_cast<SimplePage *>(page.get())->GetRecord(),
                  this->page_1->GetRecord());
      });

  // Sequence of events which persist changes to page
  this->assertor.InsertMany(
      {
          // Sequence 1
          {{"thread-a", "GetPage", tstest::Event::Type::BEGIN},
           {"thread-a", "GetPage", tstest::Event::Type::END},
           {"thread-a", "SetRecord", tstest::Event::Type::BEGIN},
           {"thread-a", "SetRecord", tstest::Event::Type::END},
           {"thread-b", "Flush", tstest::Event::Type::BEGIN},
           {"thread-b", "Flush", tstest::Event::Type::END}}
          // Sequence 2
      },
      [&]() {
        // Assert Flush wrote changes to backend this->storage
        auto page = this->storage->Read(1);
        ASSERT_EQ(page->GetId(), this->page_1->GetId());
        ASSERT_EQ(static_cast<SimplePage *>(page.get())->GetRecord(),
                  "update_testing_1"_bb);
      });

  this->runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(), 1);
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            "update_testing_1"_bb);

  // Assert the observed sequence of events
  this->assertor.Assert(this->runner.GetEventLog());
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for same page when
 * the internal buffer is full.
 *
 * While one thread calls the `get` method, loading a page by replacement from
 * backend this->storage, the other calls `Flush`. Since the `get` call pins the
 * page, the Flush call should have no effect.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * this->storage.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIFlushI) {
  // Filling up buffer
  this->buffer_manager->Get<SimplePage>(2);
  this->buffer_manager->Get<SimplePage>(3);
  ASSERT_TRUE(this->buffer_manager->IsFull());

  THREAD(this->runner, "thread-a") {
    OPERATION("GetPage", auto page = this->buffer_manager->Get<SimplePage>(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(this->runner, "thread-b") {
    OPERATION("Flush", this->buffer_manager->Flush(1));
  };

  // Sequence of events which do not persist any changes to page
  this->assertor.InsertMany(
      {// Sequence 1
       {{"thread-b", "Flush", tstest::Event::Type::BEGIN},
        {"thread-b", "Flush", tstest::Event::Type::END},
        {"thread-a", "GetPage", tstest::Event::Type::BEGIN},
        {"thread-a", "GetPage", tstest::Event::Type::END},
        {"thread-a", "SetRecord", tstest::Event::Type::BEGIN},
        {"thread-a", "SetRecord", tstest::Event::Type::END}},
       // Sequence 2
       {{"thread-a", "GetPage", tstest::Event::Type::BEGIN},
        {"thread-a", "GetPage", tstest::Event::Type::END},
        {"thread-b", "Flush", tstest::Event::Type::BEGIN},
        {"thread-b", "Flush", tstest::Event::Type::END},
        {"thread-a", "SetRecord", tstest::Event::Type::BEGIN},
        {"thread-a", "SetRecord", tstest::Event::Type::END}}},
      [&]() {
        // Assert Flush did not write changes to backend this->storage
        auto page = this->storage->Read(1);
        ASSERT_EQ(page->GetId(), this->page_1->GetId());
        ASSERT_EQ(static_cast<SimplePage *>(page.get())->GetRecord(),
                  this->page_1->GetRecord());
      });

  // Sequence of events which persist changes to page
  this->assertor.InsertMany(
      {
          // Sequence 1
          {{"thread-a", "GetPage", tstest::Event::Type::BEGIN},
           {"thread-a", "GetPage", tstest::Event::Type::END},
           {"thread-a", "SetRecord", tstest::Event::Type::BEGIN},
           {"thread-a", "SetRecord", tstest::Event::Type::END},
           {"thread-b", "Flush", tstest::Event::Type::BEGIN},
           {"thread-b", "Flush", tstest::Event::Type::END}}
          // Sequence 2
      },
      [&]() {
        // Assert Flush wrote changes to backend this->storage
        auto page = this->storage->Read(1);
        ASSERT_EQ(page->GetId(), this->page_1->GetId());
        ASSERT_EQ(static_cast<SimplePage *>(page.get())->GetRecord(),
                  "update_testing_1"_bb);
      });

  this->runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(), 1);
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            "update_testing_1"_bb);

  // Assert the observed sequence of events
  this->assertor.Assert(this->runner.GetEventLog());
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for different pages
 * when the internal buffer is empty.
 *
 * While one thread calls the `get` method on one page, the other calls `Flush`
 * on a different page. The operations should be done is such a way to avoid
 * data race in internal buffer.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIFlushJ) {
  // Assert buffer is empty
  ASSERT_TRUE(this->buffer_manager->IsEmpty());

  THREAD(this->runner, "thread-a") {
    OPERATION("GetPage", auto page = this->buffer_manager->Get<SimplePage>(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(this->runner, "thread-b") {
    OPERATION("Flush", this->buffer_manager->Flush(2));
  };

  this->runner.Run();

  // Assert Flush did not write changes to backend this->storage
  auto page = this->storage->Read(2);
  ASSERT_EQ(page->GetId(), this->page_2->GetId());
  ASSERT_EQ(static_cast<SimplePage *>(page.get())->GetRecord(),
            this->page_2->GetRecord());

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(), 1);
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            "update_testing_1"_bb);
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for different pages
 * when the internal buffer is full.
 *
 * While one thread calls the `get` method, loading a page by replacement from
 * backend this->storage, the other calls `Flush` on an already loaded page.
 * Since the two methods act on different pages, the only syncronization needed
 * is to avoid data race in internal buffer.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * this->storage.
 *
 */
TYPED_TEST_P(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIFlushJ) {
  // Filling up buffer
  this->buffer_manager->Get<SimplePage>(2);
  this->buffer_manager->Get<SimplePage>(3)->SetRecord(
      "update_testing_3"_bb); // Adding record in page 3
  ASSERT_TRUE(this->buffer_manager->IsFull());

  THREAD(this->runner, "thread-a") {
    OPERATION("GetPage", auto page = this->buffer_manager->Get<SimplePage>(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(this->runner, "thread-b") {
    OPERATION("Flush", this->buffer_manager->Flush(3));
  };

  this->runner.Run();

  // Assert Flush wrote changes to backend this->storage
  auto page = this->storage->Read(3);
  ASSERT_EQ(page->GetId(), this->page_3->GetId());
  ASSERT_EQ(static_cast<SimplePage *>(page.get())->GetRecord(),
            "update_testing_3"_bb);

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(this->buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetId(), 1);
  ASSERT_EQ(this->buffer_manager->Get<SimplePage>(1)->GetRecord(),
            "update_testing_1"_bb);
}

// Registering all tests
REGISTER_TYPED_TEST_SUITE_P(BufferManagerThreadSafetyTestFixture,
                            TestEmptyBufferGetIGetI, TestFullBufferGetIGetI,
                            TestEmptyBufferGetIGetJ, TestFullBufferGetIGetJ,
                            TestEmptyBufferGetIFlushI, TestFullBufferGetIFlushI,
                            TestEmptyBufferGetIFlushJ,
                            TestFullBufferGetIFlushJ);

} // namespace test
} // namespace persist

#endif /* PERSIST_TEST_THREADSAFETY_BUFFER_MANAGER_HPP */
