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

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/page/creator.hpp>
#include <persist/core/storage/creator.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class BufferManagerThreadSafetyTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const std::string path = "test_buffer_manager_ts";
  std::unique_ptr<SimplePage> page_1, page_2, page_3;
  std::unique_ptr<BufferManager<SimplePage>> buffer_manager;
  std::unique_ptr<Storage<SimplePage>> storage;

  // TSTest runner
  tstest::Runner runner;
  // TSTest assertor
  tstest::Assertor assertor;

  void SetUp() override {
    // Setting up pages
    ByteBuffer buffer;
    page_1 = persist::CreatePage<SimplePage>(1, page_size);
    page_1->SetRecord("test_page_1"_bb);
    page_2 = persist::CreatePage<SimplePage>(2, page_size);
    page_2->SetRecord("test_page_2"_bb);
    page_3 = persist::CreatePage<SimplePage>(3, page_size);
    page_3->SetRecord("test_page_3"_bb);

    // setting up storage
    storage = persist::CreateStorage<SimplePage>("file://" + path);
    insert();

    buffer_manager =
        std::make_unique<BufferManager<SimplePage>>(*storage, max_size);
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
  void insert() {
    storage->Open();
    storage->Write(*page_1);
    storage->Write(*page_2);
    storage->Write(*page_3);
    storage->Close();
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
  ASSERT_TRUE(buffer_manager->IsEmpty());

  THREAD(runner, "thread-a") {
    auto page = buffer_manager->Get(1);

    ASSERT_EQ(page->GetId(), page_1->GetId());
    ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
  };

  THREAD(runner, "thread-b") {
    auto page = buffer_manager->Get(1);

    ASSERT_EQ(page->GetId(), page_1->GetId());
    ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
  };

  runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), page_1->GetId());
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), page_1->GetRecord());
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
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids data race in the FSM, the backend storage, and the internal
 * buffer.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIGetI) {
  // Filling up buffer
  buffer_manager->Get(2);
  buffer_manager->Get(3);
  ASSERT_TRUE(buffer_manager->IsFull());

  THREAD(runner, "thread-a") {
    auto page = buffer_manager->Get(1);

    ASSERT_EQ(page->GetId(), page_1->GetId());
    ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
  };

  THREAD(runner, "thread-b") {
    auto page = buffer_manager->Get(1);

    ASSERT_EQ(page->GetId(), page_1->GetId());
    ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
  };

  runner.Run();

  // Assert the page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), page_1->GetId());
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), page_1->GetRecord());

  // Assert only one victim page got replaced
  ASSERT_TRUE(buffer_manager->IsFull());
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
  ASSERT_TRUE(buffer_manager->IsEmpty());

  THREAD(runner, "thread-a") {
    auto page = buffer_manager->Get(1);

    ASSERT_EQ(page->GetId(), page_1->GetId());
    ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
  };

  THREAD(runner, "thread-b") {
    auto page = buffer_manager->Get(2);

    ASSERT_EQ(page->GetId(), page_2->GetId());
    ASSERT_EQ(page->GetRecord(), page_2->GetRecord());
  };

  runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), page_1->GetId());
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), page_1->GetRecord());
  // Assert second page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(2));
  ASSERT_EQ(buffer_manager->Get(2)->GetId(), page_2->GetId());
  ASSERT_EQ(buffer_manager->Get(2)->GetRecord(), page_2->GetRecord());
}

/**
 * @brief Test concurrent call to `get` method for different pages when the
 * internal buffer is full.
 *
 * Both threads should load the pages with replacement from the backend storage.
 * The replacement of page involves finding a victim page from FSM, then
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * storage.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIGetJ) {
  // Filling up buffer
  buffer_manager->Get(2);
  buffer_manager->Get(3);
  ASSERT_TRUE(buffer_manager->IsFull());

  THREAD(runner, "thread-a") {
    auto page = buffer_manager->Get(1);

    ASSERT_EQ(page->GetId(), page_1->GetId());
    ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
  };

  THREAD(runner, "thread-b") {
    auto page = buffer_manager->Get(2);

    ASSERT_EQ(page->GetId(), page_2->GetId());
    ASSERT_EQ(page->GetRecord(), page_2->GetRecord());
  };

  runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), page_1->GetId());
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), page_1->GetRecord());
  // Assert second page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(2));
  ASSERT_EQ(buffer_manager->Get(2)->GetId(), page_2->GetId());
  ASSERT_EQ(buffer_manager->Get(2)->GetRecord(), page_2->GetRecord());
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for same page when
 * the internal buffer is empty.
 *
 * While one thread calls the `get` method, the other calls `Flush`. Since the
 * `get` call pins the page, the Flush call should have no effect.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIFlushI) {
  // Assert buffer is empty
  ASSERT_TRUE(buffer_manager->IsEmpty());

  THREAD(runner, "thread-a") {
    OPERATION("GetPage", auto page = buffer_manager->Get(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(runner, "thread-b") { OPERATION("Flush", buffer_manager->Flush(1)); };

  // Sequence of events which do not persist any changes to page
  assertor.InsertMany(
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
        // Assert Flush did not write changes to backend storage
        auto page = storage->Read(1);
        ASSERT_EQ(page->GetId(), page_1->GetId());
        ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
      });

  // Sequence of events which persist changes to page
  assertor.InsertMany(
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
        // Assert Flush wrote changes to backend storage
        auto page = storage->Read(1);
        ASSERT_EQ(page->GetId(), page_1->GetId());
        ASSERT_EQ(page->GetRecord(), "update_testing_1"_bb);
      });

  runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), 1);
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), "update_testing_1"_bb);

  // Assert the observed sequence of events
  assertor.Assert(runner.GetEventLog());
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for same page when
 * the internal buffer is full.
 *
 * While one thread calls the `get` method, loading a page by replacement from
 * backend storage, the other calls `Flush`. Since the `get` call pins the page,
 * the Flush call should have no effect.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * storage.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIFlushI) {
  // Filling up buffer
  buffer_manager->Get(2);
  buffer_manager->Get(3);
  ASSERT_TRUE(buffer_manager->IsFull());

  THREAD(runner, "thread-a") {
    OPERATION("GetPage", auto page = buffer_manager->Get(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(runner, "thread-b") { OPERATION("Flush", buffer_manager->Flush(1)); };

  // Sequence of events which do not persist any changes to page
  assertor.InsertMany(
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
        // Assert Flush did not write changes to backend storage
        auto page = storage->Read(1);
        ASSERT_EQ(page->GetId(), page_1->GetId());
        ASSERT_EQ(page->GetRecord(), page_1->GetRecord());
      });

  // Sequence of events which persist changes to page
  assertor.InsertMany(
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
        // Assert Flush wrote changes to backend storage
        auto page = storage->Read(1);
        ASSERT_EQ(page->GetId(), page_1->GetId());
        ASSERT_EQ(page->GetRecord(), "update_testing_1"_bb);
      });

  runner.Run();

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), 1);
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), "update_testing_1"_bb);

  // Assert the observed sequence of events
  assertor.Assert(runner.GetEventLog());
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
TEST_F(BufferManagerThreadSafetyTestFixture, TestEmptyBufferGetIFlushJ) {
  // Assert buffer is empty
  ASSERT_TRUE(buffer_manager->IsEmpty());

  THREAD(runner, "thread-a") {
    OPERATION("GetPage", auto page = buffer_manager->Get(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(runner, "thread-b") { OPERATION("Flush", buffer_manager->Flush(2)); };

  runner.Run();

  // Assert Flush did not write changes to backend storage
  auto page = storage->Read(2);
  ASSERT_EQ(page->GetId(), page_2->GetId());
  ASSERT_EQ(page->GetRecord(), page_2->GetRecord());

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), 1);
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), "update_testing_1"_bb);
}

/**
 * @brief Test concurrent call to `get` and `Flush` methods for different pages
 * when the internal buffer is full.
 *
 * While one thread calls the `get` method, loading a page by replacement from
 * backend storage, the other calls `Flush` on an already loaded page. Since the
 * two methods act on different pages, the only syncronization needed is to
 * avoid data race in internal buffer.
 *
 * The replacement of page involves finding a victim page from FSM, then
 * Flushing and removing it from the internal buffer. Thus it should be done in
 * a way that avoids a data race in the internal buffer, FSM and the backend
 * storage.
 *
 */
TEST_F(BufferManagerThreadSafetyTestFixture, TestFullBufferGetIFlushJ) {
  // Filling up buffer
  buffer_manager->Get(2);
  buffer_manager->Get(3)->SetRecord(
      "update_testing_3"_bb); // Adding record in page 3
  ASSERT_TRUE(buffer_manager->IsFull());

  THREAD(runner, "thread-a") {
    OPERATION("GetPage", auto page = buffer_manager->Get(1));
    OPERATION("SetRecord", page->SetRecord("update_testing_1"_bb));
  };

  THREAD(runner, "thread-b") { OPERATION("Flush", buffer_manager->Flush(3)); };

  runner.Run();

  // Assert Flush wrote changes to backend storage
  auto page = storage->Read(3);
  ASSERT_EQ(page->GetId(), page_3->GetId());
  ASSERT_EQ(page->GetRecord(), "update_testing_3"_bb);

  // Assert first page is loaded and did not get corrupt due to any data race
  ASSERT_TRUE(buffer_manager->IsPageLoaded(1));
  ASSERT_EQ(buffer_manager->Get(1)->GetId(), 1);
  ASSERT_EQ(buffer_manager->Get(1)->GetRecord(), "update_testing_1"_bb);
}
