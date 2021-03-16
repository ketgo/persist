/**
 * test_lru_replacer.cpp - Persist
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
 * LRU Replacer Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/buffer/replacer/lru_replacer.hpp>

#include "persist/test/thread_safety/replacer.hpp"

using namespace persist;
using namespace persist::test;

class LRUReplacerTestFixture : public ::testing::Test {
protected:
  const PageId page_id = 1;
  std::unique_ptr<LRUReplacer> replacer;

  void SetUp() override {
    replacer = std::make_unique<LRUReplacer>();

    LRUReplacer::LockGuard guard(replacer->lock);

    replacer->cache.push_front({page_id, 0});
    replacer->position[page_id] = replacer->cache.begin();
  }
};

TEST_F(LRUReplacerTestFixture, TestTrack) {
  LRUReplacer::LockGuard guard(replacer->lock);

  ASSERT_EQ(replacer->cache.size(), 1);
  ASSERT_EQ(replacer->position.size(), 1);

  replacer->Track(2);

  ASSERT_EQ(replacer->cache.size(), 2);
  ASSERT_EQ(replacer->position.size(), 2);
}

TEST_F(LRUReplacerTestFixture, TestForget) {
  LRUReplacer::LockGuard guard(replacer->lock);

  ASSERT_EQ(replacer->cache.size(), 1);
  ASSERT_EQ(replacer->position.size(), 1);

  replacer->Forget(page_id);

  ASSERT_EQ(replacer->cache.size(), 0);
  ASSERT_EQ(replacer->position.size(), 0);
}

TEST_F(LRUReplacerTestFixture, TestPin) {
  LRUReplacer::LockGuard guard(replacer->lock);

  replacer->Track(2);

  ASSERT_EQ(replacer->position[2]->pin_count, 0);
  replacer->Pin(2);
  ASSERT_EQ(replacer->position[2]->pin_count, 1);
  replacer->Pin(2);
  ASSERT_EQ(replacer->position[2]->pin_count, 2);
}

TEST_F(LRUReplacerTestFixture, TestIsPinned) {
  LRUReplacer::LockGuard guard(replacer->lock);

  replacer->Track(2);

  ASSERT_EQ(replacer->position[2]->pin_count, 0);
  replacer->Pin(2);
  ASSERT_EQ(replacer->position[2]->pin_count, 1);
  ASSERT_TRUE(replacer->IsPinned(2));
  replacer->Unpin(2);
  ASSERT_TRUE(!replacer->IsPinned(2));
}

TEST_F(LRUReplacerTestFixture, TestUnPin) {
  LRUReplacer::LockGuard guard(replacer->lock);

  replacer->Track(2);
  replacer->Pin(2);
  replacer->Pin(2);

  ASSERT_EQ(replacer->position[2]->pin_count, 2);
  replacer->Unpin(2);
  ASSERT_EQ(replacer->position[2]->pin_count, 1);
  replacer->Unpin(2);
  ASSERT_EQ(replacer->position[2]->pin_count, 0);
}

TEST_F(LRUReplacerTestFixture, TestGetVictum) {
  LRUReplacer::LockGuard guard(replacer->lock);

  ASSERT_EQ(replacer->GetVictumId(), 1);

  replacer->Track(2);
  replacer->Track(3);

  replacer->Pin(1);
  replacer->Pin(2);
  replacer->Unpin(2);
  replacer->Pin(3);
  replacer->Unpin(3);

  // Page ID 1 is pinned so the next LRU un-pinned ID should be returned, i.e. 2
  ASSERT_EQ(replacer->GetVictumId(), 2);
}

/**
 * @brief LRU Replacer thread safety tests.
 *
 */
INSTANTIATE_TYPED_TEST_SUITE_P(LRU, ReplacerThreadSafetyTestFixture,
                               LRUReplacer);
