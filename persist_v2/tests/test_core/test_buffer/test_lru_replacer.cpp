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

using namespace persist;

class LRUReplacerTestFixture : public ::testing::Test {
protected:
  const PageId pageId = 1;
  std::unique_ptr<LRUReplacer> replacer;

  void SetUp() override {
    replacer = std::make_unique<LRUReplacer>();
    replacer->cache.push_front({pageId, 0});
    replacer->position[pageId] = replacer->cache.begin();
  }
};

TEST_F(LRUReplacerTestFixture, TestTrack) {
  ASSERT_EQ(replacer->cache.size(), 1);
  ASSERT_EQ(replacer->position.size(), 1);

  replacer->track(2);

  ASSERT_EQ(replacer->cache.size(), 2);
  ASSERT_EQ(replacer->position.size(), 2);
}

TEST_F(LRUReplacerTestFixture, TestForget) {
  ASSERT_EQ(replacer->cache.size(), 1);
  ASSERT_EQ(replacer->position.size(), 1);

  replacer->forget(pageId);

  ASSERT_EQ(replacer->cache.size(), 0);
  ASSERT_EQ(replacer->position.size(), 0);
}

TEST_F(LRUReplacerTestFixture, TestPin) {
  replacer->track(2);

  ASSERT_EQ(replacer->position[2]->pinCount, 0);
  replacer->pin(2);
  ASSERT_EQ(replacer->position[2]->pinCount, 1);
  replacer->pin(2);
  ASSERT_EQ(replacer->position[2]->pinCount, 2);
}

TEST_F(LRUReplacerTestFixture, TestUnPin) {
  replacer->track(2);
  replacer->pin(2);
  replacer->pin(2);

  ASSERT_EQ(replacer->position[2]->pinCount, 2);
  replacer->unpin(2);
  ASSERT_EQ(replacer->position[2]->pinCount, 1);
  replacer->unpin(2);
  ASSERT_EQ(replacer->position[2]->pinCount, 0);
}

TEST_F(LRUReplacerTestFixture, TestGetVictum) {
  ASSERT_EQ(replacer->getVictumId(), 1);

  replacer->track(2);
  replacer->track(3);

  replacer->pin(1);
  replacer->pin(2);
  replacer->unpin(2);
  replacer->pin(3);
  replacer->unpin(3);

  // Page ID 1 is pinned so the next LRU un-pinned ID should be returned, i.e. 2
  ASSERT_EQ(replacer->getVictumId(), 2);
}