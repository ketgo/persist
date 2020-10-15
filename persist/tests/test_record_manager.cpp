/**
 * test_record_manager.cpp - Persist
 *
 * Copyright 2020 Ketan Goyal
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
 * @brief Record Manager Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_TESTING

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/record_manager.hpp>

using namespace persist;

class RecordManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  std::unique_ptr<RecordManager> manager;

  void SetUp() override {
    // TODO: Add page size to query when supported
    manager = std::make_unique<RecordManager>("memory://", maxSize);
    manager->start();
  }

  void TearDown() override { manager->stop(); }
};

TEST_F(RecordManagerTestFixture, TestGet) {}

TEST_F(RecordManagerTestFixture, TestInsert) {
  ByteBuffer input(2 * pageSize + 100, 'A');
  RecordBlock::Location location = manager->insert(input);

  ByteBuffer output;
  manager->get(output, location);

  ASSERT_EQ(output.size(), input.size());
  ASSERT_EQ(output, input);
}
