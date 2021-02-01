/**
 * recovery/test_log_manager_2.cpp - Persist
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
 * @brief LogManager unit test
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/recovery/log_manager_2.hpp>
#include <persist/core/storage/factory.hpp>

using namespace persist;

class LogManager2TestFixture : public ::testing::Test {
protected:
  const SeqNumber startSeqNumber = 1;
  const uint64_t maxSize = 2;
  const std::string path = "test_log_manager";
  std::unique_ptr<LogManager2> logManager;
  std::unique_ptr<Storage<LogPage>> storage;

  void SetUp() override {
    // setting up storage
    storage = createStorage<LogPage>("file://" + path);

    // Setup log manager
    logManager = std::make_unique<LogManager2>(storage.get(), maxSize);
    logManager->start();
  }

  void TearDown() override {
    storage->remove();
    logManager->stop();
  }
};

TEST_F(LogManager2TestFixture, TestAddAndGet) {
  LogRecord logRecord;
  logRecord.header.seqNumber = startSeqNumber;
  LogRecordLocation location = logManager->add(logRecord);
  ASSERT_EQ(*(logManager->get(location)), logRecord);
}

TEST_F(LogManager2TestFixture, TestGetSeqNumber) {
  ASSERT_EQ(logManager->seqNumber, 0);
}
