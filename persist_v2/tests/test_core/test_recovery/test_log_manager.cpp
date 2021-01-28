/**
 * test_log_manager.cpp - Persist
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
 * Data Block Unit Tests
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/recovery/log_manager.hpp>

using namespace persist;

class LogManagerTestFixture : public ::testing::Test {
protected:
  const SeqNumber startSeqNumber = 1;
  std::unique_ptr<LogManager> logManager;

  void SetUp() override {
    // Setup log manager
    logManager = std::make_unique<LogManager>();
  }
};

TEST_F(LogManagerTestFixture, TestAddAndGet) {
  LogRecord logRecord;
  logRecord.header.seqNumber = startSeqNumber;
  logManager->add(logRecord);
  ASSERT_EQ(logManager->get(startSeqNumber), logRecord);
}

TEST_F(LogManagerTestFixture, TestGetSeqNumber) {
  ASSERT_EQ(logManager->getSeqNumber(), 0);
}
