/**
 * test_log_record.cpp - Persist
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
 * Log Record Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/recovery/log_record.hpp>

using namespace persist;

class LogRecordTestFixture : public ::testing::Test {
protected:
  PageSlot pageSlotA, pageSlotB;
  const TransactionId txnId = 432;
  const PageSlot::Location location = {10, 1};
  const SeqNumber seqNumber = 5;
  const LogRecord::Location prevLogRecordLocation = {1, 3};
  std::unique_ptr<LogRecord> logRecord;
  ByteBuffer input;

  void SetUp() override {
    pageSlotA.data = "testing-A"_bb;
    pageSlotB.data = "testing-B"_bb;
    logRecord = std::make_unique<LogRecord>(txnId, prevLogRecordLocation,
                                            LogRecord::Type::UPDATE, location,
                                            pageSlotA, pageSlotB);
    logRecord->header.seqNumber = seqNumber;

    input = {5,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   0,
             0,   0,   0,   3,   0,   0,   0,   0,   0,   0,   0,   176, 1,
             0,   0,   0,   0,   0,   0,   238, 51,  31,  30,  100, 49,  129,
             95,  2,   0,   0,   0,   10,  0,   0,   0,   0,   0,   0,   0,
             1,   0,   0,   0,   0,   0,   0,   0,   49,  0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   195, 164, 223, 200,
             40,  173, 239, 136, 116, 101, 115, 116, 105, 110, 103, 45,  65,
             49,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
             0,   194, 164, 223, 200, 40,  173, 239, 136, 116, 101, 115, 116,
             105, 110, 103, 45,  66};
  }
};

TEST_F(LogRecordTestFixture, TestLoad) {
  LogRecord _logRecord;
  _logRecord.load(Span(input));

  ASSERT_EQ(_logRecord, *logRecord);
}

TEST_F(LogRecordTestFixture, TestLoadParseError) {
  try {
    ByteBuffer _input;
    LogRecord _logRecord;
    _logRecord.load(Span(_input));
    FAIL() << "Expected LogRecordParseError Exception.";
  } catch (LogRecordParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected LogRecordParseError Exception.";
  }
}

TEST_F(LogRecordTestFixture, TestLoadCorruptError) {
  try {
    ByteBuffer _input = input;
    _input.front() = 0;
    LogRecord _logRecord;
    _logRecord.load(Span(_input));
    FAIL() << "Expected LogRecordCorruptError Exception.";
  } catch (LogRecordCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected LogRecordCorruptError Exception.";
  }
}

TEST_F(LogRecordTestFixture, TestDump) {
  ByteBuffer output(logRecord->size());

  logRecord->dump(Span(output));

  ASSERT_EQ(input, output);
}
