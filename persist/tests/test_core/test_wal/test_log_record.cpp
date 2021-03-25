/**
 * test_log_record.cpp - Persist
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
 * Log Record Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/wal/log_record.hpp>

using namespace persist;

class LogRecordTestFixture : public ::testing::Test {
protected:
  RecordPageSlot page_slot_a, page_slot_b;
  const TransactionId txn_id = 432;
  const RecordPageSlot::Location location = {10, 1};
  const SeqNumber seq_number = 5;
  const LogRecord::Location prev_log_record_location = {1, 3};
  std::unique_ptr<LogRecord> log_record;
  ByteBuffer input;

  void SetUp() override {
    page_slot_a.data = "testing-A"_bb;
    page_slot_b.data = "testing-B"_bb;
    log_record = std::make_unique<LogRecord>(txn_id, prev_log_record_location,
                                             LogRecord::Type::UPDATE, location,
                                             page_slot_a, page_slot_b);
    log_record->SetSeqNumber(seq_number);

    input = {5,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,  0,  0, 0, 0,
             0,   3,   0,   0,   0,   0,   0,   0,   0,   176, 1,  0,  0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   2,  0,  0, 0, 10,
             0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,  0,  0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   9,  0,  0, 0, 0,
             0,   0,   0,   116, 101, 115, 116, 105, 110, 103, 45, 65, 0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0,
             0,   0,   0,   0,   0,   0,   0,   9,   0,   0,   0,  0,  0, 0, 0,
             116, 101, 115, 116, 105, 110, 103, 45,  66};
  }
};

TEST_F(LogRecordTestFixture, TestLoad) {
  LogRecord _log_record;
  _log_record.Load(input);

  ASSERT_EQ(_log_record, *log_record);
}

TEST_F(LogRecordTestFixture, TestLoadParseError) {
  ByteBuffer _input;
  LogRecord _log_record;

  ASSERT_THROW(_log_record.Load(_input), LogRecordParseError);
}

TEST_F(LogRecordTestFixture, TestDump) {
  ByteBuffer output(log_record->GetStorageSize());
  log_record->Dump(output);

  ASSERT_EQ(input, output);
}
