/**
 * test_record_block.cpp - Persist
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
 * Record Block Unit Tests
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include <persist/block.hpp>
#include <persist/exceptions.hpp>

using namespace persist;

class RecordBlockTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  const RecordBlockId recordBlockId = 10;
  const DataBlockId nextDataBlockId = 12;
  const DataBlockId prevDataBlockId = 1;
  const std::string data = "testing";
  std::unique_ptr<RecordBlock> block;

  void SetUp() override {
    RecordBlock::Header header(recordBlockId);
    header.nextDataBlockId = nextDataBlockId;
    header.prevDataBlockId = prevDataBlockId;
    block = std::make_unique<RecordBlock>(header);
    block->data = data;

    input = {123, 105, 7,   98,  108, 111, 99,  107, 73,  100, 105, 10,  105,
             11,  110, 101, 120, 116, 66,  108, 111, 99,  107, 73,  100, 105,
             12,  105, 11,  112, 114, 101, 118, 66,  108, 111, 99,  107, 73,
             100, 105, 1,   125, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(RecordBlockTestFixture, TestGetId) {
  ASSERT_EQ(block->getId(), recordBlockId);
}

TEST_F(RecordBlockTestFixture, TestGetNextBlockId) {
  ASSERT_EQ(block->getNextDataBlockId(), nextDataBlockId);
}

TEST_F(RecordBlockTestFixture, TestSetNextBlockId) {
  DataBlockId blockId = 99;
  block->setNextDataBlockId(blockId);
  ASSERT_EQ(block->getNextDataBlockId(), blockId);
}

TEST_F(RecordBlockTestFixture, TestGetPrevBlockId) {
  ASSERT_EQ(block->getPrevDataBlockId(), prevDataBlockId);
}

TEST_F(RecordBlockTestFixture, TestSetPrevBlockId) {
  DataBlockId blockId = 99;
  block->setPrevDataBlockId(blockId);
  ASSERT_EQ(block->getPrevDataBlockId(), blockId);
}

TEST_F(RecordBlockTestFixture, TestLoad) {
  RecordBlock _block;
  _block.load(input);

  ASSERT_EQ(_block.data, block->data);
  ASSERT_EQ(_block.getId(), block->getId());
  ASSERT_EQ(_block.getNextDataBlockId(), block->getNextDataBlockId());
  ASSERT_EQ(_block.getPrevDataBlockId(), block->getPrevDataBlockId());
}

TEST_F(RecordBlockTestFixture, TestLoadError) {
  try {
    std::vector<uint8_t> _input;
    RecordBlock _block;
    _block.load(_input);
    FAIL() << "Expected RecordBlockParseError Exception.";
  } catch (RecordBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockParseError Exception.";
  }
}

TEST_F(RecordBlockTestFixture, TestDump) {
  std::vector<uint8_t> output;
  block->dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(RecordBlockTestFixture, TestSize) {
  std::vector<uint8_t> output;
  block->dump(output);

  ASSERT_EQ(block->size(), output.size());
}
