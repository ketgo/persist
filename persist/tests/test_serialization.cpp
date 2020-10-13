/**
 * test_serialization.cpp - Persist
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

#include <gtest/gtest.h>

#include <persist/core/common.hpp>
#include <persist/core/page.hpp>
#include <persist/core/record_block.hpp>

using namespace persist;

TEST(TestRecordBlockSerialization, TestDump) {
  RecordBlock::Header header;
  header.nextLocation.pageId = 21;
  header.nextLocation.slotId = 10;
  header.prevLocation.pageId = 7;
  header.prevLocation.slotId = 42;

  ByteBuffer buffer(sizeof(RecordBlock::Header));
  std::memcpy(buffer.data(), &header, sizeof(header));

  for (int c : buffer) {
    std::cout << c << ",";
  }
  std::cout << "\n";
}

TEST(TestRecordBlockSerialization, TestLoad) {
  ByteBuffer buffer = {192, 8, 34, 3, 1, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0,
                       0,   0, 0,  0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0,
                       21,  0, 0,  0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0,
                       7,   0, 0,  0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0};

  RecordBlock::Header header;

  std::memcpy(&header, buffer.data(), sizeof(header));

  std::cout << "N[" << header.nextLocation.pageId << ", "
            << header.nextLocation.slotId << "] P["
            << header.prevLocation.pageId << ", " << header.prevLocation.slotId
            << "]\n";
}