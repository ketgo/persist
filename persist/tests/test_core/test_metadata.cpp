/**
 * test_metadata.cpp - Persist
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
 * Collection Metadata Unit Tests
 */

#include <gtest/gtest.h>

#include <persist/core/metadata.hpp>

using namespace persist;

class MetadataTestFixture : public ::testing::Test {
protected:
  const size_t count = 5;
  const RecordLocation first = {1, 1}, last = {10, 1};
  Metadata metadata;
  ByteBuffer input;

  void SetUp() override {
    // Setup metadata
    metadata.count = count;
    metadata.first = first;
    metadata.last = last;

    input = {5, 0, 0, 0, 0,  0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
             0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};
  }
};

TEST_F(MetadataTestFixture, TestLoad) {
  Metadata _metadata;
  _metadata.Load(input);

  ASSERT_EQ(_metadata, metadata);
}

TEST_F(MetadataTestFixture, TestLoadParseError) {
  ByteBuffer _input;
  Metadata _metadata;

  ASSERT_THROW(_metadata.Load(_input), MetadataParseError);
}

TEST_F(MetadataTestFixture, TestDump) {
  ByteBuffer output(metadata.GetStorageSize());
  metadata.Dump(output);

  ASSERT_EQ(input, output);
}
