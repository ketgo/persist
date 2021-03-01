/**
 * test_serializer.cpp - Persist
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
 * @brief Uint Test Serializer
 *
 */

#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include <persist/utility/serializer.hpp>

using namespace persist;

class UtilitySerializerTestFixture : public ::testing::Test {
protected:
  void SetUp() override {}
};

TEST_F(UtilitySerializerTestFixture, TestLoad) {}

TEST_F(UtilitySerializerTestFixture, TestDump) {
  ByteBuffer output(sizeof(uint64_t) + sizeof(char));
  uint64_t number = 32146;
  char c = 'r';
  dump(output, number, c);

  for (int i : output) {
    std::cout << i << ", ";
  }
  std::cout << "\n";
}
