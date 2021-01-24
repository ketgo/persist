/**
 * test_fsl.cpp - Persist
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
 * Free space list unit tests.
 */

#include <gtest/gtest.h>

#include <list>
#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/fsl.hpp>

using namespace persist;

class FSLTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  std::unique_ptr<FSL> fsl;
  const std::set<PageId> freePages = {0, 1, 2, 3};

  void SetUp() override {
    fsl = std::make_unique<FSL>();
    fsl->freePages = freePages;
    input = {4, 0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0,  0,   0,   0,   0,
             1, 0, 0, 0, 0, 0, 0, 0, 2,   0,   0,   0,  0,   0,   0,   0,
             3, 0, 0, 0, 0, 0, 0, 0, 167, 212, 128, 26, 106, 183, 163, 0};
  }
};

TEST_F(FSLTestFixture, TestLoad) {
  FSL _fsl;
  _fsl.load(Span(input));

  ASSERT_EQ(_fsl.freePages, fsl->freePages);
}

TEST_F(FSLTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    FSL _fsl;
    _fsl.load(Span(_input));
    FAIL() << "Expected FSLParseError Exception.";
  } catch (FSLParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected FSLParseError Exception.";
  }
}

TEST_F(FSLTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input.back() = 10;
    FSL _fsl;
    _fsl.load(Span(_input));
    FAIL() << "Expected FSLCorruptError Exception.";
  } catch (FSLCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected FSLCorruptError Exception.";
  }
}

TEST_F(FSLTestFixture, TestLoadCorruptErrorInvalidFreePagesCount) {
  try {
    ByteBuffer _input = input;
    _input[16] = 9; //<- sets the free pages count located at 16th byte to 9
    FSL _fsl;
    _fsl.load(Span(_input));
    FAIL() << "Expected FSLCorruptError Exception.";
  } catch (FSLCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected FSLCorruptError Exception.";
  }
}

TEST_F(FSLTestFixture, TestDump) {
  ByteBuffer output(fsl->size());
  fsl->dump(Span(output));

  ASSERT_EQ(input, output);
}
