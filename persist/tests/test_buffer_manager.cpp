/**
 * test_manager.cpp - Persist
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
 * Block Manager Cache Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <persist/buffer_manager.hpp>
#include <persist/exceptions.hpp>

using namespace persist;

class BufferManagerTestFixture : public ::testing::Test {
protected:
  std::unique_ptr<BufferManager> manager;

  void SetUp() override {}
};

TEST_F(BufferManagerTestFixture, TestGetFreeSpaceDataBlock) {}

TEST_F(BufferManagerTestFixture, TestGetNewDataBlock) {}

TEST_F(BufferManagerTestFixture, TestGetDataBlockWithId) {}

TEST_F(BufferManagerTestFixture, TestGetDataBlockWithIdError) {}

TEST_F(BufferManagerTestFixture, Testflush) {}
