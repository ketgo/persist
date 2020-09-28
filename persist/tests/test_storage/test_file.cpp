/**
 * test_file.cpp - Persist
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
 * File Storage Unit Tests
 */

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <persist/block.hpp>
#include <persist/storage/file.hpp>
#include <persist/utility.hpp>

using namespace persist;

class NewFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string base = "persist/tests/data";
  const std::string path = base + "/test.storage";
  const uint64_t blockSize = DEFAULT_DATA_BLOCK_SIZE;
  std::unique_ptr<FileStorage> storage;

  void SetUp() override {
    storage = std::make_unique<FileStorage>(path, blockSize);
  }
};

TEST_F(NewFileStorageTestFixture, TestReadBlock) {}

TEST_F(NewFileStorageTestFixture, TestWriteBlock) {}

TEST_F(NewFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<Storage::MetaData> metadata = storage->read();
}

TEST_F(NewFileStorageTestFixture, TestReadNonExistingFileMetaData) {}

TEST_F(NewFileStorageTestFixture, TestWriteMetaData) {}

class ExistingFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string base = "persist/tests/data";
  const std::string readPath = base + "/test_read.storage";
  const std::string writePath = base + "/test_write.storage";
  const uint64_t blockSize = DEFAULT_DATA_BLOCK_SIZE;
  std::unique_ptr<FileStorage> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage>(readPath, blockSize);
    writeStorage = std::make_unique<FileStorage>(writePath, blockSize);
  }
};

TEST_F(ExistingFileStorageTestFixture, TestReadBlock) {}

TEST_F(ExistingFileStorageTestFixture, TestWriteBlock) {}

TEST_F(ExistingFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<Storage::MetaData> metadata = readStorage->read();
  Storage::MetaData _metadata;
  _metadata.blockSize = blockSize;
  _metadata.freeBlocks = {0, 1, 2};

  ASSERT_EQ(metadata->blockSize, _metadata.blockSize);
  ASSERT_EQ(metadata->freeBlocks, _metadata.freeBlocks);
}

TEST_F(ExistingFileStorageTestFixture, TestWriteMetaData) {
  Storage::MetaData metadata;
  metadata.blockSize = DEFAULT_DATA_BLOCK_SIZE;
  metadata.freeBlocks = {1, 2, 3};

  writeStorage->write(metadata);

  // Read written file
  std::fstream file = file::open(writePath + ".metadata",
                                 std::fstream::in | std::fstream::binary);
  uint64_t fileSize = file::size(file);
  std::vector<uint8_t> buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  Storage::MetaData _metadata;
  _metadata.load(buffer);

  ASSERT_EQ(metadata.freeBlocks, _metadata.freeBlocks);
  ASSERT_EQ(metadata.blockSize, _metadata.blockSize);
}
