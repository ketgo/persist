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
#include <memory>
#include <string>
#include <vector>

#include <utility.hpp>

#include <persist/data_block.hpp>
#include <persist/exceptions.hpp>
#include <persist/storage/file_storage.hpp>

using namespace persist;

const std::string base = "persist/tests/data";

/********************************
 * Testing for Next Storage
 ********************************/

class NewFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/_read.storage";
  const std::string writePath = base + "/_write.storage";
  const uint64_t blockSize = 512;
  std::unique_ptr<FileStorage> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage>(readPath, blockSize);
    writeStorage = std::make_unique<FileStorage>(writePath, blockSize);
  }
};

TEST_F(NewFileStorageTestFixture, TestReadBlock) {
  try {
    std::unique_ptr<DataBlock> dataBlock = readStorage->read(1);
    FAIL() << "Expected DataBlockNotFoundError Exception.";
  } catch (DataBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected DataBlockNotFoundError Exception.";
  }
}

TEST_F(NewFileStorageTestFixture, TestWriteBlock) {
  RecordBlock recordBlock(1);
  recordBlock.data = "testing";

  DataBlock dataBlock(1, blockSize);
  dataBlock.addRecordBlock(recordBlock);
  writeStorage->write(dataBlock);

  std::fstream file = file::open(writePath, std::ios::in | std::ios::binary);
  ByteBuffer buffer(blockSize);
  file::read(file, buffer, 0);
  DataBlock _dataBlock;
  _dataBlock.load(buffer);
  RecordBlock &_recordBlock = dataBlock.getRecordBlock(1);

  ASSERT_EQ(dataBlock.getId(), _dataBlock.getId());
  ASSERT_EQ(recordBlock.getId(), _recordBlock.getId());
  ASSERT_EQ(recordBlock.data, _recordBlock.data);
}

TEST_F(NewFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<Storage::MetaData> metadata = readStorage->read();

  ASSERT_EQ(metadata->blockSize, blockSize);
  ASSERT_EQ(metadata->freeBlocks.size(), 0);
}

TEST_F(NewFileStorageTestFixture, TestWriteMetaData) {
  Storage::MetaData metadata;
  metadata.blockSize = blockSize;
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

/********************************
 * Testing for Existing Storage
 ********************************/

class ExistingFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/test_read.storage";
  const std::string writePath = base + "/test_write.storage";
  const uint64_t blockSize = 512;
  std::unique_ptr<FileStorage> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage>(readPath, blockSize);
    writeStorage = std::make_unique<FileStorage>(writePath, blockSize);
  }
};

TEST_F(ExistingFileStorageTestFixture, TestReadBlock) {
  std::unique_ptr<DataBlock> dataBlock = readStorage->read(1);
  RecordBlock &recordBlock = dataBlock->getRecordBlock(1);

  ASSERT_EQ(dataBlock->getId(), 1);
  ASSERT_EQ(recordBlock.getId(), 1);
  ASSERT_EQ(recordBlock.data, "testing");
}

TEST_F(ExistingFileStorageTestFixture, TestWriteBlock) {
  RecordBlock recordBlock(1);
  recordBlock.data = "testing";

  DataBlock dataBlock(1, blockSize);
  dataBlock.addRecordBlock(recordBlock);
  writeStorage->write(dataBlock);

  std::fstream file = file::open(writePath, std::ios::in | std::ios::binary);
  ByteBuffer buffer(blockSize);
  file::read(file, buffer, 0);
  DataBlock _dataBlock;
  _dataBlock.load(buffer);
  RecordBlock &_recordBlock = dataBlock.getRecordBlock(1);

  ASSERT_EQ(dataBlock.getId(), _dataBlock.getId());
  ASSERT_EQ(recordBlock.getId(), _recordBlock.getId());
  ASSERT_EQ(recordBlock.data, _recordBlock.data);
}

TEST_F(ExistingFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<Storage::MetaData> metadata = readStorage->read();
  Storage::MetaData _metadata;
  _metadata.blockSize = 1024; // Block size in saved metadata
  _metadata.freeBlocks = {0, 1, 2};

  ASSERT_EQ(metadata->blockSize, _metadata.blockSize);
  ASSERT_EQ(metadata->freeBlocks, _metadata.freeBlocks);
}

TEST_F(ExistingFileStorageTestFixture, TestWriteMetaData) {
  Storage::MetaData metadata;
  metadata.blockSize = 1024;
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
