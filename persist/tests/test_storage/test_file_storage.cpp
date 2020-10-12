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

#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>
#include <persist/core/storage/file_storage.hpp>

using namespace persist;

const std::string base = "persist/tests/data";

/********************************
 * Testing for New Storage
 ********************************/

class NewFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/_read.storage";
  const std::string writePath = base + "/_write.storage";
  const uint64_t pageSize = 512;
  std::unique_ptr<FileStorage> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage>(readPath, pageSize);
    readStorage->open();
    writeStorage = std::make_unique<FileStorage>(writePath, pageSize);
    writeStorage->open();
  }

  void TearDown() override {
    readStorage->close();
    writeStorage->close();
  }
};

TEST_F(NewFileStorageTestFixture, TestReadBlock) {
  try {
    std::unique_ptr<Page> page = readStorage->read(1);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(NewFileStorageTestFixture, TestWriteBlock) {
  RecordBlock recordBlock;
  recordBlock.data = {'t', 'e', 's', 't', 'i', 'n', 'g'};

  Page page(1, pageSize);
  PageSlotId slotId = page.addRecordBlock(recordBlock);
  writeStorage->write(page);

  std::fstream file = file::open(writePath, std::ios::in | std::ios::binary);
  ByteBuffer buffer(pageSize);
  file::read(file, buffer, 0);
  Page _dataBlock;
  _dataBlock.load(buffer);
  RecordBlock &_recordBlock = page.getRecordBlock(slotId);

  ASSERT_EQ(page.getId(), _dataBlock.getId());
  ASSERT_EQ(recordBlock.data, _recordBlock.data);
}

TEST_F(NewFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<MetaData> metadata = readStorage->read();

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, 0);
  ASSERT_EQ(metadata->freePages.size(), 0);
}

TEST_F(NewFileStorageTestFixture, TestWriteMetaData) {
  MetaData metadata;
  metadata.pageSize = pageSize;
  metadata.freePages = {1, 2, 3};

  writeStorage->write(metadata);

  // Read written file
  std::fstream file = file::open(writePath + ".metadata",
                                 std::fstream::in | std::fstream::binary);
  uint64_t fileSize = file::size(file);
  std::vector<uint8_t> buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  MetaData _metadata;
  _metadata.load(buffer);

  ASSERT_EQ(metadata.freePages, _metadata.freePages);
  ASSERT_EQ(metadata.numPages, _metadata.numPages);
  ASSERT_EQ(metadata.pageSize, _metadata.pageSize);
}

/********************************
 * Testing for Existing Storage
 ********************************/

class ExistingFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/test_read.storage";
  const std::string writePath = base + "/test_write.storage";
  const uint64_t pageSize = 512;
  std::unique_ptr<FileStorage> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage>(readPath, pageSize);
    readStorage->open();
    writeStorage = std::make_unique<FileStorage>(writePath, pageSize);
    writeStorage->open();
  }

  void TearDown() override {
    readStorage->close();
    writeStorage->close();
  }
};

TEST_F(ExistingFileStorageTestFixture, TestReadBlock) {
  std::unique_ptr<Page> page = readStorage->read(1);
  RecordBlock &recordBlock = page->getRecordBlock(1);

  ASSERT_EQ(page->getId(), 1);
  ASSERT_EQ(recordBlock.data, ByteBuffer({'t', 'e', 's', 't', 'i', 'n', 'g'}));
}

TEST_F(ExistingFileStorageTestFixture, TestWriteBlock) {
  RecordBlock recordBlock;
  recordBlock.data = {'t', 'e', 's', 't', 'i', 'n', 'g'};

  Page page(1, pageSize);
  PageSlotId slotId = page.addRecordBlock(recordBlock);
  writeStorage->write(page);

  std::fstream file = file::open(writePath, std::ios::in | std::ios::binary);
  ByteBuffer buffer(pageSize);
  file::read(file, buffer, 0);
  Page _dataBlock;
  _dataBlock.load(buffer);
  RecordBlock &_recordBlock = page.getRecordBlock(slotId);

  ASSERT_EQ(page.getId(), _dataBlock.getId());
  ASSERT_EQ(recordBlock.data, _recordBlock.data);
}

TEST_F(ExistingFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<MetaData> metadata = readStorage->read();
  MetaData _metadata;
  _metadata.pageSize = 1024; // Page in saved metadata
  _metadata.numPages = 10;
  _metadata.freePages = {1, 2, 3};

  ASSERT_EQ(metadata->pageSize, _metadata.pageSize);
  ASSERT_EQ(metadata->numPages, _metadata.numPages);
  ASSERT_EQ(metadata->freePages, _metadata.freePages);
}

TEST_F(ExistingFileStorageTestFixture, TestWriteMetaData) {
  MetaData metadata;
  metadata.pageSize = 1024;
  metadata.numPages = 10;
  metadata.freePages = {1, 2, 3};

  writeStorage->write(metadata);

  // Read written file
  std::fstream file = file::open(writePath + ".metadata",
                                 std::fstream::in | std::fstream::binary);
  uint64_t fileSize = file::size(file);
  std::vector<uint8_t> buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  MetaData _metadata;
  _metadata.load(buffer);

  ASSERT_EQ(metadata.freePages, _metadata.freePages);
  ASSERT_EQ(metadata.numPages, _metadata.numPages);
  ASSERT_EQ(metadata.pageSize, _metadata.pageSize);
}
