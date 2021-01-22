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

#include <memory>
#include <string>

#include <persist/core/exceptions.hpp>
#include <persist/core/log_manager.hpp>
#include <persist/core/page/slotted_page.hpp>
#include <persist/core/storage/file_storage.hpp>
#include <persist/core/transaction.hpp>

using namespace persist;

const std::string base = "persist_v2/tests/data";

/********************************
 * Testing for New Storage
 ********************************/

class NewFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/_read.storage";
  const std::string writePath = base + "/_write.storage";
  const uint64_t pageSize = 512;
  std::unique_ptr<FileStorage<SlottedPage>> readStorage, writeStorage;
  std::unique_ptr<LogManager> logManager;

  void SetUp() override {
    readStorage =
        std::make_unique<FileStorage<SlottedPage>>(readPath, pageSize);
    readStorage->open();
    writeStorage =
        std::make_unique<FileStorage<SlottedPage>>(writePath, pageSize);
    writeStorage->open();

    // Setup log manager
    logManager = std::make_unique<LogManager>();
  }

  void TearDown() override {
    readStorage->close();
    writeStorage->close();
  }
};

TEST_F(NewFileStorageTestFixture, TestReadBlock) {
  try {
    std::unique_ptr<SlottedPage> page = readStorage->read(1);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(NewFileStorageTestFixture, TestWriteBlock) {
  Transaction txn(*logManager, 0);
  RecordBlock recordBlock;
  recordBlock.data = "testing"_bb;

  SlottedPage page(1, pageSize);
  PageSlotId slotId = page.addRecordBlock(txn, recordBlock).first;
  writeStorage->write(page);

  std::fstream file = file::open(writePath, std::ios::in | std::ios::binary);
  ByteBuffer buffer(pageSize);
  file::read(file, buffer, 0);
  SlottedPage _page(
      0, pageSize); //<- page ID does not match expected. It should get
                    // overritten after loading from buffer.
  _page.load(Span({buffer.data(), buffer.size()}));
  RecordBlock &_recordBlock = page.getRecordBlock(txn, slotId);

  ASSERT_EQ(page.getId(), _page.getId());
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
  ByteBuffer buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  MetaData _metadata;
  _metadata.load(Span({buffer.data(), buffer.size()}));

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
  std::unique_ptr<FileStorage<SlottedPage>> readStorage, writeStorage;
  std::unique_ptr<LogManager> logManager;

  void SetUp() override {
    readStorage =
        std::make_unique<FileStorage<SlottedPage>>(readPath, pageSize);
    readStorage->open();
    writeStorage =
        std::make_unique<FileStorage<SlottedPage>>(writePath, pageSize);
    writeStorage->open();

    // Setup log manager
    logManager = std::make_unique<LogManager>();
  }

  void TearDown() override {
    readStorage->close();
    writeStorage->close();
  }
};

TEST_F(ExistingFileStorageTestFixture, TestReadBlock) {
  Transaction txn(*logManager, 0);
  std::unique_ptr<SlottedPage> page = readStorage->read(1);
  RecordBlock &recordBlock = page->getRecordBlock(txn, 1);

  ASSERT_EQ(page->getId(), 1);
  ASSERT_EQ(recordBlock.data, ByteBuffer("testing"_bb));
}

TEST_F(ExistingFileStorageTestFixture, TestWriteBlock) {
  Transaction txn(*logManager, 0);
  RecordBlock recordBlock;
  recordBlock.data = "testing"_bb;

  SlottedPage page(1, pageSize);
  PageSlotId slotId = page.addRecordBlock(txn, recordBlock).first;
  writeStorage->write(page);

  std::fstream file = file::open(writePath, std::ios::in | std::ios::binary);
  ByteBuffer buffer(pageSize);
  file::read(file, buffer, 0);
  SlottedPage _page(
      0, pageSize); //<- page ID does not match expected. It should get
                    // overritten after loading from buffer.
  _page.load(Span({buffer.data(), buffer.size()}));
  RecordBlock &_recordBlock = page.getRecordBlock(txn, slotId);

  ASSERT_EQ(page.getId(), _page.getId());
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
  ByteBuffer buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  MetaData _metadata;
  _metadata.load(Span({buffer.data(), buffer.size()}));

  ASSERT_EQ(metadata.freePages, _metadata.freePages);
  ASSERT_EQ(metadata.numPages, _metadata.numPages);
  ASSERT_EQ(metadata.pageSize, _metadata.pageSize);
}
