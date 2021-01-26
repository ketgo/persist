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
#include <persist/core/page/simple_page.hpp>
#include <persist/core/storage/file_storage.hpp>

using namespace persist;

const std::string base = "persist_v2/tests/data";

/********************************
 * Testing for New Storage
 ********************************/

class NewFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/_read";
  const std::string writePath = base + "/_write";
  const uint64_t pageSize = 512;
  std::unique_ptr<FileStorage<SimplePage>> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage<SimplePage>>(readPath, pageSize);
    readStorage->open();
    writeStorage =
        std::make_unique<FileStorage<SimplePage>>(writePath, pageSize);
    writeStorage->open();
  }

  void TearDown() override {
    readStorage->close();
    writeStorage->close();
  }
};

TEST_F(NewFileStorageTestFixture, TestOpen) {
  ByteBuffer buffer;
  FileHeader header;
  std::fstream writeFile =
      file::open(writePath + FILE_STORAGE_DATA_FILE_EXTENTION,
                 std::ios::in | std::ios::binary);
  std::fstream readFile =
      file::open(readPath + FILE_STORAGE_DATA_FILE_EXTENTION,
                 std::ios::in | std::ios::binary);

  buffer.resize(writeStorage->headerSize);
  file::read(writeFile, buffer, 0);
  header.load(buffer);
  ASSERT_EQ(header.pageSize, pageSize);

  buffer.resize(readStorage->headerSize);
  file::read(readFile, buffer, 0);
  header.load(buffer);
  ASSERT_EQ(header.pageSize, pageSize);
}

TEST_F(NewFileStorageTestFixture, TestReadPage) {
  try {
    std::unique_ptr<SimplePage> page = readStorage->read(1);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(NewFileStorageTestFixture, TestWritePage) {
  SimplePage page(1, pageSize);
  ByteBuffer record = "testing"_bb;
  page.setRecord(record);

  writeStorage->write(page);

  std::fstream file = file::open(writePath + FILE_STORAGE_DATA_FILE_EXTENTION,
                                 std::ios::in | std::ios::binary);
  ByteBuffer buffer(pageSize);
  file::read(file, buffer, writeStorage->headerSize);
  SimplePage _page(0, pageSize);
  _page.load(Span(buffer));

  ASSERT_EQ(page.getId(), _page.getId());
  ASSERT_EQ(page.getRecord(), _page.getRecord());
}

TEST_F(NewFileStorageTestFixture, TestAllocate) {
  ASSERT_EQ(readStorage->allocate(), 1);
}

TEST_F(NewFileStorageTestFixture, TestReadFSL) {
  std::unique_ptr<FSL> fsl = readStorage->read();

  ASSERT_EQ(fsl->freePages.size(), 0);
}

TEST_F(NewFileStorageTestFixture, TestWriteFSL) {
  FSL fsl;
  fsl.freePages = {1, 2, 3};

  writeStorage->write(fsl);

  // Read written file
  std::fstream file = file::open(writePath + FILE_STORAGE_FSL_FILE_EXTENTION,
                                 std::fstream::in | std::fstream::binary);
  uint64_t fileSize = file::size(file);
  ByteBuffer buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  FSL _fsl;
  _fsl.load(Span(buffer));

  ASSERT_EQ(fsl.freePages, _fsl.freePages);
}

/********************************
 * Testing for Existing Storage
 ********************************/

class ExistingFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string readPath = base + "/test_read";
  const std::string writePath = base + "/test_write";
  const uint64_t pageSize = 512;
  std::unique_ptr<FileStorage<SimplePage>> readStorage, writeStorage;

  void SetUp() override {
    readStorage = std::make_unique<FileStorage<SimplePage>>(readPath, pageSize);
    readStorage->open();
    writeStorage =
        std::make_unique<FileStorage<SimplePage>>(writePath, pageSize);
    writeStorage->open();
  }

  void TearDown() override {
    readStorage->close();
    writeStorage->close();
  }
};

TEST_F(ExistingFileStorageTestFixture, TestOpen) {
  ByteBuffer buffer;
  FileHeader header;
  std::fstream writeFile =
      file::open(writePath + FILE_STORAGE_DATA_FILE_EXTENTION,
                 std::ios::in | std::ios::binary);
  std::fstream readFile =
      file::open(readPath + FILE_STORAGE_DATA_FILE_EXTENTION,
                 std::ios::in | std::ios::binary);

  buffer.resize(writeStorage->headerSize);
  file::read(writeFile, buffer, 0);
  header.load(buffer);
  ASSERT_EQ(header.pageSize, pageSize);

  buffer.resize(readStorage->headerSize);
  file::read(readFile, buffer, 0);
  header.load(buffer);
  ASSERT_EQ(header.pageSize, pageSize);
}

TEST_F(ExistingFileStorageTestFixture, TestReadPage) {
  std::unique_ptr<SimplePage> page = readStorage->read(1);

  ASSERT_EQ(page->getId(), 1);
  ASSERT_EQ(page->getRecord(), "testing"_bb);
}

TEST_F(ExistingFileStorageTestFixture, TestWritePage) {
  SimplePage page(1, pageSize);
  ByteBuffer record = "testing"_bb;
  page.setRecord(record);

  writeStorage->write(page);

  std::fstream file = file::open(writePath + FILE_STORAGE_DATA_FILE_EXTENTION,
                                 std::ios::in | std::ios::binary);
  ByteBuffer buffer(pageSize);
  file::read(file, buffer, writeStorage->headerSize);
  SimplePage _page(0, pageSize);
  _page.load(Span(buffer));

  ASSERT_EQ(page.getId(), _page.getId());
  ASSERT_EQ(page.getRecord(), _page.getRecord());
}

TEST_F(ExistingFileStorageTestFixture, TestAllocate) {
  ASSERT_EQ(readStorage->allocate(), 2);
}

TEST_F(ExistingFileStorageTestFixture, TestReadFSL) {
  std::unique_ptr<FSL> fsl = readStorage->read();
  FSL _fsl;
  _fsl.freePages = {1, 2, 3};

  ASSERT_EQ(fsl->freePages, _fsl.freePages);
}

TEST_F(ExistingFileStorageTestFixture, TestWriteFSL) {
  FSL fsl;
  fsl.freePages = {1, 2, 3};

  writeStorage->write(fsl);

  // Read written file
  std::fstream file = file::open(writePath + FILE_STORAGE_FSL_FILE_EXTENTION,
                                 std::fstream::in | std::fstream::binary);
  uint64_t fileSize = file::size(file);
  ByteBuffer buffer;
  buffer.resize(fileSize);
  file::read(file, buffer, 0);

  FSL _fsl;
  _fsl.load(Span(buffer));

  ASSERT_EQ(fsl.freePages, _fsl.freePages);
}
