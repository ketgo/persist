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

#include <iostream>
#include <memory>
#include <string>

#include <persist/block.hpp>
#include <persist/storage/file.hpp>

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
  const std::string path = base + "/test.storage";
  const uint64_t blockSize = DEFAULT_DATA_BLOCK_SIZE;
  std::unique_ptr<FileStorage> storage;

  void SetUp() override {
    storage = std::make_unique<FileStorage>(path, blockSize);
  }
};

TEST_F(ExistingFileStorageTestFixture, TestReadBlock) {}

TEST_F(ExistingFileStorageTestFixture, TestWriteBlock) {}

TEST_F(ExistingFileStorageTestFixture, TestReadMetaData) {
  std::unique_ptr<Storage::MetaData> metadata = storage->read();
}

TEST_F(ExistingFileStorageTestFixture, TestReadNonExistingFileMetaData) {
  FileStorage _storage(base + "/temp.storage");
  std::unique_ptr<Storage::MetaData> metadata = _storage.read();
}

TEST_F(ExistingFileStorageTestFixture, TestWriteMetaData) {}
