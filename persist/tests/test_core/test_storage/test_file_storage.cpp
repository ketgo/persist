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

#include <persist/core/page/creator.hpp>
#include <persist/core/storage/file_storage.hpp>

#include "common.hpp"
#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

const std::string base = DATA_PATH;

/********************************
 * Testing for New Storage
 ********************************/

class NewFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string read_path = base + "/_read";
  const std::string write_path = base + "/_write";
  const uint64_t page_size = 512;
  std::unique_ptr<FileStorage<SimplePage>> read_storage, write_storage;

  void SetUp() override {
    read_storage =
        std::make_unique<FileStorage<SimplePage>>(read_path, page_size);
    read_storage->Open();
    write_storage =
        std::make_unique<FileStorage<SimplePage>>(write_path, page_size);
    write_storage->Open();
  }

  void TearDown() override {
    read_storage->Close();
    write_storage->Close();
  }
};

TEST_F(NewFileStorageTestFixture, TestOpen) {
  ByteBuffer buffer;
  FileHeader header;
  std::fstream write_file =
      file::open(write_path, std::ios::in | std::ios::binary);
  std::fstream read_file =
      file::open(read_path, std::ios::in | std::ios::binary);

  buffer.resize(header.GetStorageSize());
  file::read(write_file, buffer, 0);
  header.Load(buffer);
  ASSERT_EQ(header.page_size, page_size);

  buffer.resize(header.GetStorageSize());
  file::read(read_file, buffer, 0);
  header.Load(buffer);
  ASSERT_EQ(header.page_size, page_size);
}

TEST_F(NewFileStorageTestFixture, TestReadPage) {
  ASSERT_THROW(read_storage->Read(1), PageNotFoundError);
}

TEST_F(NewFileStorageTestFixture, TestWritePage) {
  auto page = CreatePage<SimplePage>(1, page_size);
  ByteBuffer record = "testing"_bb;
  page->SetRecord(record);

  write_storage->Write(*page);

  std::fstream file = file::open(write_path, std::ios::in | std::ios::binary);
  ByteBuffer buffer(page_size);
  file::read(file, buffer, FileHeader().GetStorageSize());
  auto _page = persist::LoadPage<SimplePage>(buffer);

  ASSERT_EQ(page->GetId(), _page->GetId());
  ASSERT_EQ(page->GetRecord(), _page->GetRecord());
}

TEST_F(NewFileStorageTestFixture, TestAllocate) {
  ASSERT_EQ(read_storage->Allocate(), 1);
}

/********************************
 * Testing for Existing Storage
 ********************************/

class ExistingFileStorageTestFixture : public ::testing::Test {
protected:
  const std::string read_path = base + "/test_read";
  const std::string write_path = base + "/test_write";
  const uint64_t page_size = 512;
  std::unique_ptr<FileStorage<SimplePage>> read_storage, write_storage;

  void SetUp() override {
    read_storage =
        std::make_unique<FileStorage<SimplePage>>(read_path, page_size);
    read_storage->Open();
    write_storage =
        std::make_unique<FileStorage<SimplePage>>(write_path, page_size);
    write_storage->Open();
  }

  void TearDown() override {
    read_storage->Close();
    write_storage->Close();
  }
};

TEST_F(ExistingFileStorageTestFixture, TestOpen) {
  ByteBuffer buffer;
  FileHeader header;
  std::fstream write_file =
      file::open(write_path, std::ios::in | std::ios::binary);
  std::fstream read_file =
      file::open(read_path, std::ios::in | std::ios::binary);

  buffer.resize(header.GetStorageSize());
  file::read(write_file, buffer, 0);
  header.Load(buffer);
  ASSERT_EQ(header.page_size, page_size);

  buffer.resize(header.GetStorageSize());
  file::read(read_file, buffer, 0);
  header.Load(buffer);
  ASSERT_EQ(header.page_size, page_size);
}

TEST_F(ExistingFileStorageTestFixture, TestReadPage) {
  auto page = read_storage->Read(1);

  ASSERT_EQ(page->GetId(), 1);
  ASSERT_EQ(page->GetRecord(), "testing"_bb);
}

TEST_F(ExistingFileStorageTestFixture, TestWritePage) {
  auto page = CreatePage<SimplePage>(1, page_size);
  ByteBuffer record = "testing"_bb;
  page->SetRecord(record);

  write_storage->Write(*page);

  std::fstream file = file::open(write_path, std::ios::in | std::ios::binary);
  ByteBuffer buffer(page_size);
  file::read(file, buffer, FileHeader().GetStorageSize());
  auto _page = persist::LoadPage<SimplePage>(buffer);

  ASSERT_EQ(page->GetId(), _page->GetId());
  ASSERT_EQ(page->GetRecord(), _page->GetRecord());
}

TEST_F(ExistingFileStorageTestFixture, TestAllocate) {
  ASSERT_EQ(read_storage->Allocate(), 2);
}
