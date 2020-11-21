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
 * Page Table Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/storage/memory_storage.hpp>
#include <persist/core/transaction_manager.hpp>

using namespace persist;

class PageTableWithMemoryStorageTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  std::unique_ptr<Page> page_1, page_2, page_3;
  std::unique_ptr<MetaData> metadata;
  std::unique_ptr<PageTable> pageTable;
  std::unique_ptr<MemoryStorage> storage;
  std::unique_ptr<LogManager> logManager;
  std::unique_ptr<TransactionManager> txnManager;

  void SetUp() override {
    // setting up pages
    page_1 = std::make_unique<Page>(1, pageSize);
    page_2 = std::make_unique<Page>(2, pageSize);
    page_3 = std::make_unique<Page>(3, pageSize);

    // setting up metadata
    metadata = std::make_unique<MetaData>();
    metadata->pageSize = pageSize;
    metadata->numPages = 3;
    metadata->freePages.insert(1);
    metadata->freePages.insert(2);
    metadata->freePages.insert(3);

    // setting up storage
    storage = std::make_unique<MemoryStorage>(pageSize);
    storage->open();
    storage->write(*page_1);
    storage->write(*page_2);
    storage->write(*page_3);
    storage->write(*metadata);
    storage->close();

    pageTable = std::make_unique<PageTable>(*storage, maxSize);
    pageTable->open();

    // Setup transaction manager
    logManager = std::make_unique<LogManager>();
    txnManager = std::make_unique<TransactionManager>(*pageTable, *logManager);
  }

  void TearDown() override {
    storage->remove();
    pageTable->close();
  }
};

TEST_F(PageTableWithMemoryStorageTestFixture, TestPageTableError) {
  try {
    PageTable table(*storage, 1); //<- invalid max size value
    FAIL() << "Expected PageTableError Exception.";
  } catch (PageTableError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageTableError Exception.";
  }
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestGet) {
  PageId pageId = page_1->getId();
  Page page = pageTable->get(pageId);

  ASSERT_EQ(page.getId(), pageId);
  ASSERT_EQ(page.getNextPageId(), page_1->getNextPageId());
  ASSERT_EQ(page.getPrevPageId(), page_1->getPrevPageId());
  ASSERT_EQ(page.freeSpace(), page_1->freeSpace());
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestGetError) {
  try {
    Page page = pageTable->get(10);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestGetLRUPersist) {
  Transaction txn = txnManager->begin();

  // Getting the first page and modifying it
  Page &_page_1 = pageTable->get(1);
  RecordBlock block;
  block.data =
      ByteBuffer(_page_1.freeSpace(true) - sizeof(RecordBlock::Header), 'A');
  PageSlotId slotId = _page_1.addRecordBlock(txn, block);
  txn.stage(1);
  pageTable->mark(1);

  // Filling up page table cache
  pageTable->get(2);
  pageTable->get(3); //<- this should persist page 1

  Page &__page_1 = pageTable->get(1); //<- this will load page from storage

  ASSERT_EQ(__page_1.getId(), 1);
  ASSERT_EQ(__page_1.getRecordBlock(txn, slotId).data, block.data);

  // Checking for update in metadata
  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(_metadata->numPages, metadata->numPages);
  ASSERT_EQ(_metadata->freePages.size(), metadata->freePages.size() - 1);
  ASSERT_EQ(_metadata->freePages, std::set<PageId>({2, 3}));
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestGetNewLRUPersist) {
  Transaction txn = txnManager->begin();

  // Getting the new page
  Page &_page_4 = pageTable->getNew();
  txn.stage(4);
  pageTable->mark(4);

  // Filling up page table cache
  pageTable->get(1);
  pageTable->get(2); //<- this should persist page 4

  Page &__page_4 = pageTable->get(4); //<- this will load page from storage

  ASSERT_EQ(__page_4.getId(), 4);

  // Checking for update in metadata
  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(_metadata->numPages, metadata->numPages + 1);
  ASSERT_EQ(_metadata->freePages.size(), metadata->freePages.size() + 1);
  ASSERT_EQ(_metadata->freePages, std::set<PageId>({1, 2, 3, 4}));
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestGetFree) {
  // Getting page with free space
  Page &_page = pageTable->getFree();

  ASSERT_EQ(_page.getId(), 3); //<- returns the last page in free list
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestGetFreeNew) {
  Transaction txn = txnManager->begin();

  // Fill all pages
  for (int i = 1; i <= 3; i++) {
    Page &_page = pageTable->get(i);
    RecordBlock block;
    block.data =
        ByteBuffer(_page.freeSpace(true) - sizeof(RecordBlock::Header), 'A');
    PageSlotId slotId = _page.addRecordBlock(txn, block);
    txn.stage(i);
    pageTable->mark(i);
  }

  // Getting new page with free space
  Page &_page = pageTable->getFree();

  ASSERT_EQ(_page.getId(), 4);
}

TEST_F(PageTableWithMemoryStorageTestFixture, TestSessionCommit) {
  Transaction txn = txnManager->begin();

  // Getting the first page and modifying it
  Page &_page_1 = pageTable->get(1);
  RecordBlock block;
  block.data =
      ByteBuffer(_page_1.freeSpace(true) - sizeof(RecordBlock::Header), 'A');
  PageSlotId slotId = _page_1.addRecordBlock(txn, block);
  txn.stage(1);
  pageTable->mark(1);

  // Commit
  txnManager->commit(txn);

  Page &__page_1 = pageTable->get(1); //<- this will load page from storage

  ASSERT_EQ(__page_1.getId(), 1);
  ASSERT_EQ(__page_1.getRecordBlock(txn, slotId).data, block.data);

  // Checking for update in metadata
  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(_metadata->numPages, metadata->numPages);
  ASSERT_EQ(_metadata->freePages.size(), metadata->freePages.size() - 1);
  ASSERT_EQ(_metadata->freePages, std::set<PageId>({2, 3}));
}
