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

using namespace persist;

class PageTableTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  std::unique_ptr<Page> page_1, page_2, page_3;
  std::unique_ptr<MetaData> metadata;
  std::unique_ptr<PageTable> table;
  std::unique_ptr<MemoryStorage> storage;

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

    table = std::make_unique<PageTable>(*storage, maxSize);
  }

  void TearDown() override { storage->close(); }
};

TEST_F(PageTableTestFixture, TestPageTableError) {
  try {
    PageTable table(*storage, 1); //<- invalid max size value
    FAIL() << "Expected PageTableError Exception.";
  } catch (PageTableError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageTableError Exception.";
  }
}

TEST_F(PageTableTestFixture, TestGet) {
  PageId pageId = page_1->getId();
  Page page = table->get(pageId);

  ASSERT_EQ(page.getId(), pageId);
  ASSERT_EQ(page.getNextPageId(), page_1->getNextPageId());
  ASSERT_EQ(page.getPrevPageId(), page_1->getPrevPageId());
  ASSERT_EQ(page.freeSpace(), page_1->freeSpace());
}

TEST_F(PageTableTestFixture, TestGetError) {
  try {
    Page page = table->get(10);
    FAIL() << "Expected PageNotFoundError Exception.";
  } catch (PageNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageNotFoundError Exception.";
  }
}

TEST_F(PageTableTestFixture, TestGetLRUPersist) {
  PageTable::Session session = table->createSession();

  // Getting the first page and modifying it
  Page &_page_1 = table->get(1);
  RecordBlock block;
  block.data =
      ByteBuffer(_page_1.freeSpace(true) - sizeof(RecordBlock::Header), 'A');
  PageSlotId slotId = _page_1.addRecordBlock(block);
  session.stage(1);

  // Filling up page table cache
  table->get(2);
  table->get(3); //<- this should persist page 1

  Page &__page_1 = table->get(1); //<- this will load page from storage

  ASSERT_EQ(__page_1.getId(), 1);
  ASSERT_EQ(__page_1.getRecordBlock(slotId).data, block.data);

  // Checking for update in metadata
  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(_metadata->numPages, metadata->numPages);
  ASSERT_EQ(_metadata->freePages.size(), metadata->freePages.size() - 1);
  ASSERT_EQ(_metadata->freePages, std::set<PageId>({2, 3}));
}

TEST_F(PageTableTestFixture, TestGetNewLRUPersist) {
  PageTable::Session session = table->createSession();

  // Getting the new page
  Page &_page_4 = table->getNew();
  session.stage(4);

  // Filling up page table cache
  table->get(1);
  table->get(2); //<- this should persist page 4

  Page &__page_4 = table->get(4); //<- this will load page from storage

  ASSERT_EQ(__page_4.getId(), 4);

  // Checking for update in metadata
  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(_metadata->numPages, metadata->numPages + 1);
  ASSERT_EQ(_metadata->freePages.size(), metadata->freePages.size() + 1);
  ASSERT_EQ(_metadata->freePages, std::set<PageId>({1, 2, 3, 4}));
}

TEST_F(PageTableTestFixture, TestGetFree) {
  // Getting page with free space
  Page &_page = table->getFree();

  ASSERT_EQ(_page.getId(), 3); //<- returns the last page in free list
}

TEST_F(PageTableTestFixture, TestGetFreeNew) {
  PageTable::Session session = table->createSession();

  // Fill all pages
  for (int i = 1; i <= 3; i++) {
    Page &_page = table->get(i);
    RecordBlock block;
    block.data =
        ByteBuffer(_page.freeSpace(true) - sizeof(RecordBlock::Header), 'A');
    PageSlotId slotId = _page.addRecordBlock(block);
    session.stage(i);
  }

  // Getting new page with free space
  Page &_page = table->getFree();

  ASSERT_EQ(_page.getId(), 4);
}

TEST_F(PageTableTestFixture, TestSessionCommit) {
  PageTable::Session session = table->createSession();

  // Getting the first page and modifying it
  Page &_page_1 = table->get(1);
  RecordBlock block;
  block.data =
      ByteBuffer(_page_1.freeSpace(true) - sizeof(RecordBlock::Header), 'A');
  PageSlotId slotId = _page_1.addRecordBlock(block);
  session.stage(1);

  // Commit
  session.commit();

  Page &__page_1 = table->get(1); //<- this will load page from storage

  ASSERT_EQ(__page_1.getId(), 1);
  ASSERT_EQ(__page_1.getRecordBlock(slotId).data, block.data);

  // Checking for update in metadata
  std::unique_ptr<MetaData> _metadata = storage->read();

  ASSERT_EQ(_metadata->numPages, metadata->numPages);
  ASSERT_EQ(_metadata->freePages.size(), metadata->freePages.size() - 1);
  ASSERT_EQ(_metadata->freePages, std::set<PageId>({2, 3}));
}
