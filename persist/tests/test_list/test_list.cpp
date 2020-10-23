/**
 * test_list.cpp - Persist
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
 * List Collection Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

/**
 * Enabled intrusive testing
 */
#define PERSIST_TESTING

#include <persist/core/defs.hpp>
#include <persist/list/list.hpp>

using namespace persist;

class ListIteratorTestFixture : public ::testing::Test {
protected:
  const std::string connetionString = "file://test_list.storage";
  const size_t num = 50;
  std::vector<RecordLocation> locations;
  std::unique_ptr<List> list, empty_list;

  void SetUp() override {
    empty_list = std::make_unique<List>("file://test_empty_list.storage");
    empty_list->open();

    insert();
    list = std::make_unique<List>(connetionString, 2);
    list->open();
  }

  void TearDown() override {
    empty_list->manager.storage->remove();
    empty_list->close();
    list->manager.storage->remove();
    list->close();
  }

private:
  /**
   * @brief Method to insert records in storage for testing.
   */
  void insert() {
    RecordManager manager(connetionString, 10);
    manager.start();

    List::Node prev_node, node;
    RecordLocation prev_location, location;
    ByteBuffer buffer;

    // Insert new node
    node.record = "testing-"_bb;
    node.record.push_back(std::to_string(0)[0]);
    buffer.clear();
    node.dump(buffer);
    location = manager.insert(buffer);
    locations.push_back(location);
    size_t count = 1;
    while (count < num) {
      // Set new node as previous node
      prev_location = location;
      prev_node = node;

      // Insert new node
      node.record = "testing-"_bb;
      node.record.push_back(std::to_string(count)[0]);
      node.previous = prev_location;
      buffer.clear();
      node.dump(buffer);
      location = manager.insert(buffer);
      locations.push_back(location);

      // Update previous node
      prev_node.next = location;
      buffer.clear();
      prev_node.dump(buffer);
      manager.update(buffer, prev_location);

      ++count;
    }

    manager.stop();
  }
};

TEST_F(ListIteratorTestFixture, TestForwardTraversalPrefixIncrement) {
  List::Iterator it(list.get(), locations.front()), end;
  ByteBuffer buffer;
  std::vector<RecordLocation> _locations(num);
  size_t count = 0;

  // Inequality comparision operator test
  while (it != end) {
    buffer = "testing-"_bb;
    buffer.push_back(std::to_string(count)[0]);

    ASSERT_EQ(*it, buffer);

    _locations[count] = it.getLocation();

    // Prefix increment
    ++it;
    ++count;
  }

  ASSERT_EQ(locations, _locations);

  // Equality comparision operator test
  ASSERT_TRUE(it == end);
}

TEST_F(ListIteratorTestFixture, TestForwardTraversalPostfixIncrement) {
  List::Iterator it(list.get(), locations.front()), end;
  ByteBuffer buffer;
  std::vector<RecordLocation> _locations(num);
  size_t count = 0;

  // Inequality comparision operator test
  while (it != end) {
    buffer = "testing-"_bb;
    buffer.push_back(std::to_string(count)[0]);

    ASSERT_EQ(*it, buffer);

    _locations[count] = it.getLocation();

    // Postfix increment
    it++;
    ++count;
  }

  ASSERT_EQ(locations, _locations);

  // Equality comparision operator test
  ASSERT_TRUE(it == end);
}

TEST_F(ListIteratorTestFixture, TestBackwardTraversalPrefixDecrement) {
  List::Iterator it(list.get(), locations.back()), begin;
  ByteBuffer buffer;
  std::vector<RecordLocation> _locations(num);
  size_t count = num - 1;

  // Inequality comparision operator test
  while (it != begin) {
    buffer = "testing-"_bb;
    buffer.push_back(std::to_string(count)[0]);

    ASSERT_EQ(*it, buffer);

    _locations[count] = it.getLocation();

    // Pretfix decrement
    --it;
    --count;
  }

  ASSERT_EQ(locations, _locations);

  // Equality comparision operator test
  ASSERT_TRUE(it == begin);
}

TEST_F(ListIteratorTestFixture, TestBackwardTraversalPostfixDecrement) {
  List::Iterator it(list.get(), locations.back()), begin;
  ByteBuffer buffer;
  std::vector<RecordLocation> _locations(num);
  size_t count = num - 1;

  // Inequality comparision operator test
  while (it != begin) {
    buffer = "testing-"_bb;
    buffer.push_back(std::to_string(count)[0]);

    ASSERT_EQ(*it, buffer);

    _locations[count] = it.getLocation();

    // Postfix decrement
    it--;
    --count;
  }

  ASSERT_EQ(locations, _locations);

  // Equality comparision operator test
  ASSERT_TRUE(it == begin);
}
