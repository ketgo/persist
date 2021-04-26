/**
 * test_serializer.cpp - Persist
 *
 * Copyright 2021 Ketan Goyal
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
 * @brief Uint Test Serializer
 *
 */

#include <gtest/gtest.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include <persist/utility/serializer.hpp>

using namespace persist;

struct MockData {
  uint64_t a;
  char c;

  bool operator==(const MockData &other) const {
    return other.a == a && other.c == c;
  }
} __attribute__((packed));

class UtilitySerializerTestFixture : public ::testing::Test {
protected:
  ByteBuffer input_vector2d;
  ByteBuffer input_set;
  ByteBuffer input_list;
  ByteBuffer input_umap;
  ByteBuffer input_map;
  ByteBuffer input_all;
  std::vector<std::vector<MockData>> vector2d;
  size_t vector2dSize;
  std::set<uint64_t> set;
  size_t setSize;
  std::list<char> list;
  size_t listSize;
  std::unordered_map<char, int> umap;
  size_t umapSize;
  std::map<char, MockData> map;
  size_t mapSize;
  size_t allSize;
  void SetUp() override {
    // Setting up mock data
    vector2d = {{{1, '1'}, {2, '2'}}, {{3, '3'}, {4, '4'}}};
    set = {1, 2, 3, 4, 5};
    list = {'a', 'b', 'c', 'd', 'e'};
    umap = {{'1', 1}, {'2', 2}, {'3', 3}};
    map = {{'4', {4, '4'}}, {'5', {5, '5'}}, {'6', {6, '6'}}};

    // Vector data size
    vector2dSize = 0;
    for (auto &element : vector2d) {
      vector2dSize += element.size() * sizeof(MockData);
      vector2dSize += sizeof(size_t);
    }
    vector2dSize += sizeof(size_t);
    // Set data size
    setSize = set.size() * sizeof(uint64_t) + sizeof(size_t);
    // List data size
    listSize = list.size() * sizeof(char) + sizeof(size_t);
    // Unordered map data size
    umapSize = umap.size() * (sizeof(char) + sizeof(int)) + sizeof(size_t);
    // Map data size
    mapSize = map.size() * (sizeof(char) + sizeof(MockData)) + sizeof(size_t);
    // Combined data size
    allSize = vector2dSize + setSize + listSize + umapSize + mapSize;

    // Byte buffer
    input_vector2d = {2, 0, 0, 0,  0, 0,  0, 0, 2, 0,  0, 0, 0, 0, 0,
                      0, 1, 0, 0,  0, 0,  0, 0, 0, 49, 2, 0, 0, 0, 0,
                      0, 0, 0, 50, 2, 0,  0, 0, 0, 0,  0, 0, 3, 0, 0,
                      0, 0, 0, 0,  0, 51, 4, 0, 0, 0,  0, 0, 0, 0, 52};

    input_set = {5, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
                 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
                 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0};

    input_list = {5, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 100, 101};

    input_umap = {3, 0,  0, 0, 0, 0, 0,  0, 51, 3, 0, 0,
                  0, 50, 2, 0, 0, 0, 49, 1, 0,  0, 0};

    input_map = {3, 0, 0, 0, 0, 0, 0, 0, 52, 4,  0, 0, 0, 0, 0, 0, 0, 52, 53,
                 5, 0, 0, 0, 0, 0, 0, 0, 53, 54, 6, 0, 0, 0, 0, 0, 0, 0,  54};

    input_all = {2,   0,   0,  0,  0, 0, 0, 0,  2,  0,  0,  0, 0, 0, 0,  0,  1,
                 0,   0,   0,  0,  0, 0, 0, 49, 2,  0,  0,  0, 0, 0, 0,  0,  50,
                 2,   0,   0,  0,  0, 0, 0, 0,  3,  0,  0,  0, 0, 0, 0,  0,  51,
                 4,   0,   0,  0,  0, 0, 0, 0,  52, 5,  0,  0, 0, 0, 0,  0,  0,
                 1,   0,   0,  0,  0, 0, 0, 0,  2,  0,  0,  0, 0, 0, 0,  0,  3,
                 0,   0,   0,  0,  0, 0, 0, 4,  0,  0,  0,  0, 0, 0, 0,  5,  0,
                 0,   0,   0,  0,  0, 0, 5, 0,  0,  0,  0,  0, 0, 0, 97, 98, 99,
                 100, 101, 3,  0,  0, 0, 0, 0,  0,  0,  51, 3, 0, 0, 0,  50, 2,
                 0,   0,   0,  49, 1, 0, 0, 0,  3,  0,  0,  0, 0, 0, 0,  0,  52,
                 4,   0,   0,  0,  0, 0, 0, 0,  52, 53, 5,  0, 0, 0, 0,  0,  0,
                 0,   53,  54, 6,  0, 0, 0, 0,  0,  0,  0,  54};
  }
};

TEST_F(UtilitySerializerTestFixture, TestLoadVector) {
  std::vector<std::vector<MockData>> _vector2d;

  Span span(input_vector2d);
  load(span, _vector2d);

  ASSERT_EQ(_vector2d, vector2d);
}

TEST_F(UtilitySerializerTestFixture, TestDumpVector) {
  ByteBuffer output(vector2dSize);
  Span span(output);
  dump(span, vector2d);

  ASSERT_EQ(output, input_vector2d);
}

TEST_F(UtilitySerializerTestFixture, TestLoadSet) {
  std::set<uint64_t> _set;

  Span span(input_set);
  load(span, _set);

  ASSERT_EQ(_set, set);
}

TEST_F(UtilitySerializerTestFixture, TestDumpSet) {
  ByteBuffer output(setSize);
  Span span(output);
  dump(span, set);

  ASSERT_EQ(output, input_set);
}

TEST_F(UtilitySerializerTestFixture, TestLoadList) {
  std::list<char> _list;

  Span span(input_list);
  load(span, _list);

  ASSERT_EQ(_list, list);
}

TEST_F(UtilitySerializerTestFixture, TestDumpList) {
  ByteBuffer output(listSize);
  Span span(output);
  dump(span, list);

  ASSERT_EQ(output, input_list);
}

TEST_F(UtilitySerializerTestFixture, TestLoadUmap) {
  std::unordered_map<char, int> _umap;

  Span span(input_umap);
  load(span, _umap);

  ASSERT_EQ(_umap, umap);
}

TEST_F(UtilitySerializerTestFixture, TestDumpUmap) {
  ByteBuffer output(umapSize);
  Span span(output);
  dump(span, umap);

  ASSERT_EQ(output, input_umap);
}

TEST_F(UtilitySerializerTestFixture, TestLoadMap) {
  std::map<char, MockData> _map;

  Span span(input_map);
  load(span, _map);

  ASSERT_EQ(_map.size(), map.size());
  for (auto &x : _map) {
    auto it = map.find(x.first);
    ASSERT_TRUE(it != map.end());
    ASSERT_EQ(it->second, x.second);
  }
}

TEST_F(UtilitySerializerTestFixture, TestDumpMap) {
  ByteBuffer output(mapSize);
  Span span(output);
  dump(span, map);

  ASSERT_EQ(output, input_map);
}

TEST_F(UtilitySerializerTestFixture, TestLoadAll) {
  std::vector<std::vector<MockData>> _vector2d;
  std::set<uint64_t> _set;
  std::list<char> _list;
  std::unordered_map<char, int> _umap;
  std::map<char, MockData> _map;

  Span span(input_all);
  load(span, _vector2d, _set, _list, _umap, _map);

  ASSERT_EQ(_vector2d, vector2d);
  ASSERT_EQ(_set, set);
  ASSERT_EQ(_list, list);
  ASSERT_EQ(_umap, umap);
  ASSERT_EQ(_map.size(), map.size());
  for (auto &x : _map) {
    auto it = map.find(x.first);
    ASSERT_TRUE(it != map.end());
    ASSERT_EQ(it->second, x.second);
  }
}

TEST_F(UtilitySerializerTestFixture, TestDumpAll) {
  ByteBuffer output(allSize);
  Span span(output);
  dump(span, vector2d, set, list, umap, map);

  ASSERT_EQ(output, input_all);
}
