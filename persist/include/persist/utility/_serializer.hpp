/**
 * _serializer.hpp - Persist
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
 * @brief Internal header containing low level serialization methods.
 *
 */

#ifndef UTILITY__SERIALIZER_HPP
#define UTILITY__SERIALIZER_HPP

#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#include <persist/core/defs.hpp>

namespace persist {

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class T> inline void _copy(Span &input, T &data) {
  // Copy bytes
  std::memcpy((void *)&data, (const void *)input.start, sizeof(T));
  // Updated span
  input += sizeof(T);
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class T> inline void _copy(Span &input, std::vector<T> &data) {
  // Copy number of elements in the container
  size_t size;
  _copy(input, size);
  // Copy each element in the container
  for (int i = 0; i < size; i++) {
    T element;
    _copy(input, element);
    data.insert(data.end(), element);
  }
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class T> inline void _copy(Span &input, std::list<T> &data) {
  // Copy number of elements in the container
  size_t size;
  _copy(input, size);
  // Copy each element in the container
  for (int i = 0; i < size; i++) {
    T element;
    _copy(input, element);
    data.insert(data.end(), element);
  }
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class T> inline void _copy(Span &input, std::set<T> &data) {
  // Copy number of elements in the container
  size_t size;
  _copy(input, size);
  // Copy each element in the container
  for (int i = 0; i < size; i++) {
    T element;
    _copy(input, element);
    data.insert(element);
  }
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class K, class V>
inline void _copy(Span &input, std::pair<K, V> &data) {
  // Copy pair
  _copy(input, data.first);
  _copy(input, data.second);
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class K, class V>
inline void _copy(Span &input, std::map<K, V> &data) {
  // Copy number of elements in the container
  size_t size;
  _copy(input, size);
  // Copy each element in the container
  for (int i = 0; i < size; i++) {
    std::pair<K, V> element;
    _copy(input, element);
    data.insert(element);
  }
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class K, class V>
inline void _copy(Span &input, std::unordered_map<K, V> &data) {
  // Copy number of elements in the container
  size_t size;
  _copy(input, size);
  // Copy each element in the container
  for (int i = 0; i < size; i++) {
    std::pair<K, V> element;
    _copy(input, element);
    data.insert(element);
  }
}

// -------------------------------------

// Internal method to copy content from data variable to memory location
// specified by span object
template <class T> inline void _copy(const T &data, Span &output) {
  // Dump bytes
  std::memcpy((void *)output.start, (const void *)&data, sizeof(T));
  // Updated span
  output += sizeof(T);
}

// Internal method to copy content from data variable to memory location
// specified by span object
template <class K, class V>
inline void _copy(const std::pair<K, V> &data, Span &output) {
  // Copy pair
  _copy(data.first, output);
  _copy(data.second, output);
}

// Internal method to copy content from memory location specified by span object
// to a data variable.
template <class T, template <class, class...> class C, class... Args>
inline void _copy(const C<T, Args...> &data, Span &output) {
  // Copy number of elements in the container
  _copy(data.size(), output);
  // Copy each element in the container
  for (auto &element : data) {
    _copy(element, output);
  }
}

} // namespace persist

#endif /* UTILITY__SERIALIZER_HPP */
