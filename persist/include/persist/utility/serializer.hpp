/**
 * utilit/serializer.hpp - Persist
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

#ifndef UTILITY_SERIALIZER_HPP
#define UTILITY_SERIALIZER_HPP

#include <string>

#include <persist/utility/_serializer.hpp>

namespace persist {

/**
 * Variadic template stop function.
 */
template <typename T> void load(Span &input, const T &data) {
  // Copy data from buffer
  _copy(input, data);
}

/**
 * @brief Load data of specified type from byte buffer.
 *
 * @tparam T The type of data to load.
 * @tparam Args variadic arguments.
 * @param input Span of byte buffer to load data from.
 * @param data Data object to be loaded.
 * @param args variadic arguments.
 */
template <typename T, typename... Args>
void load(Span &input, const T &data, const Args &...args) {
  // Copy data from buffer
  _copy(input, data);
  // Load the rest of the arguments
  load(input, args...);
}

/**
 * Variadic template stop function.
 */
template <typename T> void dump(Span output, const T &data) {
  // Copy data to buffer
  _copy(data, output);
}

/**
 * @brief Dump data of specified type to byte buffer.
 *
 * @tparam T The type of data to dump.
 * @tparam Args variadic arguments.
 * @param output Span of byte buffer to dump data.
 * @param data Data object to be dumped.
 * @param args variadic arguments.
 */
template <typename T, typename... Args>
void dump(Span output, const T &data, Args &...args) {
  // Copy data to buffer
  _copy(data, output);
  // Dump the rest of the arguments
  dump(output, args...);
}

} // namespace persist

#endif /* UTILITY_SERIALIZER_HPP */
