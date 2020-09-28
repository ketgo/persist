/**
 * utility.hpp - Persist
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
 * The header contains utility methods and class.
 */

#ifndef UTILITY_HPP
#define UTILITY_HPP

#include <fstream>

namespace persist {

/**
 * @brief Utility function to fill values in one vector from another starting
 * at given offset.
 *
 * @param buffer_1 buffer to fill
 * @param buffer_2 buffer from which to retrive values
 * @param offset offset at which to start filling values in buffer
 */
template <class T>
void write(std::vector<T> &buffer_1, std::vector<T> &buffer_2, size_t offset) {
  for (size_t i = 0; i < buffer_2.size(); ++i) {
    buffer_1[i + offset] = buffer_2[i];
  }
}

/**
 * @brief Utility function to fill values in buffer for given value, starting
 * offset and limit.
 *
 * @param buffer buffer to fill
 * @param value value to fill
 * @param offset offset at which to start filling buffer
 * @param limit amount of values to fill
 */
template <class T>
void write(std::vector<T> &buffer, uint8_t value, size_t offset, size_t limit) {
  for (size_t i = offset; i < offset + limit; ++i) {
    buffer[i] = value;
  }
}

namespace file {

/**
 * Cross-platform method to open file while creating non-existing
 * directories in the given path.
 *
 * @param path path of the file to open
 * @param mode open mode
 * @returns opened file stream object
 */
std::fstream open(std::string path, std::ios_base::openmode mode);

} // namespace file

} // namespace persist
#endif /* UTILITY_HPP */
