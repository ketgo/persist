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

#include <persist/core/defs.hpp>

namespace persist {
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

/**
 * Get content size of the file
 *
 * @param file opened file stream object
 * @returns size of the file
 */
uint64_t size(std::fstream &file);

/**
 * Read file content starting at given postion into a ByteBuffer. The amount of
 * data read is determined by the size of the passed ByteBuffer.
 *
 * Note:
 * - If the size of the ByteBuffer is zero then no data will be read.
 * - The content of the ByteBuffer will be overwritten
 *
 * @param file opened file stream object
 * @param buffer reference to the ByteBuffer where read data is stored
 * @param offset offset within the file from where to start reading
 */
void read(std::fstream &file, ByteBuffer &buffer, std::streampos offset);

/**
 * Write given ByteBuffer to file starting at specifed offset.
 *
 * @param file opened file stream object
 * @param buffer reference to the ByteBuffer from which data is stored
 * @param offset offset within the file from where to start writing
 */
void write(std::fstream &file, ByteBuffer &buffer, std::streampos offset);

} // namespace file

} // namespace persist
#endif /* UTILITY_HPP */
