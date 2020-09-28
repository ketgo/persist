/**
 * utility.cpp - Persist
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

#include <fstream>

#include <persist/utility.hpp>

namespace persist {

void write(ByteBuffer &buffer_1, ByteBuffer &buffer_2, size_t offset) {
  for (size_t i = 0; i < buffer_2.size(); ++i) {
    buffer_1[i + offset] = buffer_2[i];
  }
}

void write(ByteBuffer &buffer, uint8_t value, size_t offset, size_t limit) {
  for (size_t i = offset; i < offset + limit; ++i) {
    buffer[i] = value;
  }
}

namespace file {

std::fstream open(std::string path, std::ios_base::openmode mode) {
  std::fstream file;
  // TODO: Use cross-platform solution for creating sub-directories
  file.open(path.c_str(), mode);

  return file;
}

uint64_t size(std::fstream &file) {
  std::streampos fileSize;
  file.seekg(0, std::ios_base::end);
  fileSize = file.tellg();
  file.seekg(0, std::ios_base::beg);

  return uint64_t(fileSize);
}

void read(std::fstream &file, ByteBuffer &buffer, std::streampos offset) {
  // Get current position of the stream cursor before moving
  std::streampos original = file.tellg();
  file.seekg(offset);
  file.read(reinterpret_cast<char *>(&buffer[0]), buffer.size());
  // Place the moved cursor back to its original postion
  file.seekg(original);
}

void write(std::fstream &file, ByteBuffer &buffer, std::streampos offset) {
  // Get current position of the stream cursor before moving
  std::streampos original = file.tellg();
  file.seekg(offset);
  file.write(reinterpret_cast<char *>(&buffer[0]), buffer.size());
  // Place the moved cursor back to its original postion
  file.seekg(original);
}

} // namespace file

} // namespace persist
