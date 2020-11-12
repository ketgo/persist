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
#include <limits>
#include <random>

#include <persist/core/utility.hpp>

namespace persist {

namespace uuid {

uint64_t generate() {
  std::random_device rd;
  std::mt19937_64 e2(rd());
  std::uniform_int_distribution<uint64_t> dist(
      0, std::numeric_limits<uint64_t>::max());

  return dist(e2);
}

} // namespace uuid

namespace file {

std::fstream open(std::string path, std::ios_base::openmode mode) {
  // TODO: Use cross-platform solution for creating sub-directories
  std::fstream file;
  // Creating file if it does not exist
  file.open(path.c_str(), std::ios::out | std::ios::app);
  file.close();
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
