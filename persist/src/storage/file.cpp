/**
 * file.cpp - Persist
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
 * Implementation of File Storage
 */

#include <filesystem>
#include <fstream>

#include <persist/storage/file.hpp>

namespace persist {

/*********************
 * Utility Functions
 ********************/

static std::fstream open(std::string path) {
  // TODO: Use cross-platform solution to creating directories
}

/***********************
 * File Storage
 **********************/

FileStorage::FileStorage() {}

FileStorage::FileStorage(std::string path)
    : path(path), blockSize(DEFAULT_DATA_BLOCK_SIZE) {}

FileStorage::FileStorage(const char *path)
    : path(path), blockSize(DEFAULT_DATA_BLOCK_SIZE) {}

FileStorage::FileStorage(std::string path, uint64_t blockSize)
    : path(path), blockSize(blockSize) {}

FileStorage::FileStorage(const char *path, uint64_t blockSize)
    : path(path), blockSize(blockSize) {}

void FileStorage::open() {
  file.open(path.c_str(), std::fstream::in | std::fstream::out);
}

void FileStorage::close() {
  // Close storage file if opened
  if (file.is_open()) {
    file.close();
  }
}

std::unique_ptr<Storage::MetaData> FileStorage::read() {
  std::fstream metadataFile;
  std::string metadataPath = path + ".metadata";

  metadataFile.open(metadataPath.c_str(), std::fstream::in);

  return nullptr;
}

std::unique_ptr<DataBlock> FileStorage::read(DataBlockId blockId) {
  // The block ID is used to compute the offset of the block in the file.

  return nullptr;
}

void FileStorage::write(Storage::MetaData &metadata) {}

void FileStorage::write(DataBlock &block) {
  // The block ID is used to compute the offset of the block in the file.
}

} // namespace persist
