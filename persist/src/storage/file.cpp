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

#include <fstream>

#include <persist/exceptions.hpp>
#include <persist/storage/file.hpp>
#include <persist/utility.hpp>

namespace persist {

/***********************
 * File Storage
 **********************/

FileStorage::FileStorage() {}

FileStorage::FileStorage(std::string path)
    : path(path), blockSize(DEFAULT_DATA_BLOCK_SIZE) {
  // Open storage file
  open();
}

FileStorage::FileStorage(const char *path)
    : path(path), blockSize(DEFAULT_DATA_BLOCK_SIZE) {
  // Open storage file
  open();
}

FileStorage::FileStorage(std::string path, uint64_t blockSize)
    : path(path), blockSize(blockSize) {
  // Open storage file
  open();
}

FileStorage::FileStorage(const char *path, uint64_t blockSize)
    : path(path), blockSize(blockSize) {
  // Open storage file
  open();
}

FileStorage::~FileStorage() {
  // Close opened storage file
  close();
}

void FileStorage::open() {
  file = file::open(path, std::fstream::in | std::fstream::out |
                              std::fstream::binary);
}

void FileStorage::close() {
  // Close storage file if opened
  if (file.is_open()) {
    file.close();
  }
}

std::unique_ptr<Storage::MetaData> FileStorage::read() {
  // Open metadata file
  std::fstream metadataFile;
  std::string metadataPath = path + ".metadata";

  metadataFile =
      file::open(metadataPath, std::fstream::in | std::fstream::binary);

  // Check if no metadata file and storage file exists. If so then this is a
  // newly created storage so return null pointer. Otherwise throw storage
  // corrupt error.
  if (!metadataFile.is_open()) {
    if (!file.is_open()) {
      return nullptr;
    }
    throw StorageError(
        "Unable to read storage meatdata. The file may be corrupt.");
  }

  // Read the binary metadata file and check for content size. If no
  // content found then return null pointer otherwise return a serialize
  // MetaData object.
  
  // Stop eating new lines in binary mode!!!
  file.unsetf(std::ios::skipws);
  // get its size:
  std::streampos fileSize;
  metadataFile.seekg(0, std::ios::end);
  fileSize = metadataFile.tellg();
  metadataFile.seekg(0, std::ios::beg);
  // Check if the file is emtpy
  if (fileSize == 0) {
    return nullptr;
  }

  ByteBuffer buffer;
  buffer.resize(fileSize);
  metadataFile.read(reinterpret_cast<char *>(&buffer[0]), fileSize);

  // Create and load MetaData object
  std::unique_ptr<Storage::MetaData> metadataPtr =
      std::make_unique<Storage::MetaData>();
  metadataPtr->load(buffer);

  // Close metadata file
  metadataFile.close();

  return metadataPtr;
}

std::unique_ptr<DataBlock> FileStorage::read(DataBlockId blockId) {
  // The block ID is used to compute the offset of the block in the file.

  return nullptr;
}

void FileStorage::write(Storage::MetaData &metadata) {
  ByteBuffer &buffer = metadata.dump();

  // Open metadata file
  std::fstream metadataFile;
  std::string metadataPath = path + ".metadata";

  metadataFile =
      file::open(metadataPath, std::fstream::trunc | std::fstream::binary);
  metadataFile.write(reinterpret_cast<char *>(&buffer[0]), buffer.size());

  // Close metadata file
  metadataFile.close();
}

void FileStorage::write(DataBlock &block) {
  // The block ID is used to compute the offset of the block in the file.
}

} // namespace persist
