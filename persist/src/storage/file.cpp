/**
 * storage/file.cpp - Persist
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

#include "utility.hpp"

#include <persist/exceptions.hpp>
#include <persist/storage/file.hpp>

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
  file = file::open(path, std::ios::binary | std::ios::in | std::ios::out);
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
  std::unique_ptr<Storage::MetaData> metadataPtr =
      std::make_unique<Storage::MetaData>();
  // Set default block size value in metadata. This gets updated once the
  // content of the saved metadata is loaded
  metadataPtr->blockSize = blockSize;

  metadataFile = file::open(metadataPath, std::ios::in | std::ios::binary);

  // Read the binary metadata file and check for content size. If no content
  // found then return pointer to an empty metadata object otherwise return a
  // serialize MetaData object.

  // Stop eating new lines in binary mode!!!
  file.unsetf(std::ios::skipws);
  // get its size:
  uint64_t fileSize = file::size(metadataFile);
  // Check if the file is emtpy
  if (fileSize == 0) {
    return metadataPtr;
  }

  ByteBuffer buffer;
  buffer.resize(fileSize);
  file::read(metadataFile, buffer, 0);

  // Load MetaData object
  metadataPtr->load(buffer);
  blockSize = metadataPtr->blockSize;

  // Close metadata file
  metadataFile.close();

  return metadataPtr;
}

std::unique_ptr<DataBlock> FileStorage::read(DataBlockId blockId) {
  // The block ID and blockSize is used to compute the offset of the block in
  // the file.
  uint64_t offset = blockSize * (blockId - 1);

  // If offset is negative that means blockId is 0 so return null pointer.
  // This is because blockId of 0 is considered NULL.
  if (offset < 0) {
    return nullptr;
  }

  ByteBuffer buffer;
  buffer.resize(blockSize);
  std::unique_ptr<DataBlock> dataBlockPtr =
      std::make_unique<DataBlock>(blockId, blockSize);

  // Load data block from file
  file::read(file, buffer, offset);

  // TODO: Needs more restrictive exception handling. The block not found error
  // should be thrown if the offset exceeds EOF
  try {
    dataBlockPtr->load(buffer);
  } catch (...) {
    throw DataBlockNotFoundError(blockId);
  }

  return dataBlockPtr;
}

void FileStorage::write(Storage::MetaData &metadata) {
  ByteBuffer &buffer = metadata.dump();

  // Open metadata file
  std::fstream metadataFile;
  std::string metadataPath = path + ".metadata";

  metadataFile = file::open(metadataPath,
                            std::ios::out | std::ios::trunc | std::ios::binary);
  file::write(metadataFile, buffer, 0);

  // Close metadata file
  metadataFile.close();
}

void FileStorage::write(DataBlock &block) {
  // The block ID and blockSize is used to compute the offset of the block
  // in the file.
  uint64_t offset = blockSize * (block.getId() - 1);

  // If offset is negative that means blockId is 0 so return. This is because
  // blockId of 0 is considered NULL.
  if (offset < 0) {
    return;
  }

  ByteBuffer &buffer = block.dump();
  file::write(file, buffer, offset);
}

} // namespace persist
