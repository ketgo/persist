/**
 * file_storage.hpp - Persist
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
 * File Storage
 */

#ifndef FILE_STORAGE_HPP
#define FILE_STORAGE_HPP

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>

#include <persist/core/exceptions.hpp>
#include <persist/core/storage/base.hpp>

namespace persist {

/*************************************************************
 * File Low Level Interface
 ************************************************************/

namespace file {
/**
 * Cross-platform method to open file while creating non-existing
 * directories in the given path.
 *
 * @param path path of the file to open
 * @param mode open mode
 * @returns opened file stream object
 */
static std::fstream open(std::string path, std::ios_base::openmode mode) {
  // TODO: Use cross-platform solution for creating sub-directories
  std::fstream file;
  // Creating file if it does not exist
  file.open(path.c_str(), std::ios::out | std::ios::app);
  file.close();
  file.open(path.c_str(), mode);

  return file;
}

/**
 * Get content size of the file
 *
 * @param file opened file stream object
 * @returns size of the file
 */
static uint64_t size(std::fstream &file) {
  std::streampos fileSize;
  file.seekg(0, std::ios_base::end);
  fileSize = file.tellg();
  file.seekg(0, std::ios_base::beg);

  return uint64_t(fileSize);
}

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
static void read(std::fstream &file, ByteBuffer &buffer,
                 std::streampos offset) {
  // Get current position of the stream cursor before moving
  std::streampos original = file.tellg();
  file.seekg(offset);
  file.read(reinterpret_cast<char *>(&buffer[0]), buffer.size());
  // Place the moved cursor back to its original postion
  file.seekg(original);
}

/**
 * Write given ByteBuffer to file starting at specifed offset.
 *
 * @param file opened file stream object
 * @param buffer reference to the ByteBuffer from which data is stored
 * @param offset offset within the file from where to start writing
 */
static void write(std::fstream &file, ByteBuffer &buffer,
                  std::streampos offset) {
  // Get current position of the stream cursor before moving
  std::streampos original = file.tellg();
  file.seekg(offset);
  file.write(reinterpret_cast<char *>(&buffer[0]), buffer.size());
  // Place the moved cursor back to its original postion
  file.seekg(original);
}

} // namespace file

/************************************************************/

/*************************************************************
 * File Storage
 ************************************************************/

/**
 * File Storage Class
 *
 * The class implements Block IO operations for a file stored on
 * a local disk. This is the default storage used by the package.
 */
template <class PageType> class FileStorage : public Storage<PageType> {
private:
  uint64_t pageSize; //<- page block size
  std::string path;  //<- storage file name with path
  std::fstream file; //<- IO stream for file

public:
  /**
   * Constructors
   *
   * The file storage stores data in blocks of fixed size. The size of
   * the blocks can be specified at initiation. In case of an existing
   * storage file the block size stored in its metadata is used.
   *
   * @param path path to storage file
   * @param pageSize storage size of data block. Default set to 1024
   */
  FileStorage() {}
  FileStorage(std::string path) : path(path), pageSize(DEFAULT_PAGE_SIZE) {}
  FileStorage(const char *path) : path(path), pageSize(DEFAULT_PAGE_SIZE) {}
  FileStorage(std::string path, uint64_t pageSize)
      : path(path), pageSize(pageSize) {}
  FileStorage(const char *path, uint64_t pageSize)
      : path(path), pageSize(pageSize) {}

  /**
   * Destructor
   */
  ~FileStorage() {}

  /**
   * @brief Get file path to storage
   */
  std::string getPath() { return path; }

  /**
   * Opens storage file.
   */
  void open() override {
    file = file::open(path, std::ios::binary | std::ios::in | std::ios::out);
  }

  /**
   * Checks if storage file is open
   */
  bool is_open() override { return file.is_open(); }

  /**
   * Closes opened storage file. No operation is performed if
   * no file is opened.
   */
  void close() override {
    // Close storage file if opened
    if (file.is_open()) {
      file.close();
    }
  }

  /**
   * Remove storage files.
   */
  void remove() override {
    close();
    std::remove(path.c_str());
    std::string metadataPath = path + ".metadata";
    std::remove(metadataPath.c_str());
  }

  /**
   * Read metadata information from storage file
   *
   * @return pointer to MetaData object
   */
  std::unique_ptr<MetaData> read() override {
    // Open metadata file
    std::fstream metadataFile;
    std::string metadataPath = path + ".metadata";
    std::unique_ptr<MetaData> metadataPtr = std::make_unique<MetaData>();
    // Set page size value in metadata. This gets updated once the content of
    // the saved metadata is loaded. If not saved metadata is found then this
    // value is used.
    metadataPtr->pageSize = pageSize;

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
    metadataPtr->load(Span(buffer));
    pageSize = metadataPtr->pageSize;

    // Close metadata file
    metadataFile.close();

    return metadataPtr;
  }

  /**
   * Write MetaData object to storage file.
   *
   * @param metadata reference to MetaData object to be written
   */
  void write(MetaData &metadata) override {
    ByteBuffer buffer(metadata.size());
    metadata.dump(Span(buffer));

    // Open metadata file
    std::fstream metadataFile;
    std::string metadataPath = path + ".metadata";

    metadataFile = file::open(metadataPath, std::ios::out | std::ios::trunc |
                                                std::ios::binary);
    file::write(metadataFile, buffer, 0);

    // Close metadata file
    metadataFile.close();
  }

  /**
   * Reads Page with given identifier from storage file.
   *
   * @param pageId page identifier
   * @returns pointer to requested Page object
   */
  std::unique_ptr<PageType> read(PageId pageId) override {
    // The page ID and pageSize is used to compute the offset of the page in
    // the file.
    uint64_t offset = pageSize * (pageId - 1);

    // If offset is negative that means pageId is 0 so return null pointer.
    // This is because blockId of 0 is considered NULL.
    if (offset < 0) {
      return nullptr;
    }

    ByteBuffer buffer;
    buffer.resize(pageSize);
    std::unique_ptr<PageType> page =
        std::make_unique<PageType>(pageId, pageSize);

    // Load data block from file
    file::read(file, buffer, offset);

    // TODO: Needs more selective exception handling. The page not found error
    // should be thrown only if the offset exceeds EOF
    try {
      page->load(Span(buffer));
    } catch (...) {
      throw PageNotFoundError(pageId);
    }

    return page;
  }

  /**
   * Writes Page to storage file.
   *
   * @param page reference to Page object to be written
   */
  void write(PageType &page) override {
    // The block ID and blockSize is used to compute the offset of the block
    // in the file.
    uint64_t offset = pageSize * (page.getId() - 1);

    // If offset is negative that means blockId is 0 so return. This is because
    // blockId of 0 is considered NULL.
    if (offset < 0) {
      throw StorageError("Can not write page with invalid ID.");
    }

    ByteBuffer buffer(pageSize);
    page.dump(Span(buffer));
    file::write(file, buffer, offset);
  }
};

/***************************************************/

} // namespace persist

#endif /* FILE_STORAGE_HPP */
