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

#define FILE_STORAGE_DATA_FILE_EXTENTION ".stg"
#define FILE_STORAGE_FSL_FILE_EXTENTION ".fsl"

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
 * @brief File Header
 *
 * The file header contains basic information about the storage file.
 */
struct FileHeader {
  Checksum checksum; //<- storage checksum used to detect corruption
  uint64_t pageSize; //<- page size used in the storage file
  // TODO: Add page type info to header

  /**
   * Load object from byte string
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    // Load bytes
    Byte *pos = input.start;
    std::memcpy((void *)&pageSize, (const void *)pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));
  }

  /**
   * Dump object as byte string
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) {
    // Dump bytes
    Byte *pos = output.start;
    std::memcpy((void *)pos, (const void *)&pageSize, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write file header to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const FileHeader &fileHeader) {
    os << "--------- FileHeader ---------\n";
    os << "Page Size: " << fileHeader.pageSize << "\n";
    os << "----------------------------";
    return os;
  }
#endif
};

/**
 * File Storage Class
 *
 * The class implements Block IO operations for a file stored on
 * a local disk. This is the default storage used by the package.
 */
template <class PageType> class FileStorage : public Storage<PageType> {
  PERSIST_PRIVATE
  uint64_t pageSize;     //<- page block size
  uint64_t pageCount;    //<- number of pages in the storage file
  std::string path;      //<- storage files name with path
  std::fstream dataFile; //<- IO file stream for data
  std::fstream fslFile;  //<- IO file stream for free space list

public:
  static const uint64_t headerSize =
      sizeof(FileHeader); //<- file offset from which pages are stored

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
  FileStorage(const std::string &path)
      : path(path), pageSize(DEFAULT_PAGE_SIZE), pageCount(0) {}
  FileStorage(const char *path)
      : path(path), pageSize(DEFAULT_PAGE_SIZE), pageCount(0) {}
  FileStorage(const std::string &path, uint64_t pageSize)
      : path(path), pageSize(pageSize), pageCount(0) {}
  FileStorage(const char *path, uint64_t pageSize)
      : path(path), pageSize(pageSize), pageCount(0) {}

  /**
   * Destructor
   */
  ~FileStorage() {
    // Close any/all opened files
    close();
  }

  /**
   * @brief Get path to storage files
   */
  std::string getPath() { return path; }

  /**
   * Opens storage file.
   */
  void open() override {
    dataFile = file::open(path + FILE_STORAGE_DATA_FILE_EXTENTION,
                          std::ios::binary | std::ios::in | std::ios::out);
    fslFile = file::open(path + FILE_STORAGE_FSL_FILE_EXTENTION,
                         std::ios::binary | std::ios::in | std::ios::out);

    // If file is not empty then set the page size and count using data from
    // file header else write a new file header
    uint64_t size = file::size(dataFile);
    FileHeader header;
    ByteBuffer buffer(headerSize);
    if (size != 0) {
      // Load header
      file::read(dataFile, buffer, 0);
      header.load(Span(buffer));
      // Set page size value to that obtained from file header
      // TODO: Maybe we need to log warning or throw exception for incompatible
      // page size
      pageSize = header.pageSize;
      /**
       * Any incomplete written page to storage will be re-written correctly by
       * the recovery manager since the pageCount is set to the flour value of
       * (fileSize - headerSize) / pageSize
       */
      pageCount = (size - headerSize) / pageSize;
    } else {
      // Write header
      header.pageSize = pageSize;
      header.dump(Span(buffer));
      file::write(dataFile, buffer, 0);
    }
  }

  /**
   * Checks if storage file is open
   */
  bool is_open() override { return dataFile.is_open() && fslFile.is_open(); }

  /**
   * Closes opened storage file. No operation is performed if
   * no file is opened.
   */
  void close() override {
    // Close storage file if opened
    dataFile.close();
    // Close FSL file if opened
    fslFile.close();
  }

  /**
   * Remove storage files.
   */
  void remove() override {
    close();
    std::remove((path + FILE_STORAGE_DATA_FILE_EXTENTION).c_str());
    std::remove((path + FILE_STORAGE_FSL_FILE_EXTENTION).c_str());
  }

  /**
   * @brief Get page size.
   *
   * @returns page size used in storage
   */
  uint64_t getPageSize() override { return pageSize; }

  /**
   * @brief Get page count.
   *
   * @returns number of pages in storage
   */
  uint64_t getPageCount() override { return pageCount; }

  /**
   * Read free space list from storage. If no free list is found then pointer to
   * an empty FSL object is returned.
   *
   * @return pointer to FSL object
   */
  std::unique_ptr<FSL> read() override {
    // Read the binary FSL file and check for content size. If no content
    // found then return pointer to an empty FSL object otherwise return a
    // serialize MetaData object.

    std::unique_ptr<FSL> fslPtr = std::make_unique<FSL>();
    uint64_t size = file::size(fslFile);

    // Check if the file is emtpy
    if (size != 0) {
      // Load FSL object
      ByteBuffer buffer;
      buffer.resize(size);
      file::read(fslFile, buffer, 0);
      fslPtr->load(Span(buffer));
    }

    return fslPtr;
  }

  /**
   * Write FSL object to storage.
   *
   * @param fsl reference to FSL object to be written
   */
  void write(FSL &fsl) override {
    ByteBuffer buffer(fsl.size());
    fsl.dump(Span(buffer));
    file::write(fslFile, buffer, 0);
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
    uint64_t offset = headerSize + pageSize * (pageId - 1);

    // If offset is negative that means pageId is 0 so return null pointer.
    // This is because blockId of 0 is considered NULL.
    if (offset < 0) {
      return nullptr;
    }

    ByteBuffer buffer;
    buffer.resize(pageSize);
    file::read(dataFile, buffer, offset);

    // TODO: Needs more selective exception handling. The page not found error
    // should be thrown only if the offset exceeds EOF
    std::unique_ptr<PageType> page =
        std::make_unique<PageType>(pageId, pageSize);
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
    uint64_t offset = headerSize + pageSize * (page.getId() - 1);

    // If offset is negative that means blockId is 0 so return. This is because
    // blockId of 0 is considered NULL.
    if (offset < 0) {
      throw StorageError("Can not write page with invalid ID.");
    }

    ByteBuffer buffer(pageSize);
    page.dump(Span(buffer));
    file::write(dataFile, buffer, offset);
  }

  /**
   * @brief Allocate a new page in storage. The identifier of the newly created
   * page is returned.
   *
   * @returns identifier of the newly allocated page
   */
  PageId allocate() override {
    // Increase page count by 1. No need to write an empty page to storage since
    // it will be automatically handled by buffer manager.
    pageCount += 1;
    return pageCount;
  }

  /**
   * @brief Deallocate page with given identifier.
   *
   * @param pageId identifier of the page to deallocate
   */
  void deallocate(PageId pageId) override {
    // TODO: No operation performed for now
  }
};

/***************************************************/

} // namespace persist

#endif /* FILE_STORAGE_HPP */
