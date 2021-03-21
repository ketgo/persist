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

#ifndef PERSIST_CORE_FILE_STORAGE_HPP
#define PERSIST_CORE_FILE_STORAGE_HPP

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>

#include <persist/core/exceptions/storage.hpp>
#include <persist/core/page/serializer.hpp>
#include <persist/core/storage/base.hpp>

#include <persist/utility/serializer.hpp>

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
static size_t size(std::fstream &file) {
  std::streampos file_size;
  file.seekg(0, std::ios_base::end);
  file_size = file.tellg();
  file.seekg(0, std::ios_base::beg);

  return size_t(file_size);
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
struct FileHeader : public Storable {
  size_t page_size; //<- page size used in the storage file

  /**
   * @brief Get the storage size of header.
   *
   */
  size_t GetStorageSize() const override { return sizeof(FileHeader); }

  /**
   * Load object from byte string
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    // Load bytes
    persist::load(input, page_size);
  }

  /**
   * Dump object as byte string
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    // Dump bytes
    persist::dump(output, page_size);
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write file header to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const FileHeader &file_header) {
    os << "--------- FileHeader ---------\n";
    os << "Page Size: " << file_header.page_size << "\n";
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
 *
 * @tparam PageType The type of page stored by storage.
 */
template <class PageType> class FileStorage : public Storage<PageType> {
  PERSIST_PRIVATE
  size_t page_size;       //<- page block size
  uint64_t page_count;    //<- number of pages in the storage file
  std::string path;       //<- storage files name with path
  std::fstream data_file; //<- IO file stream for data
  std::fstream fsl_file;  //<- IO file stream for free space list
  static const size_t offset =
      sizeof(FileHeader); //<- Offset after which pages are stored

public:
  /**
   * Constructors
   *
   * The file storage stores data in blocks of fixed size. The size of
   * the blocks can be specified at initiation. In case of an existing
   * storage file the block size stored in its metadata is used.
   *
   * @param path path to storage file
   * @param page_size storage size of data block. Default set to 1024
   */
  FileStorage() {}
  FileStorage(const std::string &path)
      : path(path), page_size(DEFAULT_PAGE_SIZE), page_count(0) {}
  FileStorage(const char *path)
      : path(path), page_size(DEFAULT_PAGE_SIZE), page_count(0) {}
  FileStorage(const std::string &path, uint64_t page_size)
      : path(path), page_size(page_size), page_count(0) {}
  FileStorage(const char *path, uint64_t page_size)
      : path(path), page_size(page_size), page_count(0) {}

  /**
   * Destructor
   */
  ~FileStorage() {
    // Close any/all opened files
    Close();
  }

  /**
   * @brief Get path to storage files
   */
  std::string GetPath() const { return path; }

  /**
   * Opens storage file.
   */
  void Open() override {
    data_file = file::open(path + FILE_STORAGE_DATA_FILE_EXTENTION,
                           std::ios::binary | std::ios::in | std::ios::out);
    fsl_file = file::open(path + FILE_STORAGE_FSL_FILE_EXTENTION,
                          std::ios::binary | std::ios::in | std::ios::out);

    // If file is not empty then set the page size and count using data from
    // file header else write a new file header

    size_t file_size = file::size(data_file);
    FileHeader header;
    ByteBuffer buffer(offset);
    if (file_size != 0) {
      // Load header
      file::read(data_file, buffer, 0);
      header.Load(buffer);
      // Set page size value to that obtained from file header
      // TODO: Maybe we need to log warning or throw exception for incompatible
      // page size
      page_size = header.page_size;
      /**
       * Any incomplete written page to storage will be re-written correctly by
       * the recovery manager since the page_count is set to the flour value of
       * (file_size - headerSize) / page_size
       */
      page_count = (file_size - offset) / page_size;
    } else {
      // Write header
      header.page_size = page_size;
      header.Dump(buffer);
      file::write(data_file, buffer, 0);
    }
  }

  /**
   * Checks if storage file is open
   */
  bool IsOpen() override { return data_file.is_open() && fsl_file.is_open(); }

  /**
   * Closes opened storage file. No operation is performed if
   * no file is opened.
   */
  void Close() override {
    // Close storage file if opened
    data_file.close();
    // Close FSL file if opened
    fsl_file.close();
  }

  /**
   * Remove storage files.
   */
  void Remove() override {
    Close();
    std::remove((path + FILE_STORAGE_DATA_FILE_EXTENTION).c_str());
    std::remove((path + FILE_STORAGE_FSL_FILE_EXTENTION).c_str());
  }

  /**
   * @brief Get page size.
   *
   * @returns page size used in storage
   */
  size_t GetPageSize() override { return page_size; }

  /**
   * @brief Get page count.
   *
   * @returns number of pages in storage
   */
  uint64_t GetPageCount() override { return page_count; }

  /**
   * Reads Page with given identifier from storage file.
   *
   * @param page_id page identifier
   * @returns pointer to requested Page object
   */
  std::unique_ptr<PageType> Read(PageId page_id) override {
    // The page ID and page_size is used to compute the offset of the page in
    // the file.
    size_t page_offset = offset + page_size * (page_id - 1);

    // If offset is negative that means page_id is 0 so return null pointer.
    // This is because page_id of 0 is considered NULL.
    if (page_offset < 0) {
      return nullptr;
    }

    // Check if page offset is greater than equal to the file size.
    if (page_offset >= file::size(data_file)) {
      throw PageNotFoundError(page_id);
    }

    ByteBuffer buffer;
    buffer.resize(page_size);
    file::read(data_file, buffer, page_offset);

    return persist::LoadPage<PageType>(buffer);
  }

  /**
   * Writes Page to storage file.
   *
   * @param page reference to Page object to be written
   */
  void Write(PageType &page) override {
    // The block ID and blockSize is used to compute the offset of the block
    // in the file.
    size_t page_offset = offset + page_size * (page.GetId() - 1);

    // If offset is negative that means blockId is 0 so return. This is because
    // blockId of 0 is considered NULL.
    if (page_offset < 0) {
      throw StorageError("Can not write page with invalid ID.");
    }

    ByteBuffer buffer(page_size);
    persist::DumpPage(page, buffer);
    file::write(data_file, buffer, page_offset);
  }

  /**
   * @brief Allocate a new page in storage. The identifier of the newly created
   * page is returned.
   *
   * @returns identifier of the newly allocated page
   */
  PageId Allocate() override {
    // Increase page count by 1. No need to write an empty page to storage since
    // it will be automatically handled by buffer manager.
    page_count += 1;
    return page_count;
  }

  /**
   * @brief Deallocate page with given identifier.
   *
   * @param page_id identifier of the page to deallocate
   */
  void Deallocate(PageId page_id) override {
    // TODO: No operation performed for now
  }
};

/***************************************************/

} // namespace persist

#endif /* PERSIST_CORE_FILE_STORAGE_HPP */
