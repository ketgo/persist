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

#include <fstream>
#include <memory>
#include <string>

#include <persist/core/storage/base.hpp>

namespace persist {
/**
 * File Storage Class
 *
 * The class implements Block IO operations for a file stored on
 * a local disk. This is the default storage used by the package.
 */
class FileStorage : public Storage {
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
  FileStorage();
  FileStorage(std::string path);
  FileStorage(const char *path);
  FileStorage(std::string path, uint64_t pageSize);
  FileStorage(const char *path, uint64_t pageSize);

  /**
   * Destructor
   */
  ~FileStorage();

  /**
   * @brief Get file path to storage
   */
  std::string getPath() { return path; }

  /**
   * Opens storage file.
   */
  void open() override;

  /**
   * Checks if storage file is open
   */
  bool is_open() override;

  /**
   * Closes opened storage file. No operation is performed if
   * no file is opened.
   */
  void close() override;

  /**
   * Read metadata information from storage file
   *
   * @return pointer to MetaData object
   */
  std::unique_ptr<MetaData> read() override;

  /**
   * Write MetaData object to storage file.
   *
   * @param metadata reference to MetaData object to be written
   */
  void write(MetaData &metadata) override;

  /**
   * Reads Page with given identifier from storage file.
   *
   * @param pageId page identifier
   * @returns pointer to requested Page object
   */
  std::unique_ptr<Page> read(PageId pageId) override;

  /**
   * Writes Page to storage file.
   *
   * @param page reference to Page object to be written
   */
  void write(Page &page) override;
};

} // namespace persist

#endif /* FILE_STORAGE_HPP */