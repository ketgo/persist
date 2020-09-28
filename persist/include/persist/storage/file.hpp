/**
 * file.hpp - Persist
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

#ifndef STORAGE_FILE_HPP
#define STORAGE_FILE_HPP

#include <fstream>
#include <memory>
#include <string>

#include <persist/storage/base.hpp>

namespace persist {
/**
 * File Storage Class
 *
 * The class implements Block IO operations for a file stored on
 * a local disk. This is the default storage used by the package.
 */
class FileStorage : public Storage {
private:
  uint64_t blockSize; //<- data block size
  std::string path;   //<- storage file name with path
  std::fstream file;  //<- IO stream for file

  /**
   * Opens storage file.
   */
  void open();

  /**
   * Closes opened storage file. No operation is performed if
   * no file is opened.
   */
  void close();

public:
  /**
   * Constructors
   *
   * The file storage stores data in blocks of fixed size. The size of
   * the blocks can be specified at initiation. In case of an existing
   * storage file the block size stored in its metadata is used.
   *
   * @param path path to storage file
   * @param blockSize storage size of data block. Default set to 1024
   */
  FileStorage();
  FileStorage(std::string path);
  FileStorage(const char *path);
  FileStorage(std::string path, uint64_t blockSize);
  FileStorage(const char *path, uint64_t blockSize);

  /**
   * Destructor
   */
  ~FileStorage();

  /**
   * Read metadata information from storage file
   *
   * @return pointer to MetaData object
   */
  std::unique_ptr<Storage::MetaData> read() override;

  /**
   * Reads DataBlock with given identifier from storage file.
   *
   * @param blockId block identifier
   * @returns pointer to requested Block object
   */
  std::unique_ptr<DataBlock> read(DataBlockId blockId) override;

  /**
   * Write MetaData object to storage file.
   *
   * @param metadata reference to MetaData object to be written
   */
  void write(Storage::MetaData &metadata) override;

  /**
   * Writes DataBlock to storage file.
   *
   * @param block reference to DataBlock object to be written
   */
  void write(DataBlock &block) override;
};

} // namespace persist

#endif /* STORAGE_FILE_HPP */
