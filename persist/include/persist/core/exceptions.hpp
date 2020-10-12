/**
 * exceptions.hpp - Persist
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
 * The header file contains all the exceptions used in the package
 */

#ifndef EXCEPTIONS_HPP
#define EXCEPTIONS_HPP

#include <exception>
#include <string>

#include <persist/core/page.hpp>

namespace persist {

/**
 * @brief Persist Package Base Exception Class
 *
 * Use this to capture all package excpetions.
 */
class PersistException : public std::exception {};

/**
 * Record Not Found Error
 *
 * This error is thrown if a record is not found.
 */
class RecordNotFoundError : public PersistException {
private:
  std::string msg;

public:
  RecordNotFoundError() : msg("Record not found.") {}
  RecordNotFoundError(const char *msg) : msg(msg) {}
  RecordNotFoundError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Record Manager Not Started Error
 *
 * This error is thrown if the record manager is not started.
 */
class RecordManagerNotStartedError : public PersistException {
private:
  std::string msg;

public:
  RecordManagerNotStartedError() : msg("Record manager not started.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Record Block Parsing Error
 *
 * This error is thrown if unable to parse record block.
 */
class RecordBlockParseError : public PersistException {
private:
  std::string msg;

public:
  RecordBlockParseError() : msg("Record block parsing error.") {}
  RecordBlockParseError(const char *msg) : msg(msg) {}
  RecordBlockParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Record Block Not Found Error
 *
 * This error is thrown when a record block does not exists inside
 * a page.
 */
class RecordBlockNotFoundError : public PersistException {
private:
  std::string msg;

public:
  RecordBlockNotFoundError(persist::PageSlotId &slotId)
      : msg(std::string("Record Block not found at slot '") +
            std::to_string(slotId) + std::string("'.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Parsing Error
 *
 * This error is thrown if unable to parse page.
 */
class PageParseError : public PersistException {
private:
  std::string msg;

public:
  PageParseError() : msg("Data block parsing error.") {}
  PageParseError(const char *msg) : msg(msg) {}
  PageParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Not Found Error
 *
 * This error is thrown if page is not found.
 */
class PageNotFoundError : public PersistException {
private:
  std::string msg;

public:
  PageNotFoundError(persist::PageId &pageId)
      : msg(std::string("Data Block '") + std::to_string(pageId) +
            std::string("' not found.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Size Error
 *
 * This error is thrown if page size is less than minimum required size.
 */
class PageSizeError : public PersistException {
private:
  std::string msg;

public:
  PageSizeError(uint64_t &pageSize)
      : msg(std::string("Data Block size '") + std::to_string(pageSize) +
            std::string("' less then minimum required size of '") +
            std::to_string(MINIMUM_PAGE_SIZE) + std::string("'.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * MetaData Parsing Error
 *
 * This error is thrown if unable to parse storage metadata.
 */
class MetaDataParseError : public PersistException {
private:
  std::string msg;

public:
  MetaDataParseError() : msg("MetaData parsing error.") {}
  MetaDataParseError(const char *msg) : msg(msg) {}
  MetaDataParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * MetaDataDelta Parsing Error
 *
 * This error is thrown if unable to parse storage metadata delta.
 */
class MetaDataDeltaParseError : public PersistException {
private:
  std::string msg;

public:
  MetaDataDeltaParseError() : msg("MetaDataDelta parsing error.") {}
  MetaDataDeltaParseError(const char *msg) : msg(msg) {}
  MetaDataDeltaParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Storage Error
 *
 * This error is thrown if unable to open backend storage.
 */
class StorageError : public PersistException {
private:
  std::string msg;

public:
  StorageError() : msg("Unable to open Storage.") {}
  StorageError(const char *msg) : msg(msg) {}
  StorageError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

} // namespace persist

#endif /* EXCEPTIONS_HPP */
