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

#ifndef PERSIST_CORE_EXCEPTIONS_HPP
#define PERSIST_CORE_EXCEPTIONS_HPP

#include <exception>
#include <string>

#include <persist/core/defs.hpp>

// TODO: Too many exception classes in one file. Move them into seprate files
// according to the associated componenets.

namespace persist {

// --------------------------------------------------
// Base Excpetion Classes
// --------------------------------------------------

/**
 * @brief Persist package base exception class
 *
 * Use this to capture all package excpetions.
 */
class PersistException : public std::exception {};

/**
 * @brief Data corruption base exception class
 *
 * Use this to capture all corruption exceptions.
 */
class CorruptException : public PersistException {};

/**
 * @brief Data parsing base exception class
 *
 * Use this to capture all parsing exceptions.
 */
class ParseException : public PersistException {};

/**
 * @brief Not found error base excpetion class
 *
 * Use this to capture all not found exceptions.
 */
class NotFoundException : public PersistException {};

// --------------------------------------------------

// --------------------------------------------------
// Derived Exception Classes
// --------------------------------------------------

/**
 * Free Space List Parsing Error
 *
 * This error is thrown if unable to parse free space list.
 */
class FSLParseError : public ParseException {
private:
  std::string msg;

public:
  FSLParseError() : msg("FSL parsing error.") {}
  FSLParseError(const char *msg) : msg(msg) {}
  FSLParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Free Space List Corrupt Error
 *
 * This error is thrown if the loaded free space list is corrupt.
 */
class FSLCorruptError : public CorruptException {
private:
  std::string msg;

public:
  FSLCorruptError() : msg("FSL corrupt error.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Record Not Found Error
 *
 * This error is thrown if a record is not found.
 */
class RecordNotFoundError : public NotFoundException {
private:
  std::string msg;

public:
  RecordNotFoundError() : msg("Record not found.") {}
  RecordNotFoundError(const char *msg) : msg(msg) {}
  RecordNotFoundError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Record Corrupt Error
 *
 * This error is thrown if the loaded record is corrupt.
 */
class RecordCorruptError : public CorruptException {
private:
  std::string msg;

public:
  RecordCorruptError() : msg("Record corrupt error.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Collection Not Open Error
 *
 * This error is thrown if the record collection is not opened.
 */
class CollectionNotOpenError : public PersistException {
private:
  std::string msg;

public:
  CollectionNotOpenError() : msg("Collection not opened.") {}

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
 * Buffer Manager Error
 *
 * This error is thrown by the buffer manager.
 */
class BufferManagerError : public PersistException {
private:
  std::string msg;

public:
  BufferManagerError(const char *msg) : msg(msg) {}
  BufferManagerError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Log Record Parsing Error
 *
 * This error is thrown if unable to parse log record.
 */
class LogRecordParseError : public ParseException {
private:
  std::string msg;

public:
  LogRecordParseError() : msg("Log record parsing error.") {}
  LogRecordParseError(const char *msg) : msg(msg) {}
  LogRecordParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Log Record Corrupt Error
 *
 * This error is thrown if the loaded log record is corrupt.
 */
class LogRecordCorruptError : public CorruptException {
private:
  std::string msg;

public:
  LogRecordCorruptError() : msg("Log record corrupt error.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Slot Parsing Error
 *
 * This error is thrown if unable to parse page slot.
 */
class PageSlotParseError : public ParseException {
private:
  std::string msg;

public:
  PageSlotParseError() : msg("Page slot parsing error.") {}
  PageSlotParseError(const char *msg) : msg(msg) {}
  PageSlotParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Slot Corrupt Error
 *
 * This error is thrown if the loaded page slot is corrupt.
 */
class PageSlotCorruptError : public CorruptException {
private:
  std::string msg;

public:
  PageSlotCorruptError() : msg("Page slot corrupt error.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Slot Not Found Error
 *
 * This error is thrown when a page slot does not exists inside
 * a page.
 */
class PageSlotNotFoundError : public NotFoundException {
private:
  std::string msg;

public:
  PageSlotNotFoundError(PageId page_id, PageSlotId slot_id)
      : msg(std::string("Page slot '") + std::to_string(slot_id) +
            std::string("' in page with ID '") + std::to_string(page_id) +
            std::string("' not found.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Type Not Found Error
 *
 * This error is thrown if page type is not found in page factory.
 */
class PageTypeNotFoundError : public NotFoundException {
private:
  std::string msg;

public:
  PageTypeNotFoundError(PageTypeId &page_type_id)
      : msg(std::string("Page type with PageTypeID '") +
            std::to_string(page_type_id) +
            std::string("' not found. Please make sure the page is registered "
                        "with PageFactory.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Parsing Error
 *
 * This error is thrown if unable to parse page.
 */
class PageParseError : public ParseException {
private:
  std::string msg;

public:
  PageParseError() : msg("Page parsing error.") {}
  PageParseError(const char *msg) : msg(msg) {}
  PageParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Corrupt Error
 *
 * This error is thrown if the loaded page is corrupt.
 */
class PageCorruptError : public CorruptException {
private:
  std::string msg;

public:
  PageCorruptError() : msg("Page corrupt error.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Page Not Found Error
 *
 * This error is thrown if page is not found.
 */
class PageNotFoundError : public NotFoundException {
private:
  std::string msg;

public:
  PageNotFoundError(PageId &page_id)
      : msg(std::string("Page with ID '") + std::to_string(page_id) +
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
  PageSizeError(size_t &pageSize)
      : msg(std::string("Page size '") + std::to_string(pageSize) +
            std::string("' less then minimum required size of '") +
            std::to_string(MINIMUM_PAGE_SIZE) + std::string("'.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * MetaData Parsing Error
 *
 * This error is thrown if unable to parse storage metadata.
 */
class MetaDataParseError : public ParseException {
private:
  std::string msg;

public:
  MetaDataParseError() : msg("MetaData parsing error.") {}
  MetaDataParseError(const char *msg) : msg(msg) {}
  MetaDataParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * MetaData Corrupt Error
 *
 * This error is thrown if the loaded metadata is corrupt.
 */
class MetaDataCorruptError : public CorruptException {
private:
  std::string msg;

public:
  MetaDataCorruptError() : msg("Metadata corrupt error.") {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * MetaDataDelta Parsing Error
 *
 * This error is thrown if unable to parse storage metadata delta.
 */
class MetaDataDeltaParseError : public ParseException {
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
 * This error is thrown by backend storage.
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

// --------------------------------------------------

} // namespace persist

#endif /* PERSIST_CORE_EXCEPTIONS_HPP */
