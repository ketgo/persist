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

#include <persist/block.hpp>

namespace persist {

/**
 * Record Block Parsing Error
 *
 * This error is thrown if unable to parse record block.
 */
class RecordBlockParseError : public std::exception {
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
 * a data block.
 */
class RecordBlockNotFoundError : public std::exception {
private:
  std::string msg;

public:
  RecordBlockNotFoundError(persist::RecordBlockId &blockId)
      : msg(std::string("Record Block '") + std::to_string(blockId) +
            std::string("' not found.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Record Block Exists Error
 */
class RecordBlockExistsError : public std::exception {
private:
  std::string msg;

public:
  RecordBlockExistsError(persist::RecordBlockId &blockId)
      : msg(std::string("Record Block '") + std::to_string(blockId) +
            std::string("' exists.")) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Data Block Parsing Error
 *
 * This error is thrown if unable to parse data block.
 */
class DataBlockParseError : public std::exception {
private:
  std::string msg;

public:
  DataBlockParseError() : msg("Data block parsing error.") {}
  DataBlockParseError(const char *msg) : msg(msg) {}
  DataBlockParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * MetaData Parsing Error
 *
 * This error is thrown if unable to parse storage metadata.
 */
class MetaDataParseError : public std::exception {
private:
  std::string msg;

public:
  MetaDataParseError() : msg("MetaData parsing error.") {}
  MetaDataParseError(const char *msg) : msg(msg) {}
  MetaDataParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * File Opening Error
 *
 * This error is thrown if unable to open file.
 */
class FileError : public std::exception {
private:
  std::string msg;

public:
  FileError() : msg("Unable to open file.") {}
  FileError(std::string &file) : msg("Unable to open file: " + file) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Storage Error
 *
 * This error is thrown if unable to open backend storage.
 */
class StorageError : public std::exception {
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
