/**
 * metadata.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
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
 * @brief The header file contains collection metadata related exceptions.
 *
 */

#ifndef PERSIST__CORE__EXCEPTIONS__METADATA_HPP
#define PERSIST__CORE__EXCEPTIONS__METADATA_HPP

#include <string>

#include <persist/core/exceptions/base.hpp>

namespace persist {

/**
 * Metadata Parsing Error
 *
 * This error is thrown if unable to parse metadata.
 */
class MetadataParseError : public ParseException {
private:
  std::string msg;

public:
  MetadataParseError() : msg("Metadata parsing error.") {}
  MetadataParseError(const char *msg) : msg(msg) {}
  MetadataParseError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Metadata Setup Error
 *
 * This error is thrown upon invalid setup of metadata.
 */
class MetadataSetupError : public PersistException {
private:
  std::string msg;

public:
  MetadataSetupError() : msg("Metadata invalid setup.") {}
  MetadataSetupError(const char *msg) : msg(msg) {}
  MetadataSetupError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

/**
 * Metadata Not Found Error
 *
 * This error is thrown when metadata not found.
 */
class MetadataNotFoundError : public NotFoundException {
private:
  std::string msg;

public:
  MetadataNotFoundError() : msg("Metadata not found error.") {}
  MetadataNotFoundError(const char *msg) : msg(msg) {}
  MetadataNotFoundError(std::string &msg) : msg(msg) {}

  const char *what() const throw() { return msg.c_str(); }
};

} // namespace persist

#endif /* PERSIST__CORE__EXCEPTIONS__METADATA_HPP */
