/**
 * wal.hpp - Persist
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
 * @brief The header file contains write ahead logging (WAL) related exceptions.
 *
 */

#ifndef PERSIST_CORE_EXCEPTIONS_WAL_HPP
#define PERSIST_CORE_EXCEPTIONS_WAL_HPP

#include <string>

#include <persist/core/exceptions/base.hpp>

namespace persist {

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

} // namespace persist

#endif /* PERSIST_CORE_EXCEPTIONS_WAL_HPP */
