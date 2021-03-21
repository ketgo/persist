/**
 * page.hpp - Persist
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
 * @brief The header file contains page related exceptions.
 *
 */

#ifndef PERSIST_CORE_EXCEPTIONS_PAGE_HPP
#define PERSIST_CORE_EXCEPTIONS_PAGE_HPP

#include <string>

#include <persist/core/exceptions/base.hpp>

namespace persist {

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

} // namespace persist

#endif /* PERSIST_CORE_EXCEPTIONS_PAGE_HPP */
