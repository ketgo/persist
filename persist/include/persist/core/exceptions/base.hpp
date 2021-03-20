/**
 * base.hpp - Persist
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

#ifndef PERSIST_CORE_EXCEPTIONS_BASE_HPP
#define PERSIST_CORE_EXCEPTIONS_BASE_HPP

namespace persist {

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

} // namespace persist

#endif /* PERSIST_CORE_EXCEPTIONS_BASE_HPP */
