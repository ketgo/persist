/**
 * persist.hpp - Persist
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
 * The file contains definition of the Persist class used by package users for
 * saving data records on backend storage.
 */

#ifndef PERSIST_HPP
#define PERSIST_HPP

#include <cstdint>
#include <string>

namespace persist {

/**
 * Record unique identifer type
 */
typedef uint64_t RecordId;

/**
 * Persist Class
 *
 * This class exposes user facing methods for operating on data records
 * storage in the backend storage. The class supports the standard GET,
 * INSERT, UPDATE and REMOVE operations while complying with ACID (Atomic,
 * Consistent, Isolated and Durable) requirements.
 */
class Persist {
public:
  RecordId insert(std::string data);
  void update(RecordId id, std::string data);
  void remove(RecordId id);
};

} // namespace persist

#endif /* PERSIST_HPP */
