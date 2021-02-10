/**
 * utility/lock_based/map.hpp - Persist
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

#ifndef UTILITY_LOCK_BASED_MAP_HPP
#define UTILITY_LOCK_BASED_MAP_HPP

namespace persist {

namespace utility {

/**
 * @brief Lock Based Map Class
 *
 * The class implements an efficient thread-safe map which can be accessed
 * concurrently.
 *
 * @tparam KeyType type of key
 * @tparam ValueType type of value
 */
template <class KeyType, class ValueType> class LockBasedMap {
public:
  /**
   * @brief Handle Class
   *
   * Handles are used to safely access values in the map. They are essentially
   * pointers but with access control through a read-write lock. Internally, a
   * handle object holds a raw pointer to a value. It performs the locking and
   * unlocking operations upon construction and destruction respectively. The
   * value can be accessed using the standard `->` operator.
   */
  class Handle {};
};

} // namespace utility

} // namespace persist

#endif /* UTILITY_LOCK_BASED_MAP_HPP */
