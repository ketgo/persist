// lock_based_map - Persist
//
// Copyright 2021 Ketan Goyal
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
  class Handle {
  private:
    /**
     * @brief Pointer to value stored in LockBasedMap.
     */
    ValueType *value;

    /**
     * @brief Flag to indicate handle has access ownership of value.
     *
     */
    bool isOwner;

    /**
     * @brief Unset access ownserhip of value.
     *
     */
    void unset() {
      isOwner = false;
      value = nullptr;
    }

    /**
     * @brief Acquire access ownership of value.
     *
     */
    void acquire() {
      // Lock and pin value
    }

    /**
     * @brief Release access ownership of value.
     *
     */
    void release() {
      if (isOwner) {
        // Unlock and unpin value
      }
    }

  public:
    /**
     * @brief Construct a new Handle object
     *
     * @param value pointer to the value accessed through the handle
     */
    Handle(ValueType *value) : value(value) {}

    /**
     * @brief Move constructor for handle
     */
    Handle(Handle &&other) : value(other.value), isOwner(other.isOwner) {
      // Unset access ownership of the value from the moved handle
      other.unset();
    }

    /**
     * @brief Move assignment operator. This will relese access ownership of the
     * currently owned value.
     */
    Handle &operator=(Handle &&other) {
      // Check for moving of the same handle
      if (this != &other) {
        // Release access ownership of the current value in handle.
        release();

        // Copy members of the moved handle
        value = other.value;
        isOwner = other.isOwner;

        // Unset access ownership of the moved object
        other.unset();
      }

      return *this;
    }

    /**
     * @brief Value access operator
     */
    ValueType *operator->() const { return value; }

    /**
     * @brief Destroy the Handle object
     */
    ~Handle() {
      // Release access ownership of the value
      release();
    }
  };

private:

};

} // namespace utility

} // namespace persist

#endif /* UTILITY_LOCK_BASED_MAP_HPP */
