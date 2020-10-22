/**
 * list.hpp - Persist
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

#ifndef LIST_HPP
#define LIST_HPP

#include <persist/core/collection.hpp>
#include <persist/core/defs.hpp>

namespace persist {
/**
 * @brief Typedef for record location on backend storage
 */
typedef RecordBlock::Location RecordLocation;

/**
 * List Collection
 *
 * Lists are an sequence collection of elements. A double linked list is
 * implemented for this collection.
 */
class List : public Collection {
  PERSIST_PRIVATE

  /**
   * @brief Linked List Node
   *
   * The node stores the record data in bytes and the linkage between the next
   * and prior node.
   */
  struct Node {
    /**
     * @brief Next linked node
     */
    RecordLocation next;
    /**
     * @brief Previous linked node
     */
    RecordLocation previous;

    /**
     * @brief Record data as byte buffer
     */
    ByteBuffer record;

    /**
     * Load Node object from byte string.
     *
     * @param input input buffer to load
     */
    void load(ByteBuffer &input);

    /**
     * Dump Node object as byte string.
     *
     * @param output output buffer to dump
     */
    void dump(ByteBuffer &output);
  };

public:
  /**
   * Iterator Class
   *
   * The list iterator points to an element in the container and has the ability
   * to iterate through the container using the exposed set of operators.
   */
  class Iterator {
  public:
    typedef Iterator self_type;
    typedef ByteBuffer value_type;
    typedef ByteBuffer &reference;
    typedef ByteBuffer *pointer;
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef ptrdiff_t difference_type;

    /**
     * @brief Construct a new Iterator object
     */
    Iterator() {}
    Iterator(List *list, RecordLocation location);

    /**
     * @brief PREFIX increment operator
     */
    self_type &operator++() {
      location = node.next;
      loadNode();
      return *this;
    }

    /**
     * @brief POSTFIX increment operator
     */
    self_type operator++(int) {
      self_type i = *this;
      ++*this;
      return i;
    }

    /**
     * @brief PREFIX decrement operator
     */
    self_type &operator--() {
      location = node.previous;
      loadNode();
      return *this;
    }

    /**
     * @brief POSTFIX decrement operator
     */
    self_type operator--(int) {
      self_type i = *this;
      --*this;
      return i;
    }

    /**
     * @brief Dereference operator to get record value
     */
    const value_type &operator*() { return node.record; }

    /**
     * @brief Dereference operator to get address of record value
     */
    const value_type *operator->() { return &node.record; }

    /**
     * @brief Equality comparision operator
     */
    bool operator==(const self_type &rhs) { return location == rhs.location; }

    /**
     * @brief Non-equality comparision operator
     */
    bool operator!=(const self_type &rhs) { return location != rhs.location; }

    /**
     * @brief Get record location on backend storage.
     */
    const RecordLocation &getLocation() { return location; }

    PERSIST_PRIVATE
    /**
     * @brief Pointer to the underlying collection
     */
    List *list;

    /**
     * @brief Record location
     */
    RecordLocation location;

    /**
     * @brief Node stored at the above location.
     */
    Node node;

    /**
     * @brief The method loads the node at set location.
     */
    void loadNode();
  };

  /**
   * @brief Construct a new List object
   *
   * @param connectionString url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cacheSize the amount of memory in bytes to use for internal cache.
   */
  List(std::string connectionString) : Collection(connectionString) {}
  List(std::string connectionString, uint64_t cacheSize)
      : Collection(connectionString, cacheSize) {}

  /**
   * @brief Insert record at specified postion in the collection.
   *
   * @param postion constant iterator pointing to the position in the container
   * where the new elements are inserted.
   * @param record data to be inserted.
   * @returns iterator pointing to the inserted element.
   */
  Iterator insert(Iterator postion, ByteBuffer record);
};

} // namespace persist

#endif /* LIST_HPP */
