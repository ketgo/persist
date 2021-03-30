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

#ifndef PERSIST_LIST_COLLECTION_HPP
#define PERSIST_LIST_COLLECTION_HPP

#include <persist/core/exceptions/record.hpp>

#include <persist/list/record_manager.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {

/**
 * @brief List collection.
 *
 * @tparam RecordType Record type stored in list.
 *
 */
template <class RecordType> class List {
  // Record should be storage
  static_assert(std::is_base_of<Storable, RecordType>::value,
                "Record must be derived from persist::Storable");

  PERSIST_PRIVATE
  /**
   * @brief Record manager.
   *
   */
  ListRecordManager<RecordType> record_manager;

  /**
   * @brief Linked List Node
   *
   * The node stores the record data in bytes and the linkage between the next
   * and prior node.
   */
  struct Node : public Storable {
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
    RecordType record;

    /**
     * @brief Get the storage size of the storable object.
     *
     * @returns storage size in bytes.
     */
    size_t GetStorageSize() const override {
      return 2 * sizeof(RecordLocation) + record.GetStorageSize();
    }

    /**
     * Load page object from byte string.
     *
     * @param input input buffer span to load
     */
    void Load(Span input) {
      if (input.size < GetStorageSize()) {
        throw RecordParseError();
      }
      // Load bytes
      persist::load(input, next, previous);
      record.Load(input);
    }

    /**
     * Dump page object as byte string.
     *
     * @param output output buffer span to dump
     */
    void Dump(Span output) {
      if (output.size < GetStorageSize()) {
        throw RecordParseError();
      }
      // Load bytes
      persist::dump(output, next, previous);
      record.Dump(output);
    }
  };

public:
  /**
   * Iterator Class
   *
   * The list iterator points to an element in the container and has the ability
   * to iterate through the container using the exposed set of operators.
   */
  class Iterator {
    PERSIST_PRIVATE
    /**
     * @brief Pointer to the underlying collection
     */
    List<RecordType> *list;

    /**
     * @brief Record location
     */
    RecordLocation location;

    /**
     * @brief Node stored at the above location.
     */
    Node node;

    /**
     * @brief The method loads the node at current location.
     */
    void LoadNode();

  public:
    typedef Iterator self_type;
    typedef RecordType value_type;
    typedef RecordType &reference;
    typedef RecordType *pointer;
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef std::ptrdiff_t difference_type;

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
      LoadNode();
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
      LoadNode();
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
    const RecordLocation &GetLocation() { return location; }
  };

  /**
   * @brief Construct a new List object
   *
   * @param connection_string Url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cache_size Amount of memory in bytes to use for internal cache.
   */
  List(std::string connection_string) {}
  List(std::string connection_string, uint64_t cache_size) {}

  /**
   * @brief Insert record at specified postion in the collection.
   *
   * TODO: Use constant iterator when passing position to the method.
   *
   * @param postion Iterator pointing to the position in the container
   * where the new elements are inserted.
   * @param record Constant reference to the record to insert.
   * @returns Iterator pointing to the inserted element.
   */
  Iterator Insert(Iterator postion, const RecordType &record);
};

} // namespace persist

#endif /* PERSIST_LIST_COLLECTION_HPP */
