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

#include <persist/core/buffer/replacer/lru_replacer.hpp>
#include <persist/core/collection.hpp>
#include <persist/core/fsm/fsl.hpp>

#include <persist/list/node.hpp>
#include <persist/list/record_manager.hpp>

namespace persist {

/**
 * @brief List collection.
 *
 * @tparam RecordType Record type stored in list.
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 *
 */
template <class RecordType, class ReplacerType = LRUReplacer,
          class FreeSpaceManagerType = FSLManager>
class List
    : public Collection<ReplacerType, FreeSpaceManagerType,
                        ListRecordManager<ListNode<RecordType>, ReplacerType,
                                          FreeSpaceManagerType>> {
  /**
   * @brief Record manager type.
   *
   */
  typedef ListRecordManager<ListNode<RecordType>, ReplacerType,
                            FreeSpaceManagerType>
      RecordManager;

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
     * @brief Reference to the record manager.
     */
    RecordManager &record_manager;

    /**
     * @brief Record location
     */
    RecordLocation location;

    /**
     * @brief Node stored at the above location.
     */
    ListNode<RecordType> node;

    /**
     * @brief Transaction used by iterator.
     *
     */
    Transaction txn;

    /**
     * @brief The method loads the node at current location.
     */
    void LoadNode() { record_manager.Get(node, location, txn); }

  public:
    typedef Iterator self_type;
    typedef RecordType value_type;
    typedef RecordType &reference;
    typedef RecordType *pointer;
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef std::ptrdiff_t difference_type;

    /**
     * @brief Construct a new Iterator object.
     *
     * @param record_manager Constant reference to record manager.
     * @param location Constant reference to the starting location.
     */
    Iterator() = default;
    Iterator(const RecordManager &record_manager,
             const RecordLocation &location, const Transaction &txn)
        : record_manager(record_manager), location(location), txn(txn) {}

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
    bool operator==(const self_type &rhs) const {
      return location == rhs.location;
    }

    /**
     * @brief Non-equality comparision operator
     */
    bool operator!=(const self_type &rhs) const {
      return location != rhs.location;
    }

    /**
     * @brief Get record location on backend storage.
     */
    const RecordLocation &GetRecordLocation() const { return location; }
  };

  /**
   * @brief Construct a new List object
   *
   * @param connection_string Url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cache_size Amount of memory in bytes to use for internal cache.
   * @param fsm_cache_size Amount of memory in bytes to use for internal FSM
   * cache.
   */
  List(const std::string &connection_string,
       size_t cache_size = DEFAULT_BUFFER_SIZE,
       size_t fsm_cache_size = DEFAULT_FSM_BUFFER_SIZE)
      : Collection<ReplacerType, FreeSpaceManagerType, RecordManager>(
            connection_string, cache_size, fsm_cache_size) {}

  /**
   * @brief Insert record at specified postion in the collection. This increase
   * the size of the collection by 1.
   *
   * @param postion Constant reference to iterator pointing at the
   * position in the container where the new elements is to be inserted.
   * @param record Constant reference to the record to insert.
   * @param txn Constant reference to an active transaction.
   * @returns Iterator pointing to the inserted element.
   */
  Iterator Insert(const Iterator &postion, const RecordType &record,
                  const Transaction &txn) {
    // TODO: 1. Insert record to backend storage
    //       2. Update metadata:
    //           2.1 Increase count of elements in list
    //           2.2 Update/Set the location of the last element if changed
    //           2.3 Set the location of the first element if the list is empty
  }
};

} // namespace persist

#endif /* PERSIST_LIST_COLLECTION_HPP */
