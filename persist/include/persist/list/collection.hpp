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
#include <persist/core/fsm/fsl.hpp>

#include <persist/list/record_manager.hpp>

#include <persist/utility/serializer.hpp>

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
class List {
  PERSIST_PRIVATE
  /**
   * @brief Unique pointer to data storage.
   *
   */
  std::unique_ptr<Storage<RecordPage>> storage;

  /**
   * @brief Buffer manager.
   *
   */
  BufferManager<RecordPage, ReplacerType> buffer_manager;

  /**
   * @brief Free space manager.
   *
   */
  FreeSpaceManagerType fsm;

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
    void Load(Span input) override {
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
    void Dump(Span output) override {
      if (output.size < GetStorageSize()) {
        throw RecordParseError();
      }
      // Load bytes
      persist::dump(output, next, previous);
      record.Dump(output);
    }
  };

  /**
   * @brief Record manager storing linked list of nodes.
   *
   */
  typedef ListRecordManager<Node, ReplacerType, FreeSpaceManagerType>
      RecordManager;
  RecordManager record_manager;

  /**
   * @brief Flag indicating collection is openned.
   *
   */
  bool openned;

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
     * @brief Construct a new Iterator object.
     *
     * @param record_manager Constant reference to record manager.
     * @param location Constant reference to the starting location.
     */
    Iterator() = default;
    Iterator(const RecordManager &record_manager,
             const RecordLocation &location)
        : record_manager(record_manager), location(location) {}

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
      : storage(CreateStorage<RecordPage>(
            ConnectionString(connection_string, DATA_STORAGE_EXTENTION))),
        buffer_manager(storage.get(), cache_size),
        fsm(connection_string, fsm_cache_size),
        record_manager(buffer_manager, fsm), openned(false) {}

  /**
   * @brief Open collection.
   *
   */
  void Open() {
    if (!openned) {
      record_manager.Start();
    }
  }

  /**
   * @brief Close collection.
   *
   */
  void Close() {
    if (openned) {
      record_manager.Stop();
    }
  }

  /**
   * @brief Insert record at specified postion in the collection. This increase
   * the size of the collection by 1.
   *
   * @param postion Constant reference to iterator pointing at the
   * position in the container where the new elements is to be inserted.
   * @param record Constant reference to the record to insert.
   * @returns Iterator pointing to the inserted element.
   */
  Iterator Insert(const Iterator &postion, const RecordType &record);
};

} // namespace persist

#endif /* PERSIST_LIST_COLLECTION_HPP */
