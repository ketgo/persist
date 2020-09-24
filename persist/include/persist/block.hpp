/**
 * block.hpp - Persist
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

#ifndef BLOCK_HPP
#define BLOCK_HPP

#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>

#include <persist/common.hpp>

#define DEFAULT_DATA_BLOCK_SIZE 1024

namespace persist {

/**
 * Record Block identifer type
 *
 * NOTE: An ID with value 0 is considered NULL
 */
typedef uint64_t RecordBlockId;

/**
 * Data block identifier type
 *
 * NOTE: An ID with value 0 is considered NULL
 */
typedef uint64_t DataBlockId;

/**
 * Record Block Class
 *
 * The class represents a single chunk of a data record stored in backend
 * storage. A data record consits of collection of recordblock objects. The
 * package stores a data record as linked list of record blocks.
 */
class RecordBlock : public Serializable {
public:
  /**
   * Record Block Header Class
   *
   * Header data type for Record Block. It contains the metadata information for
   * facilitating read write operations of records. Since a record is stored as
   * linked list of record blocks, the header contains this linking information.
   */
  class Header : public Serializable {
  public:
    RecordBlockId blockId;       //<- record identifier
    DataBlockId nextDataBlockId; //<- data block ID containing next record block
    DataBlockId
        prevDataBlockId; //<- data block ID containing previous record block

    /**
     * Constructors
     */
    Header() {}
    Header(RecordBlockId blockId);

    /**
     * Get storage size of header.
     */
    uint64_t size();

    void load(ByteBuffer &input) override;
    ByteBuffer &dump() override;
  };

private:
  Header header; //<- record block header

public:
  std::string data; //<- data contained in the record block

  /**
   * Constructors
   */
  RecordBlock() {}
  RecordBlock(RecordBlockId blockId);
  RecordBlock(RecordBlock::Header &header);

  /**
   * Get record block ID
   */
  RecordBlockId &getId();

  /**
   * Get next data block ID
   */
  DataBlockId &getNextDataBlockId();

  /**
   * Set next data block ID
   */
  void setNextDataBlockId(DataBlockId blockId);

  /**
   * Get previous data block ID
   */
  DataBlockId &getPrevDataBlockId();

  /**
   * Set previous data block ID
   */
  void setPrevDataBlockId(DataBlockId blockId);

  /**
   * Get storage size of record block.
   */
  uint64_t size();

  void load(ByteBuffer &input) override;
  ByteBuffer &dump() override;
};

/**
 * Data Block Class
 *
 * A block is a unit chunk of data upon which atomic operations are performed
 * in a transaction. The backend storage is divided into a contiguous set of
 * data blocks. This increases IO performance. Each block contains a collection
 * of record blocks.
 */
class DataBlock : public Serializable {
public:
  /**
   * Data Block Header Class
   *
   * Header data type for Data Block. It contains the metadata information for
   * facilitating read write operations of data in the block.
   */
  class Header : public Serializable {
  public:
    DataBlockId blockId; //<- block identifier
    uint64_t blockSize;  //<- data block storage size

    /**
     * Data Entry
     *
     * Contains location and size information of data stored in the
     * data block.
     */
    struct Entry {
      uint64_t offset; //<- location offset from end of block
      uint64_t size;   //<- size of stored data
    };
    typedef std::list<Entry> Entries;
    Entries entries; //<- data entries

    /**
     * Constructors
     */
    Header() {}
    Header(DataBlockId blockId);
    Header(DataBlockId blockId, uint64_t blockSize);

    /**
     * Get storage size of header.
     */
    uint64_t size();

    /**
     * @brief Ending offset of the free space in the block
     *
     * @returns free space ending offset
     */
    uint64_t tail();

    /**
     * @brief Use up chunk of space of given size from the available free space
     * of the data block.
     *
     * @param size amount of space in bytes to occupy
     * @returns pointer to the new entry
     */
    Entry *useSpace(uint64_t size);

    /**
     * @brief Free up used chunk of space occupied by given data entry.
     *
     * @param entry poiter to entry to free
     */
    void freeSpace(Entry *entry);

    void load(ByteBuffer &input) override;
    ByteBuffer &dump() override;
  };

private:
  Header header; //<- data block header

  typedef std::unordered_map<RecordBlockId,
                             std::pair<RecordBlock, Header::Entry *>>
      RecordBlockCache;
  RecordBlockCache cache; //<- cached collection of records stored in the block

public:
  /**
   * Constructors
   */
  DataBlock();
  DataBlock(DataBlockId blockId);
  DataBlock(DataBlockId blockId, uint64_t blockSize);

  /**
   * Get data block ID
   */
  DataBlockId &getId();

  /**
   * Get free space in bytes available in the data block
   *
   * @returns free space available in data block
   */
  uint64_t freeSpace();

  /**
   * Get RecordBlock object with given identifier
   *
   * @param recordId data record identifier
   * @returns reference to RecordBlock object
   */
  RecordBlock &getRecordBlock(RecordBlockId recordBlockId);

  /**
   * Add RecordBlock object to the data block
   *
   * @param recordBlock recRecordBlock object to be added
   */
  void addRecordBlock(RecordBlock &recordBlock);

  /**
   * Remove RecordBlock object with given identifier
   *
   * @param recordBlockId record block identifier
   */
  void removeRecordBlock(RecordBlockId recordBlockId);

  void load(ByteBuffer &input) override;
  ByteBuffer &dump() override;
};

} // namespace persist

#endif /* BLOCK_HPP */
