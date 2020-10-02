/**
 * data_block.hpp - Persist
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

#ifndef DATA_BLOCK_HPP
#define DATA_BLOCK_HPP

#include <cstdint>
#include <list>
#include <unordered_map>

#include <persist/common.hpp>
#include <persist/record_block.hpp>

#define MINIMUM_DATA_BLOCK_SIZE 256
#define DEFAULT_DATA_BLOCK_SIZE 1024

namespace persist {

/**
 * Data Block identifier type
 *
 * NOTE: An ID with value 0 is considered NULL
 */
typedef uint64_t DataBlockId;

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
   * Header data type for block. It contains the metadata information for
   * facilitating read write operations of data in the block.
   */
  class Header : public Serializable {
  public:
    DataBlockId blockId;     //<- block identifier
    DataBlockId nextBlockId; //<- next block ID in case of data overflow
    DataBlockId prevBlockId; //<- previous block ID in case of data overflow

    uint64_t blockSize; //<- data block storage size

    /**
     * Data Entry
     *
     * Contains location and size information of data stored in the data
     * block.
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
    Header();
    Header(DataBlockId blockId);
    Header(DataBlockId blockId, uint64_t blockSize);

    /**
     * Get storage size of header.
     */
    uint64_t size();

    /**
     * Ending offset of the free space in the block.
     *
     * @returns free space ending offset
     */
    uint64_t tail();

    /**
     * Use up chunk of space of given size from the available free space of
     * the block.
     *
     * @param size amount of space in bytes to occupy
     * @returns pointer to the new entry
     */
    Entry *useSpace(uint64_t size);

    /**
     * Free up used chunk of space occupied by given data entry.
     *
     * @param entry poiter to entry to free
     */
    void freeSpace(Entry *entry);

    /**
     * Load object from byte string
     *
     * @param input input buffer to load
     */
    void load(ByteBuffer &input) override;

    /**
     * Dump object as byte string
     *
     * @returns reference to the buffer with results
     */
    ByteBuffer &dump() override;
  };

private:
  Header header; //<- block header
  bool modified; //<- flag indicating the block has been modified

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
   * Get block ID.
   *
   * @returns block identifier
   */
  DataBlockId &getId() { return header.blockId; }

  /**
   * Get next block ID. This is the ID for the next block when there is data
   * overflow. A value of `0` means there is no next block.
   *
   * @returns next data block identifier
   */
  DataBlockId &getNextBlockId() { return header.nextBlockId; }

  /**
   * Set next block ID. This is the ID for the next block when there is data
   * overflow. A value of `0` means there is no next block.
   *
   * @param blockId next block ID
   */
  void setNextBlockId(DataBlockId blockId) { header.nextBlockId = blockId; }

  /**
   * Get previous block ID. This is the ID for the previous block when there is
   * data overflow. A value of 0 means there is no previous block.
   *
   * @returns previous data block identifier
   */
  DataBlockId &getPrevBlockId() { return header.prevBlockId; }

  /**
   * Set previous block ID. This is the ID for the previous block when there is
   * data overflow. A value of 0 means there is no previous block.
   *
   * @param blockId previous block ID
   */
  void setPrevBlockId(DataBlockId blockId) { header.prevBlockId = blockId; }

  /**
   * Check if the block has been modified since being read from storage.
   *
   * @returns true if modifed else false
   */
  bool isModified() { return modified; }

  /**
   * Get free space in bytes available in the block.
   *
   * @returns free space available in data block
   */
  uint64_t freeSpace();

  /**
   * Get RecordBlock object with given identifier.
   *
   * @param recordId data record identifier
   * @returns reference to RecordBlock object
   */
  RecordBlock &getRecordBlock(RecordBlockId recordBlockId);

  /**
   * Add RecordBlock object to the block.
   *
   * @param recordBlock recRecordBlock object to be added
   */
  void addRecordBlock(RecordBlock &recordBlock);

  /**
   * Remove RecordBlock object with given identifier from block.
   *
   * @param recordBlockId record block identifier
   */
  void removeRecordBlock(RecordBlockId recordBlockId);

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer to load
   */
  void load(ByteBuffer &input) override;

  /**
   * Dump Block object as byte string.
   *
   * @returns reference to the buffer with results
   */
  ByteBuffer &dump() override;
};

} // namespace persist

#endif /* DATA_BLOCK_HPP */
