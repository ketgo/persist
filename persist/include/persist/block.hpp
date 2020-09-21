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
#include <unordered_map>
#include <vector>

#include <persist/common.hpp>
#include <persist/record.hpp>

namespace persist {

#define BLOCK_SIZE 1024

/**
 * Data block identifier type
 */
typedef uint64_t DataBlockId;

/**
 * Data Block Header Class
 *
 * Header data type for Data Block. It contains
 * the metadata information for facilitating read
 * write operations of records on the block.
 */
class DataBlockHeader : public Serializable {
public:
  DataBlockId blockId; //<- block identifier
  uint64_t tail;       //<- starting index of the free space in block
  /**
   * Data Block Header Record Entries
   *
   * Contains location information of records stored in the block.
   */
  struct Entry {
    uint64_t offset; //<- location offset from end of block
    uint64_t size;   //<- size of record stored
  };
  std::vector<Entry> entries; //<- block entries

  /**
   * Constructors
   */
  DataBlockHeader() {}
  DataBlockHeader(DataBlockId blockId) : blockId(blockId), tail(BLOCK_SIZE) {}

  void load(std::vector<uint8_t> &input) override;
  void dump(std::vector<uint8_t> &output) override;
};

/**
 * Data Block Class
 *
 * A block is a unit chunk of data upon which atomic operations are performed
 * in a transaction. The backend storage is divided into a contiguous set of
 * data blocks. This increases IO performance. Each block contains a collection
 * of data records.
 */
class DataBlock : public Serializable {
private:
  DataBlockHeader header;
  std::unordered_map<RecordId, DataRecord>
      records; //<- collection of records stored in the block

public:
  /**
   * Constructor
   */
  DataBlock() {}
  DataBlock(DataBlockId blockId) : header(blockId) {}

  void load(std::vector<uint8_t> &input) override;
  void dump(std::vector<uint8_t> &output) override;
};

} // namespace persist

#endif /* BLOCK_HPP */
