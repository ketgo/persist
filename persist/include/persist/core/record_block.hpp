/**
 * record.hpp - Persist
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

#ifndef RECORD_HPP
#define RECORD_HPP

#include <cstdint>
#include <string>

#include <persist/core/common.hpp>

namespace persist {

/**
 * Record Block Class
 *
 * The class represents a single chunk of a data record stored in backend
 * storage. A data record can consist of one or more RecordBlock objects
 * with same identifier.
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
    RecordBlockId blockId; //<- record identifier

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
   * Get storage size of record block.
   */
  uint64_t size();

  /**
   * Load RecordBlock object from byte string.
   *
   * @param input input buffer to load
   */
  void load(ByteBuffer &input) override;

  /**
   * Dump RecordBlock object as byte string.
   *
   * @returns reference to the buffer with results
   */
  ByteBuffer &dump() override;
};

} // namespace persist

#endif /* RECORD_HPP */
