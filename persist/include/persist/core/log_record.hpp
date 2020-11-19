/**
 * log_record.hpp - Persist
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

/**
 * @brief Transaction Log Record
 *
 * A log record contains required information to recreate or rollback a
 * transaction on system failures or transaction abort. There are different
 * types of log records corresponding to the different operations performed
 * during a transaction.
 *
 */

#ifndef LOG_RECORD_HPP
#define LOG_RECORD_HPP

#include <persist/core/defs.hpp>
#include <persist/core/record_block.hpp>

namespace persist {

/**
 * @brief Log Record
 *
 * Records used to store operations performed by transactions.
 */
class LogRecord {
  friend class LogManager;  // forward declared
  friend class Transaction; // forward declared

public:
  /**
   * @brief Log Record Header
   *
   * The header contains the metadata information of the record.
   */
  class Header {
  public:
    /**
     * @brief Record sequence number
     */
    SeqNumber seqNumber;

    /**
     * @brief Previous record sequence number with valid values greater than
     * `1`. For the first record this is set to `0`.
     */
    SeqNumber prevSeqNumber;

    /**
     * @brief Transaction ID
     */
    TransactionId transactionId;

    /**
     * @brief Checksum to detect page corruption
     */
    Checksum checksum;

    /**
     * Constructors
     */
    Header(SeqNumber seqNumber = 0, SeqNumber prevSeqNumber = 0,
           TransactionId transactionId = 0)
        : seqNumber(seqNumber), prevSeqNumber(prevSeqNumber),
          transactionId(transactionId), checksum(0) {}

    /**
     * Load record block header from byte string.
     *
     * @param input input buffer span to load
     */
    void load(Span input);

    /**
     * Dump record block header as byte string.
     *
     * @param output output buffer span to dump
     */
    void dump(Span output);

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write log record header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "---- Header ----\n";
      os << "seqNumber: " << header.seqNumber << "\n";
      os << "prevSeqNumber: " << header.prevSeqNumber << "\n";
      os << "transactionId: " << header.transactionId << "\n";
      os << "-----------------";
      return os;
    }
#endif
  };
  /**
   * @brief Different types of log records.
   */
  enum class Type { BEGIN, INSERT, UPDATE, DELETE, COMMIT, ABORT, DONE };

  PERSIST_PRIVATE

  /**
   * @brief Log record header
   *
   */
  Header header;

  /**
   * @brief Log record type
   */
  Type type;

  /**
   * @brief Record Block Location
   */
  RecordBlock::Location location;

  /**
   * @brief Record Blocks
   */
  RecordBlock recordBlockA, recordBlockB;

  /**
   * @brief Computes checksum for record block.
   */
  Checksum _checksum();

public:
  /**
   * Default constructor
   */
  LogRecord(){};

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create BEGIN, COMMIT, ABORT and DONE type log
   * records.
   */
  LogRecord(TransactionId transactionId, SeqNumber prevSeqNumber = 0,
            Type type = Type::BEGIN)
      : header(0, prevSeqNumber, transactionId), type(type) {}

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create INSERT and DELETE type log
   * records.
   */
  LogRecord(TransactionId transactionId, SeqNumber prevSeqNumber, Type type,
            RecordBlock::Location location, RecordBlock recordBlock)
      : header(0, prevSeqNumber, transactionId), type(type), location(location),
        recordBlockA(recordBlock) {}

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create UPDATE type log
   * record.
   */
  LogRecord(TransactionId transactionId, SeqNumber prevSeqNumber, Type type,
            RecordBlock::Location location, RecordBlock oldRecordBlock,
            RecordBlock newRecordBlock)
      : header(0, prevSeqNumber, transactionId), type(type), location(location),
        recordBlockA(oldRecordBlock), recordBlockB(newRecordBlock) {}

  /**
   * @brief Get the Seq Number of log record
   *
   * @returns log record sequence number
   */
  SeqNumber getSeqNumber() { return header.seqNumber; }

  /**
   * Load log record from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input);

  /**
   * Dump log record as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output);

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write log record to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const LogRecord &logRecord) {
    os << "[" << logRecord.header << ", " << uint64_t(logRecord.type) << ", "
       << logRecord.location << ", " << logRecord.recordBlockA << ", "
       << logRecord.recordBlockB << "]";
    return os;
  }
#endif
};

} // namespace persist

#endif /* LOG_RECORD_HPP */
