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
#include <persist/core/exceptions.hpp>
#include <persist/core/page/slotted_page/page_slot.hpp>

namespace persist {

/**
 * @brief Log Record
 *
 * Records used to store operations performed by transactions.
 */
class LogRecord {
  friend class TransactionManager; // forward declared

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
     * `1`. For the first record of a transaction this is set to `0`.
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
     * @brief Get size of the log record header.
     */
    uint64_t size() { return sizeof(Header); }

    /**
     * Load record block header from byte string.
     *
     * @param input input buffer span to load
     */
    void load(Span input) {
      if (input.size < size()) {
        throw LogRecordParseError();
      }

      // Load bytes
      Byte *pos = input.start;
      std::memcpy((void *)&seqNumber, (const void *)pos, sizeof(SeqNumber));
      pos += sizeof(SeqNumber);
      std::memcpy((void *)&prevSeqNumber, (const void *)pos, sizeof(SeqNumber));
      pos += sizeof(SeqNumber);
      std::memcpy((void *)&transactionId, (const void *)pos,
                  sizeof(TransactionId));
      pos += sizeof(TransactionId);
      std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));
    }

    /**
     * Dump record block header as byte string.
     *
     * @param output output buffer span to dump
     */
    void dump(Span output) {
      if (output.size < size()) {
        throw LogRecordParseError();
      }

      // Dump bytes
      Byte *pos = output.start;
      std::memcpy((void *)pos, (const void *)&seqNumber, sizeof(SeqNumber));
      pos += sizeof(SeqNumber);
      std::memcpy((void *)pos, (const void *)&prevSeqNumber, sizeof(SeqNumber));
      pos += sizeof(SeqNumber);
      std::memcpy((void *)pos, (const void *)&transactionId,
                  sizeof(TransactionId));
      pos += sizeof(TransactionId);
      std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Header &other) const {
      return seqNumber == other.seqNumber &&
             prevSeqNumber == other.prevSeqNumber &&
             transactionId == other.transactionId;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Header &other) const {
      return seqNumber != other.seqNumber ||
             prevSeqNumber != other.prevSeqNumber ||
             transactionId != other.transactionId;
    }

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
   * @brief Different types of log records given by the enumeration class. The
   * description for each is as shown in the following comments.
   */
  enum class Type {
    BEGIN,  //<- The log record represents begining of a transaction.
    INSERT, //<- The log record represents insert operation as part of a
            // transaction.
    UPDATE, //<- The log record represents update operation as part of a
            // transaction.
    DELETE, //<- The log record represents remove operation as part of a
            // transaction.
    COMMIT, //<- The log record represents a transaction has been partially
            // comitted. This implies that the transaction is in
            // `PARTIALLY_COMMITTED` state.
    ABORT,  //<- The log record represents that a transaction has successfully
            // aborted. This implies that the transaction is in `ABORTED` state.
    DONE //<- The log record represents a transaction has successfully comitted.
         // This implies that the transaction is in `COMMITTED` state.
  };

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
  PageSlot::Location location;

  /**
   * @brief Record Blocks
   */
  PageSlot pageSlotA, pageSlotB;

  /**
   * @brief Computes checksum for record block.
   */
  Checksum _checksum() {

    // Implemented hash function based on comment in
    // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

    Checksum seed = pageSlotA.size() + pageSlotB.size();

    seed = std::hash<SeqNumber>()(header.seqNumber) + 0x9e3779b9 + (seed << 6) +
           (seed >> 2);
    seed ^= std::hash<SeqNumber>()(header.prevSeqNumber) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<TransactionId>()(header.transactionId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<Type>()(type) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    seed ^= std::hash<PageId>()(location.pageId) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    seed ^= std::hash<PageSlotId>()(location.slotId) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    seed ^= std::hash<size_t>()(pageSlotA.size()) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    seed ^= std::hash<size_t>()(pageSlotB.size()) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);

    return seed;
  }

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
            PageSlot::Location location, PageSlot pageSlot)
      : header(0, prevSeqNumber, transactionId), type(type), location(location),
        pageSlotA(pageSlot) {}

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create UPDATE type log
   * record.
   */
  LogRecord(TransactionId transactionId, SeqNumber prevSeqNumber, Type type,
            PageSlot::Location location, PageSlot oldPageSlot,
            PageSlot newPageSlot)
      : header(0, prevSeqNumber, transactionId), type(type), location(location),
        pageSlotA(oldPageSlot), pageSlotB(newPageSlot) {}

  /**
   * @brief Get the sequence number of log record
   *
   * @returns log record sequence number
   */
  const SeqNumber &getSeqNumber() const { return header.seqNumber; }

  /**
   * @brief Set the sequence number of log record
   *
   * @param seqNumber log record sequence number
   */
  void setSeqNumber(SeqNumber seqNumber) { header.seqNumber = seqNumber; }

  /**
   * @brief Get the previous log record sequence number
   *
   * @returns Consant reference to previous sequence number
   */
  const SeqNumber &getPrevSeqNumber() const { return header.prevSeqNumber; }

  /**
   * @brief Get the transaction ID of the log record
   *
   * @return Consant reference to previous sequence number
   */
  const TransactionId &getTransactionId() const { return header.transactionId; }

  /**
   * @brief Get the log record type
   *
   * @returns Consant reference to type of log record
   */
  const Type &getLogType() const { return type; }

  /**
   * @brief Get the page slot location targeted by log record
   *
   * @returns Consant reference to page slot location targeted by log record
   */
  const PageSlot::Location &getLocation() const { return location; }

  /**
   * @brief Get the first PageSlot targeted by log record
   *
   * @return Consant reference to page slot
   */
  const PageSlot &getPageSlotA() const { return pageSlotA; }

  /**
   * @brief Get the second PageSlot targeted by log record
   *
   * @return Consant reference to page slot
   */
  const PageSlot &getPageSlotB() const { return pageSlotB; }

  /**
   * @brief Get size of log record.
   * - sizeof(header)
   * - sizeof(type)
   * - sizeof(location)
   * - sizeof(pageSlotA.size())
   * - pageSlotA.size()
   * - sizeof(pageSlotB.size())
   * - pageSlotB.size()
   */
  uint64_t size() {
    return header.size() + sizeof(type) + sizeof(location) +
           2 * sizeof(uint64_t) + pageSlotA.size() + pageSlotB.size();
  }

  /**
   * Load log record from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    if (input.size < size()) {
      throw LogRecordParseError();
    }
    // Load header
    header.load(input);

    // Load bytes
    Byte *pos = input.start + header.size();
    std::memcpy((void *)&type, (const void *)pos, sizeof(Type));
    pos += sizeof(Type);
    std::memcpy((void *)&location, (const void *)pos,
                sizeof(PageSlot::Location));
    pos += sizeof(PageSlot::Location);
    uint64_t pageSlotASize;
    std::memcpy((void *)&pageSlotASize, (const void *)pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    pageSlotA.load(Span(pos, pageSlotASize));
    pos += pageSlotASize;
    uint64_t pageSlotBSize;
    std::memcpy((void *)&pageSlotBSize, (const void *)pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    pageSlotB.load(Span(pos, pageSlotBSize));

    // Check for corruption by matching checksum
    if (_checksum() != header.checksum) {
      throw LogRecordCorruptError();
    }
  }

  /**
   * Dump log record as byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) {
    if (output.size < size()) {
      throw LogRecordParseError();
    }

    // Compute and set checksum

    header.checksum = _checksum();
    // Dump header
    header.dump(output);

    // Dump bytes
    Byte *pos = output.start + header.size();
    std::memcpy((void *)pos, (const void *)&type, sizeof(Type));
    pos += sizeof(Type);
    std::memcpy((void *)pos, (const void *)&location,
                sizeof(PageSlot::Location));
    pos += sizeof(PageSlot::Location);
    uint64_t pageSlotASize = pageSlotA.size();
    std::memcpy((void *)pos, (const void *)&pageSlotASize, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    pageSlotA.dump(Span(pos, pageSlotASize));
    pos += pageSlotASize;
    uint64_t pageSlotBSize = pageSlotB.size();
    std::memcpy((void *)pos, (const void *)&pageSlotBSize, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    pageSlotB.dump(Span(pos, pageSlotBSize));
  }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const LogRecord &other) const {
    return header == other.header && type == other.type &&
           location == other.location && pageSlotA == other.pageSlotA &&
           pageSlotB == other.pageSlotB;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const LogRecord &other) const {
    return header != other.header || type != other.type ||
           location != other.location || pageSlotA != other.pageSlotA ||
           pageSlotB != other.pageSlotB;
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write log record to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const LogRecord &logRecord) {
    os << logRecord.header << "\nType: " << uint64_t(logRecord.type)
       << "\nLocation: " << logRecord.location << "\nRecord A: \n"
       << logRecord.pageSlotA << "\nRecord B: \n"
       << logRecord.pageSlotB;
    return os;
  }
#endif
};

} // namespace persist

#endif /* LOG_RECORD_HPP */
