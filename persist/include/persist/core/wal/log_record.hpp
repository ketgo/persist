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

#ifndef PERSIST_CORE_WAL_LOG_RECORD_HPP
#define PERSIST_CORE_WAL_LOG_RECORD_HPP

#include <persist/core/common.hpp>
#include <persist/core/exceptions/wal.hpp>
#include <persist/core/page/log_page/slot.hpp>
#include <persist/core/page/record_page/slot.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {

/**
 * @brief Log Record
 *
 * Records used to store operations performed by transactions.
 */
class LogRecord : public Storable {
public:
  /**
   * @brief Log record location type
   *
   */
  typedef LogPageSlot::Location Location;

  /**
   * @brief Log Record Header
   *
   * The header contains the metadata information of the record.
   */
  class Header : public Storable {
  public:
    /**
     * @brief Record sequence number
     */
    SeqNumber seq_number;

    /**
     * @brief Previous log record location. This is used to link log records in
     * chronological order.
     */
    Location prev_log_record_location;

    /**
     * @brief Transaction ID
     */
    TransactionId transaction_id;

    /**
     * Constructors
     */
    Header(SeqNumber seq_number = 0, Location prev_log_record_location = {0, 0},
           TransactionId transaction_id = 0)
        : seq_number(seq_number),
          prev_log_record_location(prev_log_record_location),
          transaction_id(transaction_id) {}

    /**
     * @brief Get size of the log record header.
     */
    size_t GetStorageSize() const override { return sizeof(Header); }

    /**
     * Load record block header from byte string.
     *
     * @param input input buffer span to load
     */
    void Load(Span input) override {
      if (input.size < GetStorageSize()) {
        throw LogRecordParseError();
      }
      // Load bytes
      persist::load(input, seq_number, prev_log_record_location,
                    transaction_id);
    }

    /**
     * Dump record block header as byte string.
     *
     * @param output output buffer span to dump
     */
    void Dump(Span output) override {
      if (output.size < GetStorageSize()) {
        throw LogRecordParseError();
      }
      // Dump bytes
      persist::dump(output, seq_number, prev_log_record_location,
                    transaction_id);
    }

    /**
     * @brief Equality comparision operator.
     */
    bool operator==(const Header &other) const {
      return seq_number == other.seq_number &&
             prev_log_record_location == other.prev_log_record_location &&
             transaction_id == other.transaction_id;
    }

    /**
     * @brief Non-equality comparision operator.
     */
    bool operator!=(const Header &other) const {
      return seq_number != other.seq_number ||
             prev_log_record_location != other.prev_log_record_location ||
             transaction_id != other.transaction_id;
    }

#ifdef __PERSIST_DEBUG__
    /**
     * @brief Write log record header to output stream
     */
    friend std::ostream &operator<<(std::ostream &os, const Header &header) {
      os << "---- Header ----\n";
      os << "seq_number: " << header.seq_number << "\n";
      os << "prev_log_record_location: " << header.prev_log_record_location
         << "\n";
      os << "transaction_id: " << header.transaction_id << "\n";
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
    BEGIN = 0, //<- The log record represents begining of a transaction.
    INSERT,    //<- The log record represents insert operation as part of a
               // transaction.
    UPDATE,    //<- The log record represents update operation as part of a
               // transaction.
    DELETE,    //<- The log record represents remove operation as part of a
               // transaction.
    ABORT, //<- The log record represents that a transaction has successfully
           // aborted. This implies that the transaction is in `ABORTED` state.
    COMMIT //<- The log record represents a transaction has successfully
           // comitted.
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
   * @brief Record page slot location
   */
  RecordPageSlot::Location location;

  /**
   * @brief Record page slots
   */
  RecordPageSlot page_slot_a, page_slot_b;

public:
  /**
   * Default constructor
   */
  LogRecord() = default;

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create BEGIN, COMMIT and ABORT type log
   * records.
   */
  LogRecord(TransactionId transaction_id,
            Location prev_log_record_location = {0, 0}, Type type = Type::BEGIN)
      : header(0, prev_log_record_location, transaction_id), type(type) {}

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create INSERT and DELETE type log
   * records.
   */
  LogRecord(TransactionId transaction_id, Location prev_log_record_location,
            Type type, RecordPageSlot::Location location,
            RecordPageSlot pageSlot)
      : header(0, prev_log_record_location, transaction_id), type(type),
        location(location), page_slot_a(pageSlot) {}

  /**
   * @brief Construct a new Log Record object
   *
   * This constructor is used to create UPDATE type log record.
   */
  LogRecord(TransactionId transaction_id, Location prev_log_record_location,
            Type type, RecordPageSlot::Location location,
            RecordPageSlot oldPageSlot, RecordPageSlot newPageSlot)
      : header(0, prev_log_record_location, transaction_id), type(type),
        location(location), page_slot_a(oldPageSlot), page_slot_b(newPageSlot) {
  }

  /**
   * @brief Get the sequence number of log record
   *
   * @returns log record sequence number
   */
  const SeqNumber &GetSeqNumber() const { return header.seq_number; }

  /**
   * @brief Set the sequence number of log record
   *
   * @param seq_number log record sequence number
   */
  void SetSeqNumber(SeqNumber seq_number) { header.seq_number = seq_number; }

  /**
   * @brief Get the previous log record location
   *
   * @returns Consant reference to previous location
   */
  const Location &GetPrevLocation() const {
    return header.prev_log_record_location;
  }

  /**
   * @brief Get the transaction ID of the log record
   *
   * @return Consant reference to previous sequence number
   */
  const TransactionId &GetTransactionId() const {
    return header.transaction_id;
  }

  /**
   * @brief Get the log record type
   *
   * @returns Consant reference to type of log record
   */
  const Type &GetLogType() const { return type; }

  /**
   * @brief Get the page slot location targeted by log record
   *
   * @returns Consant reference to page slot location targeted by log record
   */
  const RecordPageSlot::Location &GetLocation() const { return location; }

  // TODO: Add const qualifier for get page slot methods.

  /**
   * @brief Get the first PageSlot targeted by log record
   *
   * @return Consant reference to page slot
   */
  RecordPageSlot &GetPageSlotA() { return page_slot_a; }

  /**
   * @brief Get the second PageSlot targeted by log record
   *
   * @return Consant reference to page slot
   */
  RecordPageSlot &GetPageSlotB() { return page_slot_b; }

  /**
   * @brief Get size of log record.
   * - sizeof(header)
   * - sizeof(type)
   * - sizeof(location)
   * - page_slot_a.GetSize()
   * - page_slot_b.GetSize()
   */
  size_t GetStorageSize() const override {
    return header.GetStorageSize() + sizeof(type) + sizeof(location) +
           page_slot_a.GetStorageSize() + page_slot_b.GetStorageSize();
  }

  /**
   * Load log record from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < GetStorageSize()) {
      throw LogRecordParseError();
    }
    // Load header
    header.Load(input);
    input += header.GetStorageSize();
    // Load bytes
    persist::load(input, type, location);
    page_slot_a.Load(input);
    input += page_slot_a.GetStorageSize();
    page_slot_b.Load(input);
    input += page_slot_b.GetStorageSize();
  }

  /**
   * Dump log record as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < GetStorageSize()) {
      throw LogRecordParseError();
    }
    // Dump header
    header.Dump(output);
    output += header.GetStorageSize();
    // Dump bytes
    persist::dump(output, type, location);
    page_slot_a.Dump(output);
    output += page_slot_a.GetStorageSize();
    page_slot_b.Dump(output);
    output += page_slot_b.GetStorageSize();
  }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const LogRecord &other) const {
    return header == other.header && type == other.type &&
           location == other.location && page_slot_a == other.page_slot_a &&
           page_slot_b == other.page_slot_b;
  }

  /**
   * @brief Non-equality comparision operator.
   */
  bool operator!=(const LogRecord &other) const {
    return header != other.header || type != other.type ||
           location != other.location || page_slot_a != other.page_slot_a ||
           page_slot_b != other.page_slot_b;
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write log record to output stream
   */
  friend std::ostream &operator<<(std::ostream &os,
                                  const LogRecord &log_record) {
    os << log_record.header << "\nType: " << uint64_t(log_record.type)
       << "\nLocation: " << log_record.location << "\nRecord A: \n"
       << log_record.page_slot_a << "\nRecord B: \n"
       << log_record.page_slot_b;
    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_WAL_LOG_RECORD_HPP */
