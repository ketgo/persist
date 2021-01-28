/**
 * log_manager.hpp - Persist
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

#ifndef LOG_MANAGER_HPP
#define LOG_MANAGER_HPP

#include <unordered_map>

#include <persist/core/defs.hpp>
#include <persist/core/recovery/log_record.hpp>

namespace persist {

/**
 * @brief Log Manager Class
 *
 * The log manager handles collection of log records for all transactions.
 */
class LogManager {
  PERSIST_PRIVATE
  /**
   * @brief Sequence number of the latest log record. This is used to set
   * sequence number of the next log record.
   */
  SeqNumber seqNumber;

  /**
   * @brief Log record buffer
   */
  typedef std::unordered_map<SeqNumber, LogRecord> Buffer;
  Buffer buffer;

public:
  /**
   * @brief Construct a new Log Manager object
   *
   * TODO: Obtain sequence number from log records backend storage
   */
  LogManager() : seqNumber(0) {}

  /**
   * @brief Add log record to transaction logs
   *
   * @param logRecord reference to the log record object to add
   * @returns sequence number of the added log record
   */
  SeqNumber add(LogRecord &logRecord) {
    seqNumber++;
    logRecord.header.seqNumber = seqNumber;
    buffer.insert(std::pair<SeqNumber, LogRecord>(seqNumber, logRecord));

    return seqNumber;
  }

  /**
   * @brief Get Log record of given sequence number.
   *
   * @param seqNumber sequence number of the log record to get
   * @returns reference to the log record
   */
  LogRecord &get(SeqNumber seqNumber) { return buffer[seqNumber]; }

  /**
   * @brief Get sequence number.
   *
   * @returns sequence number in the log
   */
  SeqNumber getSeqNumber() { return seqNumber; }
};

} // namespace persist

#endif /* LOG_MANAGER_HPP */
