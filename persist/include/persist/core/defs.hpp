/**
 * defs.hpp - Persist
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
 * The file contains common defines using throughout the core components of the
 * package.
 */

#ifndef PERSIST_CORE_DEFS_HPP
#define PERSIST_CORE_DEFS_HPP

#include <cstddef>
#include <cstdint>

#ifdef __PERSIST_DEBUG__
#include <ostream>
#endif

/**
 * Used for intrusive testing
 *
 */
#ifdef PERSIST_INTRUSIVE_TESTING
#define PERSIST_PRIVATE public:
#define PERSIST_PROTECTED public:
#else
#define PERSIST_PRIVATE private:
#define PERSIST_PROTECTED protected:
#endif

/**
 * Global Constants
 */

// Minimum user defined page type identifer value. All values below this number
// are for package use.
#define MINIMUM_PAGE_TYPE_ID 24

// Minimum allowed page size in bytes
#define MINIMUM_PAGE_SIZE 512
// Default page size in bytes
#define DEFAULT_PAGE_SIZE 1024
// Default buffer size. This is the maximum number of pages the buffer can
// load in-memory.
#define DEFAULT_BUFFER_SIZE 1024

// Default log page size in bytes
#define DEFAULT_LOG_PAGE_SIZE 1024
// Default log buffer size. This is the default maximum number of log pages the
// log buffer can load in-memory.
#define DEFAULT_LOG_BUFFER_SIZE 8
// Default FSL buffer size. This is the default maximum number of FSL pages the
// FSLManager can load in-memory.
#define DEFAULT_FSL_BUFFER_SIZE 8

// Backend data storage extension
#define DATA_STORAGE_EXTENTION ".stg"
// Backend free space manager storage extension
#define FSM_STORAGE_EXTENTION ".fsm"

namespace persist {

/**
 * @brief Enumerated list of data operations that can be performed.
 *
 */
enum class Operation { READ, INSERT, UPDATE, DELETE };

/**
 * Page identifier type
 *
 * NOTE: An ID with value 0 is considered NULL
 */
typedef uint64_t PageId;

/**
 * Page slot identifier type
 */
typedef uint64_t PageSlotId;

/**
 * Checksum type
 */
typedef uint64_t Checksum;

/**
 * @brief Transaction ID type
 */
typedef uint64_t TransactionId;

/**
 * @brief Log Sequence Number Type
 */
typedef uint64_t SeqNumber;

} // namespace persist

#endif /* PERSIST_CORE_DEFS_HPP */
