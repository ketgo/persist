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

#ifndef CORE_DEFS_HPP
#define CORE_DEFS_HPP

#include <vector>

/**
 * Used for intrusive testing
 *
 */
#ifdef PERSIST_TESTING
#define PERSIST_PRIVATE public
#else
#define PERSIST_PRIVATE private:
#endif

/**
 * Global Constants
 */
// Minimum allowed page size
#define MINIMUM_PAGE_SIZE 512
// Default page size
#define DEFAULT_AGE_SIZE 1024

namespace persist {

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
 * @brief Byte buffer type
 */
typedef uint8_t Byte;
typedef struct {
  Byte *start;
  size_t size;
} Span;
typedef std::vector<Byte> ByteBuffer;

} // namespace persist

#endif /* CORE_DEFS_HPP */