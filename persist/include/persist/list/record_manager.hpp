/**
 * record_manager.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
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

#ifndef PERSIST_LIST_RECORD_MANAGER_HPP
#define PERSIST_LIST_RECORD_MANAGER_HPP

#include <persist/core/common.hpp>
#include <persist/core/page_allocator.hpp>
#include <persist/core/record_manager.hpp>

namespace persist {

/**
 * @brief List collection record manager.
 *
 * @tparam RecordType Record type stored in the list.
 * @tparam PageAllocatorType Type of page allocator.
 */
template <class RecordType, class PageAllocatorType = PageAllocator<>>
class ListRecordManager : public RecordManager<RecordType> {
  PERSIST_PRIVATE
  /**
   * @brief Reference to page allocator.
   *
   */
  PageAllocatorType &page_allocator;

public:
};

} // namespace persist

#endif /* PERSIST_LIST_RECORD_MANAGER_HPP */
