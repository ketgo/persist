/**
 * buffer_manager.hpp - Persist
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

#ifndef BUFFER_MANAGER_HPP
#define BUFFER_MANAGER_HPP

#include <persist/core/defs.hpp>
#include <persist/core/page/base.hpp>

// At the minimum 2 pages are needed in memory by record manager.
#define MINIMUM_BUFFER_SIZE 2

namespace persist {

/**
 * @brief Buffer Manager
 *
 * The buffer manager handles buffer of pages loaded in memory from a backend
 * storage. The reading of pages while wrting of modifed pages are perfromed in
 * compliance with the page repleacement policy.
 *
 * @tparam PageType The type of Page object handled by the buffer manager
 */
template <class PageType> class BufferManager {
  static_assert(std::is_base_of<PageType, PageBase>::value,
                "PageType must be derived from PageBase.");
};

} // namespace persist
#endif /* BUFFER_MANAGER_HPP */
