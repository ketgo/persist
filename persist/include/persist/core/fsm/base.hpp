/**
 * fsm/base.hpp - Persist
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

#ifndef PERSIST_CORE_FSM_BASE_HPP
#define PERSIST_CORE_FSM_BASE_HPP

#include <persist/core/defs.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

/**
 * @brief Free Space Manager Base Class
 *
 */
class FreeSpaceManager {
public:
  /**
   * @brief Destroy the Free Space Manager object
   *
   */
  virtual ~FreeSpaceManager() = default;

  /**
   * @brief Start free space manager.
   *
   */
  virtual void Start() = 0;

  /**
   * @brief Stop free space manager.
   *
   */
  virtual void Stop() = 0;

  /**
   * @brief Get ID of page with free space of specified size. Note that the size
   * is treated only as a hint and the manager is free to ignore it. In case no
   * page with free space found then '0' is returned.
   *
   * @param size_hint Desired free space size.
   * @returns Page identifer if a page with free space found else '0'.
   */
  virtual PageId GetPageId(size_t size_hint) = 0;

  /**
   * @brief Manage free space details of specified page.
   *
   * @param page Constant reference to a Page object.
   */
  virtual void Manage(const Page &page) = 0;
};

} // namespace persist

#endif /* PERSIST_CORE_FSM_BASE_HPP */
