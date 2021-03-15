/**
 * fsl.hpp - Persist
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

#ifndef PERSIST_CORE_FSM_FSL_HPP
#define PERSIST_CORE_FSM_FSL_HPP

#include <cstring>
#include <memory>
#include <set>

#include <persist/core/exceptions.hpp>
#include <persist/core/fsm/_fsl.hpp>
#include <persist/core/fsm/base.hpp>
#include <persist/core/storage/base.hpp>

namespace persist {

/**
 * @brief Free Space List Manager
 *
 * The class manages list of free pages.
 *
 */
class FSLManager : public FreeSpaceManager {
  PERSIST_PRIVATE
  /**
   * @brief Pointer to backend storage.
   *
   */
  Storage *storage;

  /**
   * @brief Free space list object.
   */
  std::unique_ptr<FSL> fsl;

public:
  /**
   * @brief Construct a new FSL object
   *
   * @param buffer
   */
  explicit FSLManager(Storage *storage) : storage(storage) {}

  /**
   * @brief Start free space manager.
   *
   */
  void Start() override { fsl = storage->Read(); }

  /**
   * @brief Stop free space manager.
   *
   */
  void Stop() override {
    // Persist free space list
    storage->Write(*fsl);
  }

  /**
   * @brief Get ID of page with free space. The size hint is ignored and the
   * last recorded page ID in the free list is returned. If the free list is
   * empty then '0' is returned.
   *
   * @param size_hint Desired free space size.
   * @returns Page identifer if a page with free space found else '0'.
   */
  PageId GetPageId(size_t size_hint) override {
    if (fsl->freePages.empty()) {
      return 0;
    }
    return *std::prev(fsl->freePages.end());
  }

  /**
   * @brief Manage free space details of specified page.
   *
   * @param page Constant reference to a Page object.
   */
  void Manage(const Page &page) override {
    PageId page_id = page.GetId();
    // Check if page has free space and update free space list accordingly. Note
    // that since FSL is used to get pages with free space for INSERT page
    // operation, free space for only INSERT is checked.
    if (page.GetFreeSpaceSize(Operation::INSERT) > 0) {
      fsl->freePages.insert(page_id);
    } else {
      fsl->freePages.erase(page_id);
    }
  }
};

} // namespace persist

#endif /* PERSIST_CORE_FSM_FSL_HPP */
