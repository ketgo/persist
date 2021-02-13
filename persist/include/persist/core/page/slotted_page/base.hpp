/**
 * slotted_page/base.hpp - Persist
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

#ifndef SLOTTED_PAGE_BASE_HPP
#define SLOTTED_PAGE_BASE_HPP

#include <persist/core/page/base.hpp>
#include <persist/core/page/slotted_page/page_slot.hpp>
#include <persist/core/transaction/transaction.hpp>

namespace persist {

// TODO: Use move operations in interface for writing page slots in page.

/**
 * @brief Slotted Page Base Class
 *
 * The class exposes interface for slotted types of pages. Such pages are used
 * by collections to store records. A slotted page consists of one or more slots
 * where each slot contains a full or partial data record. Thus a data record is
 * made up of multiple slots present in one or more slotted pages. Since a slot
 * stores part of a record, it is also sometimes referred to as a record block.
 *
 * Slotted pages usually come in two flavours respectively having fixed length
 * and variable length slots. A fixed length slot slotted page is used in
 * collections storing fixed length records. Similarly, the variable length
 * slot slotted page is used in collections storing variable length records.
 */
class SlottedPage : public virtual Page {
public:
  /**
   * @brief Destroy the Slotted Page Base object
   *
   */
  virtual ~SlottedPage() {}

  /**
   * Get page slot of given identifier within the page.
   *
   * @param slotId Slot identifier
   * @param txn Reference to active transaction
   * @returns Constant reference to the PageSlot object if found
   * @throws PageSlotNotFoundError
   */
  virtual const PageSlot &getPageSlot(PageSlotId slotId,
                                      Transaction &txn) const = 0;

  // TODO: The insert interface is not thread safe as it exposes a loophole for
  // accessing protected slots by returning a pointer. We need a SlotHandle
  // which locks the access to slots from concurrent read and writes.
  
  /**
   * Insert page slot to the page.
   *
   * @param pageSlot Reference to the PageSlot object to insert
   * @param txn Reference to active transaction
   * @returns SlotId and pointer to the inserted PageSlot
   */
  virtual std::pair<PageSlotId, PageSlot *>
  insertPageSlot(PageSlot &pageSlot, Transaction &txn) = 0;

  /**
   * Update page slot in the page.
   *
   * @param slotId Identifier of the slot to update
   * @param pageSlot Reference to updated page slot
   * @param txn Reference to active transaction
   * @throws PageSlotNotFoundError
   */
  virtual void updatePageSlot(PageSlotId slotId, PageSlot &pageSlot,
                              Transaction &txn) = 0;

  /**
   * Remove page slot of given identifier within page.
   *
   * @param slotId Identifier of the slot to remove
   * @param txn Reference to active transaction
   * @throws PageSlotNotFoundError
   */
  virtual void removePageSlot(PageSlotId slotId, Transaction &txn) = 0;

  /**
   * Undo remove page slot of given identifier within page.
   *
   * @param slotId Identifier of the slot to insert back
   * @param pageSlot Reference to the slot to insert back
   * @param txn Reference to active transaction
   */
  virtual void undoRemovePageSlot(PageSlotId slotId, PageSlot &pageSlot,
                                  Transaction &txn) = 0;
};

} // namespace persist

#endif /* SLOTTED_PAGE_BASE_HPP */
