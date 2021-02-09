<!--
 Notes.md - Persist
 
 Copyright 2021 Ketan Goyal
 
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
-->

# Development Notes

1. A transaction is considered committed when the commit log is written to the log file.

2. Before a modified page is written to storage, all its transaction logs should be written. This is called write-ahead logging (WAL). We achieve this by logging all operations performed on a page while it is pinned. Since a pinned page can not be written by the buffer manager, WAL is ensured.

3. Different modes for writing Pages to storage:

    a. TransactionManager:
        - FORCE: When a transaction commits, all modified pages by that transaction will be written.
        - NO-FORCE: Allows a transaction to commit without enforcing all the modified pages to be written. The pages will automatically be written during the page replacement process of the buffer manager. Note that all modified pages are flushed to the storage when the destructor of the buffer manager is called.

    b. BufferManager:
        - NO-STEAL: Page modified by a transaction that is still active, i.e. yet to commit, should not be written to disk
        - STEAL: Page modified by a transaction that is still active, i.e. yet to commit, can be written to disk

    Persist implements the STEAL mode for buffer manager as it results in better memory handling and supports both FORCE and NO-FORCE modes for transactions.

4. Approaches for handling Free Space Map (FSM):-

    Maintaining an up to date non-volatile (saved in storage) free space map is not required. In case the FSM is lost, such as on system restart, at most any new INSERT operation will use a new page. Any free space available on previously written pages will be left unutilized while all collection operations will still function correctly. The FSM will automatically be recovered with time whenever records stored on the pages not present in FSM are accessed. This is since the FSM is updated (in-memory) every time any page is accessed as part of a transaction. The following approaches can thus be taken to persist FSM.

        - Keep it in-memory always and never persist. The drawback of this approach is after a system failure or a restart, we will lose track of all the pages with free space. This implies that any following insert operations will use new pages. We can partially resolve this problem by updating the FSM on every `getPage` method call. In this way when a page with free space is accessed, the FSM will get notified and start tracking that page.

        - Persist FSM when the destructor of the buffer manager is called. In this approach the FSM will be updated only when a persistent collection is closed. This ensures the FSM is up to date after a graceful restart of the system. On such grantee is there on system failures.

        - [v0.1]Persist FSM on every flush call, i.e. when writing any modified page to storage. This method results in poor performance due to high frequency of IO.

        - Persist FSM on every transaction commit, i.e. after a commit log of a transaction is written. Note we persist the FSM irrespective of the mode of the buffer manager.

        - [v0.2] Store FSM in pages and let the buffer manager persist those pages during page replacement. This approach will also ensure that the FSM is stored on every commit if the buffer manager is in FORCE mode. Note that changes in FSM pages are not logged for recovery to ensure fast performance.

5. Add storage manager which allocates and de-allocates pages. The total number of pages in storage can be computes from file(s) size. This implies that no metadata object is needed for the lower level disk IO. Note the storage manager can be removed and its functionality can be moved to the implementation of the backend Storage.

6. Add interface to get file size in the `Storage` class. This is used by the storage manager/storage class itself to figure out the total number of pages. The total number of pages is needed in order to implement the `allocation` and `de-allocate` methods, and to set the correct page ID when creating new pages.

7. The log records can have different levels of granularity: page-level and record-level. A record-level log is composed of single or multiple Page-level logs. In `persist` page-level log records are tracked. It enables us to construct a record-level log record if needed by combining all the related page-level log records. A page-level log record is stored in a page called log-page by the log manager. A further advantage of using page-level log records is they usually fit within a single log-page. At most two log-pages will be required for big log records.

8. Concurrency control is slightly different from thread safety. It refers to preventing two or more active transactions from inappropriately influencing each other. Thus the ConcurrencyControlManager can be unit tested without multi-threading.

9. In multi-threaded settings, there are different levels of granularity of atomic operations to be implemented for thread safety:
    - Page-Level: needed for concurrent access of pages --> Implemented in the BufferManager class
    - Slot-Level: needed for concurrent access of slots within a page --> Implemented in the SlottedPage class
    - Record-Level: needed for concurrent operations on a record. A record is comprised of multiple slots and thus in-turn pages. --> Implemented in the ConcurrencyControlManager.

10. Use lazy serialization and de-serialization for better performance.
