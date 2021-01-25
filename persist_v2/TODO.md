<!--
 TODO.md - Persist
 
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

# TODO List

1. [DONE] Refactor backend storage
    - covert to template with PageType as parameter
    - add allocate and de-allocate methods
    - refactor metadata object to FSM
    - refactor read and write methods for metadata to that for FSM.

2. [IN-PROGRESS] Refactor buffer manager to
    - use replacers for the page replacement policy
    - new page creation using allocate method of backend storage
    - implement the STEAL and NO-STEAL mode of operations

3. [NOT SURE NEEDED] Implement FreeSpaceManager class to manage:
    - Serialization and de-serialization of polymorphic page objects
    - use the free space map (FSM) to track pages with free space
    - create a new page when FSM is empty using allocate method of backend storage
    - remove completely empty pages using the de-allocate method of backend storage
    - use FreeSpaceManager in the buffer manager
    - remove read and write methods for metadata objects in the Storage class

4. Implement a persistent log file for transaction logging:
    - create page class for storing log records
    - use buffer manager for loading pages of log records in the log manager
    - log manager to persist pages using the flush method of buffer manager

5. Implement the FORCE and NO-FORCE mode of operations of the transaction manager.

6. Create collection metadata manager:
    - handles metadata containing the starting location, ending location and number of records in a collection
    - writes metadata to the first record block of the collection

7. Implement List collection

8. Create Concurrency Control Manager:
    - Design manager class. The design should be extendable to support different concurrency control protocols
    - Implement different types of page locks for buffer manager needed by the manager
