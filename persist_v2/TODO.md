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
    - [DONE] covert to template with PageType as parameter
    - [DONE] add allocate and de-allocate methods
    - [DONE] refactor metadata object to FSL
    - [DONE] refactor read and write methods for metadata to that for FSL.

2. [IN-PROGRESS] Refactor buffer manager to
    - [DONE] use replacers for the page replacement policy
    - [DONE] new page creation using allocate method of the backend storage
    - [DONE] implement very basic thread-safety by applying a recursive mutex to each method call

3. [LOW-PRIORITY] Create a FreeSpaceManager for efficient detection and handling of pages with free space
    - refactor `getFree` method of buffer manager to take sizeHint parameter as an argument. This parameter provides a hint to the free space manager about the amount of free space requested. Note that this parameter is treated only as a hint as the FreeSpaceManager is free to return a page with less available free space.
    - create an interface for FreeSpaceManager to allow for different FreeSpaceManager implementations
    - implement a basic FreeSpaceManager
    - refactor buffer manager to use FreeSpaceManager
    - remove the FSL and the read and write method of backend storage
    - FreeSpaceManager should use Pages obtained from the buffer manager to persist FSL

4. [IN-PROGRESS] Implement a persistent log file for transaction logging:
    - create page class for storing log records
    - use buffer manager for loading pages of log records in the log manager
    - log manager to persist pages using the flush method of buffer manager

5. Implement the FORCE and NO-FORCE mode of operations of the transaction manager.

6. Implement PAGE-LEVEL and SLOT-LEVEL granularity of atomic operations for thread safety:
    - refactor buffer manager for PAGE-LEVEL granularity
        - implement and use no lock concurrent hash map for page buffer
        - implement and use no lock concurrent replacer
        - implement and use no lock concurrent FreeSpaceManager
    - create a new thread-safe slotted page implementation for SLOT-LEVEL granularity

7. Create collection metadata manager:
    - handles metadata containing the starting location, ending location, and number of records in a collection
    - writes metadata to the first record block of the collection

8. Implement List collection
    - implement record manager for list collection
    - implement list collection class

9. Create Concurrency Control Manager:
    - Design manager class. The design should be extendable to support different concurrency control protocols
    - Implement different types of concurrency control policies
    - Implement RECORD-LEVEL atomic operations
