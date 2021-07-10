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

# Task List

## Core Components

1. [DONE] Refactor backend storage
    - [DONE] covert to template with PageType as parameter
    - [DONE] add allocate and de-allocate methods
    - [DONE] refactor metadata object to FSL
    - [DONE] refactor read and write methods for metadata to that for FSL.

2. [DONE] Refactor buffer manager to
    - [DONE] use replacers for the page replacement policy
    - [DONE] new page creation using allocate method of the backend storage
    - [DONE] implement very basic thread-safety by applying a recursive mutex to each method call

3. [IN-PROGRESS] Create a FreeSpaceManager for efficient detection and handling of pages with free space
    - [SKIPPED] serialize/deserialize polymorphic pages
    - [SKIPPED] refactor buffer manager to use polymorphic page
    - [DONE] refactor `GetFree` method of buffer manager to take sizeHint parameter as an argument. This parameter provides a hint to the free space manager about the amount of free space requested. Note that this parameter is treated only as a hint with the FreeSpaceManager free to return a page with less available free space.
    - [DONE] create an interface for FreeSpaceManager to allow for different FreeSpaceManager implementations
    - [DONE] implement a basic FreeSpaceManager
    - [DONE] decouple buffer manager from FreeSpaceManager
    - [DONE] remove the FSL and the read and write method of backend storage
    - [DONE] FreeSpaceManager should use Pages obtained from the buffer manager to persist FSL

4. [DONE] Implement a persistent log file for transaction logging:
    - [DONE] create page class for storing log records
    - [DONE] use buffer manager for loading pages of log records in the log manager
    - [DONE] log manager to persist pages using the flush method of buffer manager

5. [DONE] Implement the FORCE and NO-FORCE mode of operations of the transaction manager:
    - [DONE] Re-design and implement transaction class
    - [DONE] Refactor transaction manager to use FORCE and NO-FORCE policies
    - [SKIPPED] Maybe introduce transaction context?

6. [IN-PROGRESS] Implement thread safety for different classes:
    - Implement Utility Data Structures:
        - implement and use no-lock/lock-based concurrent hash map --> used in multiple places like buffer manager and slotted pages
        - implement and use no-lock/lock-based concurrent replacer --> used by the buffer manager
        - implement and use no-lock/lock-based concurrent FreeSpaceManager --> used by the buffer manager
    - [DONE] Add thread-safety annotations
    - [IN-PROGRESS] Add lock guards for basic thread-safety in:
        - [DONE] BufferManager
        - [DONE] LRUReplacer
        - [DONE] LogManager --> Possibly can be skipped
        - [DONE] FSM
    - [DONE] Create utility classes for unit-testing thread-safety --> [Created TSTest lib]
        - [DONE] Create an event queue to store a chronologically ordered sequence of unit operations
        - [DONE] Create an `UnitOperation` Class to run one or more statements as a single unit. The class should push the start and end event records in the event queue.
        - [DONE] Create a `Runner` class to execute a sequence of `UnitOperation` objects in multiple threads.
        - [DONE] Create an `AssertionMap` class to map a sequence of events to a lambda function. The lambda function will assert expected behavior for the mapped event sequence.
    - [SKIPPED] Support plugin of thread-safe and unsafe Replacers for BufferManager:
        - Add `ReplacerTraits` template class containing thread-safety trait information for replacers
        - Create a `ThreadSafeReplacer` class to wrap thread-unsafe replacer implementations
        - Create traits class for LRUReplacer implying it is thread-safe
    - [IN-PROGRESS] Create Type-Parameterized thread-safety test fixtures inside test headers
    - [IN-PROGRESS] Write thread-safety unit tests for:
        - [IN-PROGRESS] BufferManager
        - [IN-PROGRESS] LRUReplacer
        - LogManager
        - FSM

7. [DONE] Refactor all components to policy-based design.

8. [DONE] Design a Storable object interface. A storable object should expose the interface:
    - [DONE] size_t GetStorageSize(): The amount of storage space in bytes occupied by the object
    - [SKIPPED] size_t GetMinSize(): The minimum amount of storage space in bytes occupied by the object.
    - [SKIPPED] size_t GetMaxSize(): The maximum amount of storage space in bytes occupied by the object. A returned value of 0 indicates no max size.
    - [DONE] void Load(Span input): Load object from byte buffer
    - [DONE] void Dump(Span output): Dump object to byte buffer

9. Create Concurrency Control Manager:
    - Design manager class. The design should be extendable to support different concurrency control protocols
    - Create a separate project to PoC concurrency manager design
    - Implement different types of concurrency control policies
    - Implement RECORD-LEVEL atomic operations by providing concurrency control at PAGE_SLOT-LEVEL
    - Design approach to handle the scenario where FSM returns the same page ID for multiple concurrent transactions.

10. Implement recovery manager and checkpoint manager

11. [DONE] Create collection metadata manager:
    - [DONE] handles metadata containing the starting location, ending location, and number of records in a collection
    - [DONE] writes metadata to the first record block of the collection
    - improve interface

12. Transaction Manager for a group of collections.
    - Use a map data structure with key as collection namespace and value as a pointer to collection buffer manager.
    - Add collection namespace as part of log record so that the transaction manager can manage multiple collections.
    - Refactor abort and commit routines of transaction manager to handle multiple collections. The collection to operate upon can be obtained from the stored collection namespace in the log records.
    - Add context field to transaction class. The context stores operational information such as:
        a. currently operated upon collection namespace
        b. operation start time
        c. operation end time

13. Remove the observer pattern and put all the logic to PageHandle.

14. Implement read-only and read-write PageHandle classes.

------------------------------------------------------------

## Collections

1. [IN-PROGRESS] Implement List collection
    - [Done] implement record manager for list collection
    - [IN-PROGRESS] implement list collection class
        - Refactor transaction manager to use a pointer instead of a reference to collection buffer manager.
        - Add register method in transaction manager to store a pointer to the buffer manager of a collection.

------------------------------------------------------------

## Release Related Tasks

1. Benchmarking and Lazy serialization

2. Split tests into unit-tests and integration-tests

------------------------------------------------------------