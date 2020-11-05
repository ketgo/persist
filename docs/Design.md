<!--
 Design.md - Persist
 
 Copyright 2020 Ketan Goyal
 
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

# Design Documentation

## Introduction

This document describes the design and implementation of different components in the Persist package. From a high-level, the package exposes API for persisting the following data structures

- List
- Hash Table
- Binary Tree

where each data structure is a collection of objects. These collections can be persisted on backend storages like RAM, Disk, S3, etc. The exposed API has been designed to comply with ACID requirements.

## Usage Cases

The following uses cases are supported:

- Construct an empty persistent collection.

- Insert an object into the persistent collection.

- Remove an object from the persistent collection.

- Update an existing object in the persistent collection.

- Search objects in a persistent collection.

The proposed API for each use case is the rest of the document.

## Implementation Design

This section details the implementation of the different package components.

### Collection

A `Collection` comprises of one or more objects which can be persisted in backend storage. Each object is stored in binary format. The package comes with the collections of type `List`, `Map`, and `BinaryTree`.

#### Objects

This is any C++ data type that can be parsed from and into a binary form. The package supports both, primitive types as well as composite types like `struct` and `class`. An object is stored by splitting its binary form into chunks called [record blocks](#record-blocks). This approach enables efficient usage of storage space as large-sized objects are split into manageable sizes.

#### Iterator

An `Iterator` is a cursor on persistent collections. It is used to traverse the collection while pointing to the stored objects. Each collection type implements its version of the iterator.

#### Record Manager

The `RecordManager` handles read and write operations of [objects](#objects) on [pages](#page). Internally it interfaces with the page table to access the required pages. One or more operations performed by the record manager are logged for concurrency control and durability. The following API is exposed by the record manager:

```c++
```

#### Page Table

The `PageTable` is responsible for loading [pages](#page) from [backend storage](#backend-storage) and writing modified [pages](#page) back. It utilizes the exposed API by the `Storage` interface to achieve its goals. The buffer size is kept fixed and the least recently used (LRU) policy is used to replace a page. If the page has been modified since the last read, it is written back onto the storage. The following API are exposed by the manager:

```c++
/**
 * Get page with specified ID in buffer. If no such page exists, it either loads one from backend storage or throws PageNotFound exception.
 */
Page& get(PageId id);
```

### Backend Storage

A persistent collection can be stored upon a volatile or in-volatile storage media like RAM, disk, S3, etc. To support read and write operations on these different media, the `Storage` interface is provided. Any backend storage implementation inherits from this class.

For efficient input-output performance, the storage is logically divided into contiguous blocks of space called [pages](#page). Each page consists of metadata which facilitates reading and writing [record blocks](#record-block).

The `Storage` interface exposes the following API:

```c++
/**
 * Read Page with given identifier from storage.
 *
 * @param pageId page identifier
 * @returns pointer to Page object
 */
std::unique_ptr<Page> read(PageId pageId) = 0;

/**
 * Write Page object to storage.
 *
 * @param page reference to Page object to be written
 */
void write(Page &page) = 0;
```

#### Storage MetaData

#### Page

A Page is a logical chunk of space on backend storage. Each page comprises of a header, free space, and stored [record blocks](#record-block). The page header contains the page unique identifier along with the next and previous page identifiers in case the page is linked. It also contains entries of offset values indicating where each record-block in the page is located.

#### Page Header

#### Record Block

#### Record Block Header

### Operations Manager

The operations manager is responsible for concurrency control and facilitates ACID transactions. The underlying protocol implemented is described in detail in the [Protocol](Protocol.md) document.
