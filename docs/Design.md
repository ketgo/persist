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

- Search objects in persistent collection.

The proposed API for each use case is the rest of the document.

## Implementation Design

This section details the implementation of the different package components.

### Collection

A collection comprises of one or more objects which can be persisted in backend storage. Each object is stored in binary format. The package comes with the collections of type `List`, `Map`, and `BinaryTree`.

#### Objects

This is any C++ data type that can be parsed from and into a binary form. The package supports both, primitive types as well as composite types like `struct` and `class`. An object is stored by splitting its binary form into chunks called [record blocks](#record-blocks). This approach enables efficient usage of storage space as large-sized objects are split into manageable sizes.

#### Cursors

A `Cursor` is just an iterator on persistent collections. It is used to traverse the collection while pointing to the stored objects. Each collection type implements its version of the cursor.

#### Buffer Manager

The buffer manager is responsible for loading [pages](#page) from [backend storage](#backend-storage) and writing modified [pages](#page) back. It utilizes the exposed API by the `Storage` interface to achieve its goals. The buffer size is kept fixed and the least recently used (LRU) policy is used to replace a page. If the page has been modified since last read, it is written back onto the storage. The following API are exposed by the manager:

```c++
/**
 * Get a page containing free space in buffer. If no such page exists, either it loads an existing one from storage or facilitates creation of a new page.
 */
Page& get();

/**
 * Get page with specified ID in buffer. If no such page exists, it either loads one from backend storage or throws PageNotFound exception.
 */
Page& get(PageId id);
```

### Backend Storage

A persistent collection can be stored upon a volatile or in-volatile storage media like RAM, disk, S3, etc. To support read and write operations on these different media, the `Storage` interface is provided. Any backend storage implementation inherits from this class.

For efficient input-output performance, the storage is logically divided into contiguous blocks of space called [pages](#page). Each page consists of metadata which facilitates reading and writing [record blocks](#record-block).

#### Page

A data block is unit chunk of data stored on the storage. Each block comprises of header, free space and stored [record blocks](#record-block).

#### Page MetaData

#### Storage MetaData

#### Record Block

### Operations Manager

The operations manager is responsible for concurrency control and facilitates ACID transactions. The underlying protocol implemented is described in detail in the [Protocol](Protocol.md) document.
