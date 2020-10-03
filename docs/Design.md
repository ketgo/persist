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
- B+ Tree

where each data structure is a collection of objects. These collections can be persisted on backend storages like RAM, Disk, S3, etc. The exposed API has been designed to comply with ACID requirements.

## Usage Cases

The following uses cases are supported:

- Construct an empty persistent collection.

- Insert an object into the persistent collection.

- Remove an object from the persistent collection.

- Update an existing object in the persistent collection.

- Search objects in persistent collection.

The proposed API for each use case is described below.

## Implementation Design

This section details the implementation approach of the different package components.

### Collection

#### Record Objects

#### Cursors

### Backend Storage

A persistent collection can be stored upon a volatile or in-volatile storage media like RAM, disk, S3, etc. To support read and write operations on these different media, the `Storage` interface is exposed. Any backend storage implementation should inherit from this class.

For efficient input-output performance, any backend storage is divided into contiguous blocks of memory called pages. Each page consists of metadata which facilitates reading and writing [record blocks](#record-block).

#### Page

A data block is unit chunk of data stored on the storage. Each block comprises of header, free space and stored [record blocks](#record-block).

#### Page MetaData

#### Storage MetaData

#### Record Block

#### Record Manager

### Operations Manager

The operations manager is responsible for concurrency control and facilitates ACID transactions. The underlying protocol implemented is described in detail in the [Protocol](Protocol.md) document.
