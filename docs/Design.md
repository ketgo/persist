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

# Design

This document describes the design and implementation of different components in the Persist package. From a high-level, the package exposes API for persisting the following data structures

- List
- Hash Table
- B+ Tree

where each data structure is a collection of objects. These collections can be persisted on backend storages like RAM, Disk, S3, etc. The exposed API has been designed to comply with ACID requirements.

## Usage Cases

The following uses cases are supported:

- Insert an object into the stored collection.

- Remove an object from the stored collection.

- Update an existing object in the stored collection.

- Search objects in the stored collection.

The proposed API for each use case is shown below corresponding to the different types of collections.

### List

### Hash Table

### B+ Tree

## Storage

### Data Block

A data block is unit chunk of data stored on the storage. Each block comprises of header, free space and stored [record blocks](#record-block).

### MetaData

### Storage Manager

## Records

### Record Block

### Record Manager
