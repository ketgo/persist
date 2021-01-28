# Scratch Pad

## Page Table Diagrams

The following diagrams depict the pages on backend storage before and while different operations are performed.

### Initial Diagram

Page 1
+----+------------+---------+--+---+
| M  |R4_1        |R3       |R2|R1 |
+----+------------+---------+--+---+
Page 2
+----+-------------+---------------+
| M  |R5_1         |R4_2           |
+----+-------------+---------------+
Page 3
+----+---------------------+--+----+
| M  |       Free          |R6|R5_2|
+----+---------------------+--+----+

Free Pages ID: 3

### Delete

Operation: Delete R4

Page 1
+----+------------+---------+--+---+
| M  |   Free     |R3       |R2|R1 |
+----+------------+---------+--+---+
Page 2
+----+-------------+---------------+
| M  |    Free     |R5_1           |
+----+-------------+---------------+
Page 3
+----+---------------------+--+----+
| M  |       Free          |R6|R5_2|
+----+---------------------+--+----+

Free Pages ID: 3, 2, 1

### Insert

Operation: Insert R7

Page 1
+----+------------+---------+--+---+
| M  |   Free     |R3       |R2|R1 |
+----+------------+---------+--+---+
Page 2
+----+-------------+---------------+
| M  |R7           |R5_1           |
+----+-------------+---------------+
Page 3
+----+---------------------+--+----+
| M  |       Free          |R6|R5_2|
+----+---------------------+--+----+

Free Pages ID: 3, 1

## Page Table Transaction

List of operations:

- Get Page with ID

- Get New Page
  - Create a new page
  - Update storage metadata

- Get a Page with free space

- Mark Page as dirty
  - Updates metadata if free space is available in the page.

## Common Collection Operations

- Insert object
- Bulk insert objects
- Update object
- Bulk update objects
- Delete object
- Bulk delete objects

## Transactions

- Begin
- RollBack: Abort transactions
- Commit

### Single-Collection Session

- Same as page table session
- rollback
- recovery
- API usage

### Multi-Collection Session

- Multi-collection access
- Rollback
- recovery
- API usage

-------------------------------------------------

## Concurrent Buffer Manager Access

PUT:

- `<T_i, P_a>` & `<T_j, P_b>`: Since the two threads are loading different pages, they are non-conflicting

- `<T_i, P_a>` & `<T_j, P_a>`: This should never be allowed.

GET:

- P_a and P_b already loaded

  - `<T_i, P_a>` & `<T_j, P_b>`: Since the two threads are reading different pages, they are non-conflicting

  - `<T_i, P_a>` & `<T_j, P_a>`: As the two threads are just reading the pages, they are non-conflicting

- Load P_a after page replacement and then read

  - `<T_i, P_a>` & `<T_j, P_b>`: Since thread T_i is loading P_a in a different memory location via PUT than where P_b is present, they are non-conflicting

  - `<T_i, P_a>` & `<T_j, P_a>`: One of the threads should load via PUT while the other waits

- Load P_a and P_b after page replacement and then read

  - `<T_i, P_a>` & `<T_j, P_b>`: Since the two threads are reading different pages, they are non-conflicting

GET_NEW:

  - Assuming backend storage supports atomic `allocate`, this operation boils down to PUT.

GET_FREE:

FLUSH:

FLUSH_ALL:
