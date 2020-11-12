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
