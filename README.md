# RustDB: A Rust-based In-Memory Database with Persistence

RustDB is a lightweight, in-memory database implemented in Rust, featuring basic persistence and querying capabilities.

## Motivation

The primary motivation behind RustDB is to gain a deeper understanding of how databases work internally. By building a database from scratch, this project aims to:

1. Explore the core mechanisms of database management systems.
2. Implement and understand efficient data structures and algorithms for data storage and retrieval.
3. Gain hands-on experience with query processing and execution.
4. Understand the trade-offs between in-memory operations and persistent storage.

This project serves as a learning tool for diving into the internals of database systems. By implementing features like B-tree indexing, basic SQL parsing, and data persistence, it provides insights into the fundamental concepts that power larger, more complex database systems. While not intended for production use, RustDB offers a tangible way to grasp database concepts through hands-on implementation in Rust.

## Features

1. In-memory data storage with persistence to disk
2. Support for creating tables with custom schemas
3. Basic SQL-like query language (CREATE TABLE, INSERT, SELECT)
4. WHERE clause support for filtering data
5. B-tree indexing for efficient querying
6. Concurrent client connections using tokio
7. Efficient binary serialization using bincode

## How It Works

### Connection and Query Processing

1. The database listens for TCP connections on port 8080.
2. Clients connect and send SQL-like queries as plain text.
3. The server processes each query and returns the result.

### Data Storage and Indexing

- Tables are stored in-memory as a collection of rows and columns.
- Each table has an associated B-tree index for efficient lookups.
- The B-tree index maps column values to row indices for quick retrieval.

### Query Execution

1. CREATE TABLE: Initializes a new table with the specified schema.
2. INSERT: Adds a new row to the table and updates the B-tree index.
3. SELECT: Retrieves data from the table, optionally using the B-tree index for WHERE clauses.

### Persistence

- The database state is periodically saved to disk using binary serialization (bincode).
- Saving occurs after a configurable interval or number of write operations.
- On startup, the database loads its state from the saved file.

### Serialization

- The entire database state (tables, indexes, and data) is serialized using bincode.
- Bincode provides efficient, compact binary serialization and deserialization.

### B-tree Index

- Each table has a B-tree index implemented using Rust's BTreeMap.
- The index maps column values to vectors of row indices.
- This allows for efficient lookups when processing WHERE clauses.

## Performance Considerations

- In-memory storage provides fast read and write operations.
- B-tree indexing enables efficient querying, especially for large datasets.
- Periodic persistence helps balance performance and durability.
- Binary serialization with bincode ensures compact storage and fast loading/saving.

## Future Improvements

- Support for more complex SQL operations (JOIN, GROUP BY, etc.)
- Transaction support
- Better error handling and recovery
- Query optimization
- Support for multiple indexes per table

## Running the Database

1. Clone the repository
2. Run `cargo build --release`
3. Execute `./target/release/simple_db`

The database will start and listen for connections on `127.0.0.1:8080`.

## Running Tests

Execute `cargo test` to run the test suite, which includes end-to-end tests and performance benchmarks.
