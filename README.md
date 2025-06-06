# CS307 Database Management System

[![Java](https://img.shields.io/badge/Java-17+-blue.svg)](https://www.oracle.com/java/)
[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-red.svg)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-Educational-green.svg)](LICENSE)

[中文文档](README_CN.md) | English

## Overview

This is a comprehensive database management system developed for the CS307 Database Systems course at SUSTech. The project is built upon the foundation of [CS307-Database/engine-project](https://github.com/CS307-Database/engine-project) and significantly extends it with advanced features including B+ tree indexing, query optimization, web interface, and full CRUD operations support.

### Key Features

- **🗄️ Complete Database Engine**: Extended from the base framework with comprehensive RDBMS functionality
- **📝 Full SQL Support**: Complete SQL parsing and execution using JSqlParser
- **🔍 Query Optimization**: Advanced logical and physical query optimization with cost-based planning
- **💾 Storage Management**: Enhanced page-based storage with buffer pool and LRU replacement
- **🌳 B+ Tree Indexing**: Efficient indexing system with automatic index management
- **🌐 Web Interface**: Modern, responsive web UI for database interaction
- **🔗 REST API**: HTTP server for programmatic database access
- **⚡ High Performance**: Optimized for concurrent operations with proper transaction handling
- **📊 Aggregation Functions**: Support for SUM, MAX, MIN, COUNT, AVG operations
- **🔢 Extended Data Types**: Full support for single-precision FLOAT and double-precision DOUBLE
- **📋 Advanced Queries**: Implemented ORDER BY sorting and GROUP BY grouping
- **🔗 Multi-table Joins**: Support for LEFT JOIN, INNER JOIN, RIGHT JOIN, CROSS JOIN operations

### Extensions from Base Framework

Our implementation significantly extends the original [engine-project](https://github.com/CS307-Database/engine-project) with:

- **Enhanced SQL Operations**: Added DELETE, complex WHERE clauses, JOIN operations, aggregation functions
- **Advanced Indexing**: Implemented B+ tree indexing with automatic index management and optimization
- **Query Optimization**: Built comprehensive logical and physical query planners
- **Web Interface**: Developed a modern web-based interface for database interaction
- **Performance Improvements**: Added buffer pool optimizations and concurrent operation support
- **Extended Data Types**: Support for FLOAT, DOUBLE, and enhanced CHAR handling
- **System Management**: Added table metadata management, index synchronization, and persistence

## Architecture

### Core Components

1. **Storage Layer** (Enhanced from base)
   - `DiskManager`: File I/O and page management
   - `BufferPool`: Memory management with LRU replacement policy
   - `Page`: Fixed-size page abstraction (4KB default)

2. **Record Management** (Extended)
   - `RecordManager`: Record lifecycle management
   - `RecordFileHandle`: File-level operations
   - `Record`: Individual record representation

3. **Metadata Management** (New)
   - `MetaManager`: Schema and table metadata persistence
   - `TableMeta`: Table structure definitions
   - `ColumnMeta`: Column specifications and constraints

4. **Indexing System** (New)
   - `IndexManager`: Index lifecycle and operations
   - `BPlusTreeIndex`: B+ tree implementation
   - `Index`: Generic index interface

5. **Query Processing** (Significantly Enhanced)
   - `PhysicalPlanner`: Query execution plan generation
   - `LogicalOperator`: Abstract query operations
   - `PhysicalOperator`: Concrete execution operators

6. **System Interface** (New)
   - `DBManager`: Central database coordination
   - `HttpServer`: REST API endpoint
   - Web UI: Modern frontend interface

### Technology Stack

- **Language**: Java 17
- **SQL Parser**: JSqlParser 5.1
- **Network**: Netty 4.1 (HTTP server & zero-copy buffers)
- **Serialization**: Jackson 2.17
- **Logging**: TinyLog 1.3
- **Testing**: JUnit 5 & AssertJ
- **Frontend**: HTML5/CSS3/JavaScript with Flask server
- **Build Tool**: Maven

## Installation & Setup

### Prerequisites

- Java 17 or higher
- Maven 3.6+
- Python 3.7+ (for web interface)

### Building the Project

```bash
# Clone the repository
git clone <repository-url>
cd SUSTech_CS307_DB_Proj2

# Build with Maven
mvn clean compile

# Run tests
mvn test
```

### Starting the Database Server

```bash
# Method 1: Using Maven
mvn exec:java -Dexec.mainClass="edu.sustech.cs307.DBEntry"

# Method 2: Direct execution
java -cp "target/classes:target/dependency/*" edu.sustech.cs307.DBEntry
```

The database server will start on port 8080 with the following services:
- Console interface for direct SQL interaction
- HTTP API for programmatic access
- Automatic metadata persistence

### Starting the Web Interface

```bash
# Navigate to frontend directory
cd front-end

# Install Flask (if not already installed)
pip install flask

# Start the web server
python server.py
```

Access the web interface at: http://localhost:3000

## Usage

### Console Interface

The database provides an interactive console supporting comprehensive SQL operations:

```sql
-- Table operations
SHOW TABLES;
DESC table_name;
CREATE TABLE students (id INT, name CHAR, age INT);
DROP TABLE students;

-- Data operations
INSERT INTO students VALUES (1, 'Alice', 20);
SELECT * FROM students WHERE age > 18;
UPDATE students SET age = 21 WHERE name = 'Alice';
DELETE FROM students WHERE age < 18;

-- Advanced queries
SELECT s.name, c.course_name 
FROM students s 
JOIN courses c ON s.course_id = c.id 
WHERE s.age > 20;

-- Index operations
CREATE INDEX idx_name ON students(name);
SHOW BTREE students name;
DROP INDEX idx_name ON students;
```

### HTTP API

The system exposes REST endpoints for external integration:

```bash
# Execute SQL queries
curl "http://localhost:8080/?sql=SELECT * FROM students"

# Get table list
curl "http://localhost:8080/?sql=SHOW TABLES"

# Post SQL commands
curl -X POST "http://localhost:8080" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "sql=SELECT * FROM students"
```

### Web Interface Features

- **Interactive SQL Editor**: Real-time SQL editing with template support
- **Real-time Results**: Immediate query execution and result display
- **Table Browser**: Visual table structure and data exploration
- **Performance Metrics**: Query execution time and statistics
- **Responsive Design**: Works on desktop and mobile devices

## Development

### Project Structure

```
src/main/java/edu/sustech/cs307/
├── DBEntry.java              # Main application entry point
├── exception/                # Custom exception types
├── http/                     # HTTP server implementation
├── index/                    # B+ tree and indexing system
├── logicalOperator/          # Query logical operators
├── meta/                     # Metadata management
├── optimizer/                # Query optimization engine
├── physicalOperator/         # Physical execution operators
├── record/                   # Record and page management
├── storage/                  # Storage engine components
├── system/                   # Core system managers
├── tuple/                    # Tuple representation
└── value/                    # Data type system
```

## Acknowledgments

This project is built upon the foundation provided by [CS307-Database/engine-project](https://github.com/CS307-Database/engine-project). We extend our gratitude to the original framework contributors for providing the solid base infrastructure.