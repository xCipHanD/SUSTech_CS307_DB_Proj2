# CS307 数据库管理系统

[![Java](https://img.shields.io/badge/Java-17+-blue.svg)](https://www.oracle.com/java/)
[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-red.svg)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-Educational-green.svg)](LICENSE)

中文 | [English](README.md)

## 项目概述

这是一个为南方科技大学CS307数据库系统课程开发的综合性数据库管理系统。该项目基于 [CS307-Database/engine-project](https://github.com/CS307-Database/engine-project) 基础框架构建，并在此基础上大幅扩展，新增了B+树索引、查询优化、Web界面以及完整的CRUD操作支持。

### 核心特性

- **🗄️ 完整数据库引擎**: 在基础框架上扩展的全功能关系型数据库管理系统
- **📝 完整SQL支持**: 使用JSqlParser实现完整的SQL解析和执行
- **🔍 查询优化**: 基于成本的高级逻辑和物理查询优化
- **💾 存储管理**: 增强的页式存储，支持缓冲池和LRU替换策略
- **🌳 B+树索引**: 高效索引系统，支持自动索引管理
- **🌐 Web界面**: 现代化响应式Web用户界面
- **🔗 REST API**: HTTP服务器支持程序化数据库访问
- **⚡ 高性能**: 针对并发操作优化，支持事务处理
- **📊 聚合功能**: 支持SUM、MAX、MIN、COUNT、AVG等聚合函数
- **🔢 扩展数据类型**: 完整支持单精度浮点数(FLOAT)和双精度浮点数(DOUBLE)
- **📋 高级查询**: 实现ORDER BY排序和GROUP BY分组查询
- **🔗 多表连接**: 支持LEFT JOIN、INNER JOIN、RIGHT JOIN、CROSS JOIN操作

### 基于原始框架的扩展功能

我们的实现在原始 [engine-project](https://github.com/CS307-Database/engine-project) 基础上进行了重大扩展：

- **增强SQL操作**: 新增DELETE语句、复杂WHERE条件、JOIN操作、聚合函数
- **高级索引**: 实现B+树索引，支持自动索引管理和优化
- **查询优化**: 构建了完整的逻辑和物理查询规划器
- **Web界面**: 开发了现代化的Web数据库交互界面
- **性能改进**: 增加缓冲池优化和并发操作支持
- **扩展数据类型**: 支持FLOAT、DOUBLE和增强的CHAR处理
- **系统管理**: 新增表元数据管理、索引同步和持久化

## 系统架构

### 核心组件

1. **存储层** (基于原框架增强)
   - `DiskManager`: 文件I/O和页面管理
   - `BufferPool`: 内存管理和LRU替换策略
   - `Page`: 固定大小页面抽象(默认4KB)

2. **记录管理** (扩展功能)
   - `RecordManager`: 记录生命周期管理
   - `RecordFileHandle`: 文件级操作
   - `Record`: 单个记录表示

3. **元数据管理** (全新实现)
   - `MetaManager`: 模式和表元数据持久化
   - `TableMeta`: 表结构定义
   - `ColumnMeta`: 列规范和约束

4. **索引系统** (全新实现)
   - `IndexManager`: 索引生命周期和操作
   - `BPlusTreeIndex`: B+树实现
   - `Index`: 通用索引接口

5. **查询处理** (大幅增强)
   - `PhysicalPlanner`: 查询执行计划生成
   - `LogicalOperator`: 抽象查询操作
   - `PhysicalOperator`: 具体执行算子

6. **系统接口** (全新实现)
   - `DBManager`: 中央数据库协调器
   - `HttpServer`: REST API端点
   - Web UI: 现代化前端界面

### 技术栈

- **编程语言**: Java 17
- **SQL解析器**: JSqlParser 5.1
- **网络框架**: Netty 4.1 (HTTP服务器和零拷贝缓冲区)
- **序列化**: Jackson 2.17
- **日志系统**: TinyLog 1.3
- **测试框架**: JUnit 5 & AssertJ
- **前端**: HTML5/CSS3/JavaScript + Flask服务器
- **构建工具**: Maven

## 安装配置

### 环境要求

- Java 17 或更高版本
- Maven 3.6+
- Python 3.7+ (用于Web界面)

### 构建项目

```bash
# 克隆仓库
git clone <repository-url>
cd SUSTech_CS307_DB_Proj2

# 使用Maven构建
mvn clean compile

# 运行测试
mvn test
```

### 启动数据库服务器

```bash
# 方法1: 使用Maven
mvn exec:java -Dexec.mainClass="edu.sustech.cs307.DBEntry"

# 方法2: 直接执行
java -cp "target/classes:target/dependency/*" edu.sustech.cs307.DBEntry
```

数据库服务器将在8080端口启动，提供以下服务：
- 控制台界面用于直接SQL交互
- HTTP API用于程序化访问
- 自动元数据持久化

### 启动Web界面

```bash
# 进入前端目录
cd front-end

# 安装Flask (如果尚未安装)
pip install flask

# 启动Web服务器
python server.py
```

访问Web界面: http://localhost:3000

## 使用说明

### 控制台界面

数据库提供交互式控制台，支持全面的SQL操作：

```sql
-- 表操作
SHOW TABLES;
DESC table_name;
CREATE TABLE students (id INT, name CHAR, age INT, gpa FLOAT);
CREATE TABLE courses (id INT, name CHAR, credits DOUBLE);
DROP TABLE students;

-- 数据操作
INSERT INTO students VALUES (1, 'Alice', 20, 3.8);
INSERT INTO students VALUES (2, 'Bob', 22, 3.5);
INSERT INTO courses VALUES (1, 'Database', 4.0);
SELECT * FROM students WHERE age > 18;
UPDATE students SET gpa = 3.9 WHERE name = 'Alice';
DELETE FROM students WHERE age < 18;

-- 聚合函数查询
SELECT COUNT(*) FROM students;
SELECT AVG(gpa) FROM students;
SELECT SUM(credits), MAX(credits), MIN(credits) FROM courses;

-- GROUP BY 分组查询
SELECT age, COUNT(*), AVG(gpa) 
FROM students 
GROUP BY age;

-- ORDER BY 排序查询
SELECT * FROM students ORDER BY gpa DESC;
SELECT * FROM students ORDER BY age ASC, gpa DESC;

-- JOIN 连接查询
-- INNER JOIN
SELECT s.name, c.name 
FROM students s 
INNER JOIN courses c ON s.course_id = c.id;

-- LEFT JOIN
SELECT s.name, c.name 
FROM students s 
LEFT JOIN courses c ON s.course_id = c.id;

-- RIGHT JOIN
SELECT s.name, c.name 
FROM students s 
RIGHT JOIN courses c ON s.course_id = c.id;

-- CROSS JOIN
SELECT s.name, c.name 
FROM students s 
CROSS JOIN courses c;

-- 复合查询 (GROUP BY + ORDER BY + 聚合函数)
SELECT age, COUNT(*) as student_count, AVG(gpa) as avg_gpa
FROM students 
GROUP BY age 
ORDER BY avg_gpa DESC;

-- 浮点数数据类型支持
CREATE TABLE measurements (
    id INT,
    temperature FLOAT,     -- 单精度浮点数
    precision_value DOUBLE -- 双精度浮点数
);
INSERT INTO measurements VALUES (1, 25.5, 3.141592653589793);

-- 索引操作
CREATE INDEX idx_name ON students(name);
CREATE INDEX idx_gpa ON students(gpa);
SHOW BTREE students name;
DROP INDEX idx_name ON students;
```

### HTTP API

系统暴露REST端点用于外部集成：

```bash
# 执行SQL查询
curl "http://localhost:8080/?sql=SELECT * FROM students"

# 获取表列表
curl "http://localhost:8080/?sql=SHOW TABLES"

# POST SQL命令
curl -X POST "http://localhost:8080" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "sql=SELECT * FROM students"
```

### Web界面功能

- **交互式SQL编辑器**: 实时SQL编辑，支持模板功能
- **实时结果显示**: 即时查询执行和结果展示
- **表浏览器**: 可视化表结构和数据探索
- **性能指标**: 查询执行时间和统计信息
- **响应式设计**: 支持桌面和移动设备

## 开发指南

### 项目结构

```
src/main/java/edu/sustech/cs307/
├── DBEntry.java              # 主应用程序入口
├── exception/                # 自定义异常类型
├── http/                     # HTTP服务器实现
├── index/                    # B+树和索引系统
├── logicalOperator/          # 查询逻辑算子
├── meta/                     # 元数据管理
├── optimizer/                # 查询优化引擎
├── physicalOperator/         # 物理执行算子
├── record/                   # 记录和页面管理
├── storage/                  # 存储引擎组件
├── system/                   # 核心系统管理器
├── tuple/                    # 元组表示
└── value/                    # 数据类型系统
```

## 致谢

本项目基于 [CS307-Database/engine-project](https://github.com/CS307-Database/engine-project) 提供的基础框架构建。我们向原始框架的贡献者表示感谢，感谢他们提供了坚实的基础架构。
