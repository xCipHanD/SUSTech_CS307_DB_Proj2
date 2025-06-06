# CS307 数据库管理系统 - 前端界面

这是一个现代化的Web界面，用于与CS307数据库系统进行交互。

## 功能特性

### 🚀 核心功能
- **SQL查询执行**: 支持所有SQL语句的执行
- **实时结果显示**: 以表格形式展示查询结果
- **数据库信息展示**: 显示数据库名称和表列表
- **交互式表名插入**: 点击表名自动插入到SQL输入框

### ⌨️ 快捷操作
- **Ctrl + Enter**: 快速执行SQL查询
- **点击表名**: 自动插入到SQL输入框光标位置
- **自动刷新**: 执行DDL语句后自动刷新表列表

### 📱 响应式设计
- 支持桌面和移动设备
- 现代化的界面设计
- 流畅的动画效果

## 使用方法

### 1. 启动数据库服务器
首先确保你的数据库服务器正在运行：
```bash
# 在项目根目录下
cd src/main/java
javac -cp "../../../../target/classes:../../../../target/dependency/*" edu/sustech/cs307/DBEntry.java
java -cp "../../../../target/classes:../../../../target/dependency/*" edu.sustech.cs307.DBEntry
```

### 2. 启动前端服务器
在front-end目录下：
```bash
# 安装Flask（如果未安装）
pip install flask

# 启动前端服务器
python server.py
```

### 3. 访问Web界面
打开浏览器访问: http://localhost:3000

## 界面说明

### 左侧边栏 - 数据库信息
- **数据库名**: 显示当前连接的数据库名称
- **数据表列表**: 显示所有可用的数据表
- **刷新按钮**: 手动刷新表列表

### 中央区域 - SQL查询
- **SQL输入框**: 输入SQL查询语句
- **执行按钮**: 执行当前SQL语句
- **清空按钮**: 清空SQL输入框

### 底部区域 - 查询结果
- **执行状态**: 显示查询执行状态和元信息
- **结果表格**: 以表格形式展示查询结果
- **错误信息**: 显示SQL执行错误信息

## 支持的SQL语句示例

### 查看数据库结构
```sql
-- 显示所有表
SHOW TABLES;

-- 显示表结构
DESC students;
```

### 数据查询
```sql
-- 查询所有数据
SELECT * FROM students;

-- 条件查询
SELECT name, age FROM students WHERE age > 20;

-- 连接查询
SELECT s.name, c.course_name 
FROM students s 
JOIN courses c ON s.course_id = c.id;
```

### 数据操作
```sql
-- 插入数据
INSERT INTO students (name, age) VALUES ('张三', 20);

-- 更新数据
UPDATE students SET age = 21 WHERE name = '张三';

-- 删除数据
DELETE FROM students WHERE age < 18;
```

### 表结构操作
```sql
-- 创建表
CREATE TABLE test_table (
    id INT,
    name CHAR,
    score FLOAT
);

-- 删除表
DROP TABLE test_table;
```

## 技术架构

### 前端技术
- **HTML5 + CSS3**: 现代化界面设计
- **JavaScript (ES6+)**: 异步数据交互
- **Fetch API**: 与后端API通信

### 后端服务
- **Python Flask**: 静态文件服务器
- **Java HTTP Server**: 数据库API服务器(端口8080)

### 数据流程
```
浏览器 → Flask服务器(3000) → 静态文件
浏览器 → 数据库API(8080) → SQL执行 → 结果返回
```

## 故障排除

### 连接问题
1. **无法访问界面**: 检查Flask服务器是否在端口3000上运行
2. **无法执行SQL**: 检查数据库服务器是否在端口8080上运行
3. **CORS错误**: 数据库服务器已配置CORS，如仍有问题请检查浏览器控制台

### 常见错误
1. **"网络错误或服务器不可用"**: 数据库服务器未启动
2. **"请输入SQL查询语句"**: SQL输入框为空
3. **SQL语法错误**: 检查SQL语句语法

## 开发扩展

如需添加新功能，可以修改以下文件：
- `index.html`: 界面和交互逻辑
- `server.py`: Flask服务器配置
- 后端API: 修改DBEntry.java中的HttpServer

## 许可证

本项目为CS307课程作业，仅供学习使用。