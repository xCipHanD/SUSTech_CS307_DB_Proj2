#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CS307数据库项目 - 全面Fuzz测试工具
测试所有SQL功能、边界条件、错误处理和性能
"""

import requests
import json
import random
import string
import time
import threading
import itertools
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse


@dataclass
class TestResult:
    test_name: str
    sql: str
    success: bool
    response_time: float
    status_code: int
    response: Dict[str, Any]
    error_message: Optional[str] = None


class DatabaseFuzzTester:
    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url
        self.test_results: List[TestResult] = []
        self.table_names = []
        self.created_tables = set()

    def log(self, message: str, level: str = "INFO"):
        """日志输出"""
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")

    def execute_sql(
        self, sql: str, timeout: int = 30
    ) -> Tuple[Dict[str, Any], float, int]:
        """执行SQL语句并返回结果"""
        start_time = time.time()
        try:
            encoded_sql = urllib.parse.quote(sql)
            response = requests.get(
                f"{self.base_url}?sql={encoded_sql}", timeout=timeout
            )
            execution_time = time.time() - start_time

            try:
                result = response.json()
            except json.JSONDecodeError:
                result = {
                    "status": "error",
                    "message": "Invalid JSON response",
                    "raw": response.text,
                }

            return result, execution_time, response.status_code
        except Exception as e:
            execution_time = time.time() - start_time
            return {"status": "error", "message": str(e)}, execution_time, 0

    def run_test(self, test_name: str, sql: str) -> TestResult:
        """运行单个测试"""
        self.log(f"Running test: {test_name}")
        self.log(f"SQL: {sql}")

        response, exec_time, status_code = self.execute_sql(sql)

        success = status_code == 200 and response.get("status") in ["success", "help"]
        error_msg = response.get("message") if not success else None

        result = TestResult(
            test_name=test_name,
            sql=sql,
            success=success,
            response_time=exec_time,
            status_code=status_code,
            response=response,
            error_message=error_msg,
        )

        self.test_results.append(result)
        status = "✅ PASS" if success else "❌ FAIL"
        self.log(f"{status} {test_name} ({exec_time:.3f}s)")
        if not success:
            self.log(f"Error: {error_msg}", "ERROR")

        return result

    # =========================== 基础功能测试 ===========================

    def test_basic_commands(self):
        """测试基础命令"""
        self.log("=== 测试基础命令 ===")

        tests = [
            ("Help命令", "HELP;"),
            ("Show Tables", "SHOW TABLES;"),
            ("Show Tables无分号", "SHOW TABLES"),
            ("Help无分号", "HELP"),
        ]

        for test_name, sql in tests:
            self.run_test(test_name, sql)

    def test_table_creation(self):
        """测试表创建功能"""
        self.log("=== 测试表创建 ===")

        # 基础表创建
        basic_tables = [
            (
                "创建简单表",
                """
                CREATE TABLE simple_table (
                    id INTEGER,
                    name CHAR
                );
            """,
            ),
            (
                "创建复杂表",
                """
                CREATE TABLE complex_table (
                    id INTEGER,
                    name CHAR,
                    age INTEGER,
                    salary FLOAT,
                    score DOUBLE,
                    active CHAR
                );
            """,
            ),
            (
                "创建测试表1",
                """
                CREATE TABLE test_table1 (
                    student_id INTEGER,
                    student_name CHAR,
                    grade FLOAT
                );
            """,
            ),
            (
                "创建测试表2",
                """
                CREATE TABLE test_table2 (
                    course_id INTEGER,
                    course_name CHAR,
                    credits INTEGER
                );
            """,
            ),
        ]

        for test_name, sql in basic_tables:
            result = self.run_test(test_name, sql)
            if result.success:
                # 提取表名
                table_name = sql.split("TABLE")[1].split("(")[0].strip()
                self.created_tables.add(table_name)
                self.table_names.append(table_name)

        # 边界条件测试
        edge_cases = [
            ("空表名", "CREATE TABLE () (id INTEGER);"),
            ("超长表名", f"CREATE TABLE {'a' * 100} (id INTEGER);"),
            ("重复表名", "CREATE TABLE simple_table (id INTEGER);"),
            ("无列定义", "CREATE TABLE empty_table ();"),
            ("无效数据类型", "CREATE TABLE invalid_table (id INVALID_TYPE);"),
        ]

        for test_name, sql in edge_cases:
            self.run_test(test_name, sql)

    def test_data_insertion(self):
        """测试数据插入功能"""
        self.log("=== 测试数据插入 ===")

        if not self.created_tables:
            self.log("No tables created, skipping insertion tests", "WARNING")
            return

        # 基础插入测试
        basic_inserts = [
            ("插入简单数据", 'INSERT INTO simple_table VALUES (1, "Alice");'),
            (
                "插入复杂数据",
                'INSERT INTO complex_table VALUES (1, "John Doe", 25, 50000.5, 95.7, "Y");',
            ),
            (
                "插入多行数据",
                'INSERT INTO test_table1 VALUES (1, "Student1", 85.5), (2, "Student2", 92.0);',
            ),
            ("指定列插入", 'INSERT INTO simple_table (id, name) VALUES (2, "Bob");'),
        ]

        for test_name, sql in basic_inserts:
            self.run_test(test_name, sql)

        # 边界条件和错误测试
        edge_cases = [
            (
                "插入过长字符串",
                'INSERT INTO simple_table VALUES (3, "' + "x" * 100 + '");',
            ),
            ("类型不匹配", 'INSERT INTO simple_table VALUES ("abc", 123);'),
            ("列数不匹配", "INSERT INTO simple_table VALUES (4);"),
            ("插入空值", 'INSERT INTO simple_table VALUES (5, "");'),
            ("插入到不存在的表", 'INSERT INTO non_existent VALUES (1, "test");'),
        ]

        for test_name, sql in edge_cases:
            self.run_test(test_name, sql)

        # 大批量插入测试
        self.test_bulk_insert()

    def test_bulk_insert(self):
        """测试大批量数据插入"""
        self.log("=== 测试大批量插入 ===")

        # 生成大量数据
        values = []
        for i in range(100):
            name = f"Student{i}"
            grade = round(random.uniform(60, 100), 2)
            values.append(f"({i + 100}, '{name}', {grade})")

        bulk_sql = f"INSERT INTO test_table1 VALUES {', '.join(values)};"
        self.run_test("大批量插入100条记录", bulk_sql)

    def test_data_selection(self):
        """测试数据查询功能"""
        self.log("=== 测试数据查询 ===")

        # 基础查询
        basic_selects = [
            ("查询所有数据", "SELECT * FROM simple_table;"),
            ("指定列查询", "SELECT id, name FROM simple_table;"),
            ("条件查询", "SELECT * FROM simple_table WHERE id = 1;"),
            ("范围查询", "SELECT * FROM test_table1 WHERE grade > 90;"),
            ("字符串匹配", 'SELECT * FROM simple_table WHERE name = "Alice";'),
        ]

        for test_name, sql in basic_selects:
            self.run_test(test_name, sql)

        # 复杂查询
        complex_selects = [
            (
                "多条件查询",
                "SELECT * FROM test_table1 WHERE student_id > 50 AND grade < 90;",
            ),
            ("排序查询", "SELECT * FROM test_table1 ORDER BY grade DESC;"),
            ("限制结果", "SELECT * FROM test_table1 LIMIT 5;"),
            ("聚合函数", "SELECT COUNT(*) FROM test_table1;"),
            ("求和", "SELECT SUM(grade) FROM test_table1;"),
            ("平均值", "SELECT AVG(grade) FROM test_table1;"),
            ("最大值", "SELECT MAX(grade) FROM test_table1;"),
            ("最小值", "SELECT MIN(grade) FROM test_table1;"),
        ]

        for test_name, sql in complex_selects:
            self.run_test(test_name, sql)

        # JOIN查询测试
        self.test_join_queries()

    def test_join_queries(self):
        """测试JOIN查询"""
        self.log("=== 测试JOIN查询 ===")

        # 首先插入一些测试数据
        setup_queries = [
            "INSERT INTO test_table2 VALUES (1, 'Mathematics', 4);",
            "INSERT INTO test_table2 VALUES (2, 'Physics', 3);",
            "INSERT INTO test_table2 VALUES (3, 'Chemistry', 3);",
        ]

        for sql in setup_queries:
            self.execute_sql(sql)

        join_queries = [
            (
                "简单JOIN",
                """
                SELECT t1.student_name, t2.course_name 
                FROM test_table1 t1 
                JOIN test_table2 t2 ON t1.student_id = t2.course_id;
            """,
            ),
            (
                "LEFT JOIN",
                """
                SELECT t1.student_name, t2.course_name 
                FROM test_table1 t1 
                LEFT JOIN test_table2 t2 ON t1.student_id = t2.course_id;
            """,
            ),
        ]

        for test_name, sql in join_queries:
            self.run_test(test_name, sql)

    def test_data_updates(self):
        """测试数据更新功能"""
        self.log("=== 测试数据更新 ===")

        updates = [
            (
                "简单更新",
                "UPDATE simple_table SET name = 'Updated Alice' WHERE id = 1;",
            ),
            ("批量更新", "UPDATE test_table1 SET grade = grade + 5 WHERE grade < 80;"),
            ("更新所有记录", "UPDATE simple_table SET name = 'Everyone';"),
            (
                "条件更新",
                "UPDATE test_table1 SET student_name = 'Top Student' WHERE grade > 95;",
            ),
        ]

        for test_name, sql in updates:
            self.run_test(test_name, sql)

        # 错误更新测试
        error_updates = [
            ("更新不存在的表", 'UPDATE non_existent SET col = "value";'),
            ("更新不存在的列", 'UPDATE simple_table SET non_existent = "value";'),
            ("类型不匹配更新", 'UPDATE simple_table SET id = "not_a_number";'),
        ]

        for test_name, sql in error_updates:
            self.run_test(test_name, sql)

    def test_data_deletion(self):
        """测试数据删除功能"""
        self.log("=== 测试数据删除 ===")

        deletions = [
            ("条件删除", "DELETE FROM test_table1 WHERE grade < 70;"),
            ("删除单条记录", "DELETE FROM simple_table WHERE id = 2;"),
            ("删除多条记录", "DELETE FROM test_table1 WHERE student_id > 150;"),
        ]

        for test_name, sql in deletions:
            self.run_test(test_name, sql)

        # 错误删除测试
        error_deletions = [
            ("删除不存在的表", "DELETE FROM non_existent WHERE id = 1;"),
            ("无效WHERE条件", "DELETE FROM simple_table WHERE non_existent = 1;"),
        ]

        for test_name, sql in error_deletions:
            self.run_test(test_name, sql)

    # =========================== 高级功能测试 ===========================

    def test_index_operations(self):
        """测试索引操作"""
        self.log("=== 测试索引操作 ===")

        # 注意：根据代码分析，索引是自动创建的，我们主要测试使用索引的查询
        index_tests = [
            ("索引查询测试1", "SELECT * FROM test_table1 WHERE student_id = 1;"),
            ("索引范围查询", "SELECT * FROM test_table1 WHERE student_id > 50;"),
            (
                "复合条件索引查询",
                "SELECT * FROM test_table1 WHERE student_id = 1 AND grade > 80;",
            ),
        ]

        for test_name, sql in index_tests:
            self.run_test(test_name, sql)

    def test_explain_queries(self):
        """测试EXPLAIN功能"""
        self.log("=== 测试EXPLAIN功能 ===")

        explain_tests = [
            ("解释简单查询", "EXPLAIN SELECT * FROM simple_table;"),
            (
                "解释复杂查询",
                "EXPLAIN SELECT * FROM test_table1 WHERE grade > 85 ORDER BY student_id;",
            ),
            (
                "解释JOIN查询",
                "EXPLAIN SELECT t1.*, t2.* FROM test_table1 t1 JOIN test_table2 t2 ON t1.student_id = t2.course_id;",
            ),
        ]

        for test_name, sql in explain_tests:
            self.run_test(test_name, sql)

    def test_describe_tables(self):
        """测试表结构查看"""
        self.log("=== 测试DESCRIBE功能 ===")

        for table in self.created_tables:
            self.run_test(f"描述表{table}", f"DESCRIBE {table};")
            self.run_test(f"DESC表{table}", f"DESC {table};")

    # =========================== 边界条件和错误测试 ===========================

    def test_sql_injection_attempts(self):
        """测试SQL注入防护"""
        self.log("=== 测试SQL注入防护 ===")

        injection_attempts = [
            (
                "单引号注入",
                "SELECT * FROM simple_table WHERE name = 'Alice'; DROP TABLE simple_table; --';",
            ),
            (
                "联合查询注入",
                "SELECT * FROM simple_table WHERE id = 1 UNION SELECT * FROM test_table1;",
            ),
            (
                "注释注入",
                "SELECT * FROM simple_table WHERE id = 1 /* comment */ AND name = 'test';",
            ),
            ("分号注入", "SELECT * FROM simple_table; DELETE FROM simple_table;"),
        ]

        for test_name, sql in injection_attempts:
            self.run_test(test_name, sql)

    def test_malformed_sql(self):
        """测试畸形SQL语句"""
        self.log("=== 测试畸形SQL ===")

        malformed_sqls = [
            ("无分号", "SELECT * FROM simple_table"),
            ("语法错误1", "SELCT * FROM simple_table;"),
            ("语法错误2", "SELECT * FORM simple_table;"),
            ("缺少FROM", "SELECT *;"),
            ("空SQL", ""),
            ("只有分号", ";"),
            ("不完整的CREATE", "CREATE TABLE"),
            ("不完整的INSERT", "INSERT INTO"),
            ("不完整的SELECT", "SELECT"),
            ("无效字符", "SELECT * FROM simple_table WHERE id = 1 @#$%^&*();"),
        ]

        for test_name, sql in malformed_sqls:
            self.run_test(test_name, sql)

    def test_edge_case_values(self):
        """测试边界值"""
        self.log("=== 测试边界值 ===")

        # 先创建测试用的表
        self.run_test(
            "创建边界测试表",
            """
            CREATE TABLE boundary_test (
                int_col INTEGER,
                char_col CHAR(10),
                float_col FLOAT,
                double_col DOUBLE
            );
        """,
        )

        boundary_tests = [
            (
                "插入最大整数",
                "INSERT INTO boundary_test VALUES (2147483647, 'max', 1.0, 1.0);",
            ),
            (
                "插入最小整数",
                "INSERT INTO boundary_test VALUES (-2147483648, 'min', 1.0, 1.0);",
            ),
            ("插入零值", "INSERT INTO boundary_test VALUES (0, '', 0.0, 0.0);"),
            (
                "插入最大字符串",
                f"INSERT INTO boundary_test VALUES (1, '{'x' * 10}', 1.0, 1.0);",
            ),
            (
                "插入特殊字符",
                "INSERT INTO boundary_test VALUES (2, 'áéíóú', 1.0, 1.0);",
            ),
            (
                "插入很大的浮点数",
                "INSERT INTO boundary_test VALUES (3, 'big', 999999.999, 999999999.999999);",
            ),
            (
                "插入很小的浮点数",
                "INSERT INTO boundary_test VALUES (4, 'small', 0.000001, 0.000000000001);",
            ),
        ]

        for test_name, sql in boundary_tests:
            self.run_test(test_name, sql)

    # =========================== 并发和性能测试 ===========================

    def test_concurrent_operations(self):
        """测试并发操作"""
        self.log("=== 测试并发操作 ===")

        # 创建并发测试表
        self.run_test(
            "创建并发测试表",
            """
            CREATE TABLE concurrent_test (
                id INTEGER,
                thread_id INTEGER,
                timestamp CHAR(20)
            );
        """,
        )

        def concurrent_insert(thread_id: int, count: int):
            results = []
            for i in range(count):
                sql = f"INSERT INTO concurrent_test VALUES ({i}, {thread_id}, '{time.time()}');"
                result, exec_time, status = self.execute_sql(sql)
                results.append((result.get("status") == "success", exec_time))
            return results

        # 启动多个线程并发插入
        num_threads = 5
        inserts_per_thread = 20

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            start_time = time.time()

            for thread_id in range(num_threads):
                future = executor.submit(
                    concurrent_insert, thread_id, inserts_per_thread
                )
                futures.append(future)

            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

            total_time = time.time() - start_time

        success_count = sum(1 for success, _ in all_results if success)
        total_operations = num_threads * inserts_per_thread
        avg_time = sum(exec_time for _, exec_time in all_results) / len(all_results)

        self.log(f"并发测试结果: {success_count}/{total_operations} 成功")
        self.log(f"总耗时: {total_time:.3f}s, 平均单操作耗时: {avg_time:.3f}s")
        self.log(f"吞吐量: {success_count/total_time:.2f} ops/sec")

    def test_performance_stress(self):
        """性能压力测试"""
        self.log("=== 性能压力测试 ===")

        # 创建性能测试表
        self.run_test(
            "创建性能测试表",
            """
            CREATE TABLE performance_test (
                id INTEGER,
                data CHAR(50),
                value FLOAT
            );
        """,
        )

        # 插入大量数据
        batch_size = 50
        num_batches = 10

        for batch in range(num_batches):
            values = []
            for i in range(batch_size):
                record_id = batch * batch_size + i
                data = f"test_data_{record_id}"
                value = random.uniform(0, 1000)
                values.append(f"({record_id}, '{data}', {value})")

            sql = f"INSERT INTO performance_test VALUES {', '.join(values)};"
            result = self.run_test(f"批量插入第{batch+1}批", sql)

            if not result.success:
                self.log(f"批量插入失败，停止性能测试", "ERROR")
                break

        # 性能查询测试
        perf_queries = [
            ("全表扫描", "SELECT * FROM performance_test;"),
            ("条件查询", "SELECT * FROM performance_test WHERE id > 250;"),
            (
                "聚合查询",
                "SELECT COUNT(*), AVG(value), MAX(value), MIN(value) FROM performance_test;",
            ),
            ("排序查询", "SELECT * FROM performance_test ORDER BY value DESC;"),
        ]

        for test_name, sql in perf_queries:
            self.run_test(test_name, sql)

    # =========================== HTTP API特定测试 ===========================

    def test_http_api_features(self):
        """测试HTTP API特定功能"""
        self.log("=== 测试HTTP API功能 ===")

        # URL编码测试
        special_queries = [
            ("URL编码测试", "SELECT * FROM simple_table WHERE name = 'Alice & Bob';"),
            (
                "特殊字符查询",
                "SELECT * FROM simple_table WHERE name LIKE '%特殊字符%';",
            ),
            (
                "多行SQL",
                """
                SELECT * FROM simple_table;
                SELECT COUNT(*) FROM test_table1;
            """,
            ),
        ]

        for test_name, sql in special_queries:
            self.run_test(test_name, sql)

        # POST请求测试
        self.test_post_requests()

    def test_post_requests(self):
        """测试POST请求"""
        self.log("=== 测试POST请求 ===")

        try:
            # 测试POST请求
            sql = "SELECT COUNT(*) FROM simple_table;"
            response = requests.post(
                self.base_url,
                data={"sql": sql},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )

            result = TestResult(
                test_name="POST请求测试",
                sql=sql,
                success=response.status_code == 200,
                response_time=0,
                status_code=response.status_code,
                response=(
                    response.json()
                    if response.status_code == 200
                    else {"error": response.text}
                ),
            )

            self.test_results.append(result)
            status = "✅ PASS" if result.success else "❌ FAIL"
            self.log(f"{status} POST请求测试")

        except Exception as e:
            self.log(f"POST请求测试失败: {e}", "ERROR")

    # =========================== 清理和统计 ===========================

    def cleanup_tables(self):
        """清理测试表"""
        self.log("=== 清理测试表 ===")

        cleanup_tables = ["boundary_test", "concurrent_test", "performance_test"]

        for table in cleanup_tables:
            self.run_test(f"删除表{table}", f"DROP TABLE {table};")

        # 清理主要测试表
        for table in self.created_tables:
            self.run_test(f"删除表{table}", f"DROP TABLE {table};")

    def generate_report(self):
        """生成测试报告"""
        self.log("=== 生成测试报告 ===")

        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result.success)
        failed_tests = total_tests - passed_tests

        avg_response_time = (
            sum(result.response_time for result in self.test_results) / total_tests
            if total_tests > 0
            else 0
        )

        print("\n" + "=" * 80)
        print("📊 CS307数据库Fuzz测试报告")
        print("=" * 80)
        print(f"总测试数: {total_tests}")
        print(f"通过测试: {passed_tests} ({passed_tests/total_tests*100:.1f}%)")
        print(f"失败测试: {failed_tests} ({failed_tests/total_tests*100:.1f}%)")
        print(f"平均响应时间: {avg_response_time:.3f}s")
        print("=" * 80)

        # 失败测试详情
        if failed_tests > 0:
            print("\n❌ 失败的测试:")
            print("-" * 60)
            for result in self.test_results:
                if not result.success:
                    print(f"• {result.test_name}")
                    print(f"  SQL: {result.sql[:100]}...")
                    print(f"  错误: {result.error_message}")
                    print()

        # 性能统计
        slow_tests = [r for r in self.test_results if r.response_time > 1.0]
        if slow_tests:
            print(f"\n⏱️ 慢查询 (>1s):")
            print("-" * 60)
            for result in sorted(
                slow_tests, key=lambda x: x.response_time, reverse=True
            )[:10]:
                print(f"• {result.test_name}: {result.response_time:.3f}s")

        # 保存详细报告到文件
        self.save_detailed_report()

    def save_detailed_report(self):
        """保存详细报告到文件"""
        report_file = f"fuzz_test_report_{int(time.time())}.json"

        report_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_tests": len(self.test_results),
                "passed": sum(1 for r in self.test_results if r.success),
                "failed": sum(1 for r in self.test_results if not r.success),
                "avg_response_time": sum(r.response_time for r in self.test_results)
                / len(self.test_results),
            },
            "test_results": [
                {
                    "test_name": r.test_name,
                    "sql": r.sql,
                    "success": r.success,
                    "response_time": r.response_time,
                    "status_code": r.status_code,
                    "response": r.response,
                    "error_message": r.error_message,
                }
                for r in self.test_results
            ],
        }

        try:
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            self.log(f"详细报告已保存到: {report_file}")
        except Exception as e:
            self.log(f"保存报告失败: {e}", "ERROR")

    # =========================== 主测试流程 ===========================

    def run_all_tests(self):
        """运行所有测试"""
        start_time = time.time()

        try:
            self.log("🚀 开始CS307数据库全面Fuzz测试")

            # 基础功能测试
            self.test_basic_commands()
            self.test_table_creation()
            self.test_data_insertion()
            self.test_data_selection()
            self.test_data_updates()
            self.test_data_deletion()

            # 高级功能测试
            self.test_index_operations()
            self.test_explain_queries()
            self.test_describe_tables()

            # 边界条件和错误测试
            self.test_sql_injection_attempts()
            self.test_malformed_sql()
            self.test_edge_case_values()

            # 并发和性能测试
            self.test_concurrent_operations()
            self.test_performance_stress()

            # HTTP API测试
            self.test_http_api_features()

            # 清理
            self.cleanup_tables()

        except KeyboardInterrupt:
            self.log("测试被用户中断", "WARNING")
        except Exception as e:
            self.log(f"测试过程中发生错误: {e}", "ERROR")
        finally:
            total_time = time.time() - start_time
            self.log(f"测试完成，总耗时: {total_time:.2f}s")
            self.generate_report()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="CS307数据库Fuzz测试工具")
    parser.add_argument(
        "--url", default="http://localhost:8080", help="数据库HTTP API地址"
    )
    parser.add_argument("--quick", action="store_true", help="快速测试模式")

    args = parser.parse_args()

    tester = DatabaseFuzzTester(args.url)

    # 检查数据库连接
    try:
        response, _, status = tester.execute_sql("HELP;")
        if status != 200:
            print(f"❌ 无法连接到数据库: {args.url}")
            print("请确保数据库服务正在运行")
            return
        print(f"✅ 成功连接到数据库: {args.url}")
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return

    if args.quick:
        print("🏃 快速测试模式")
        # 只运行基础测试
        tester.test_basic_commands()
        tester.test_table_creation()
        tester.test_data_insertion()
        tester.test_data_selection()
        tester.cleanup_tables()
        tester.generate_report()
    else:
        # 运行完整测试
        tester.run_all_tests()


if __name__ == "__main__":
    main()
