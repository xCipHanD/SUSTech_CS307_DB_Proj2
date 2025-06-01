#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CS307数据库项目 - 性能基准测试工具
专门用于测试数据库的性能指标和基准
"""

import requests
import json
import time
import random
import string
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple
import urllib.parse
import matplotlib.pyplot as plt
import pandas as pd
from dataclasses import dataclass


@dataclass
class PerformanceResult:
    test_name: str
    operation_type: str
    record_count: int
    execution_time: float
    throughput: float  # operations per second
    success: bool
    error_message: str = None


class PerformanceBenchmark:
    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url
        self.results: List[PerformanceResult] = []
        self.test_data_created = False

    def log(self, message: str, level: str = "INFO"):
        """日志输出"""
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")

    def execute_sql(
        self, sql: str, timeout: int = 60
    ) -> Tuple[Dict[str, Any], float, bool]:
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
                success = response.status_code == 200 and result.get("status") in [
                    "success",
                    "help",
                ]
                return result, execution_time, success
            except json.JSONDecodeError:
                return (
                    {"status": "error", "message": "Invalid JSON"},
                    execution_time,
                    False,
                )

        except Exception as e:
            execution_time = time.time() - start_time
            return {"status": "error", "message": str(e)}, execution_time, False

    def create_test_tables(self):
        """创建性能测试所需的表"""
        self.log("创建性能测试表...")

        tables = [
            """CREATE TABLE perf_small (
                id INTEGER,
                name CHAR(32),
                value FLOAT,
                category INTEGER
            );""",
            """CREATE TABLE perf_medium (
                id INTEGER,
                title CHAR(64),
                content CHAR(128),
                score DOUBLE,
                created_at CHAR(20)
            );""",
            """CREATE TABLE perf_large (
                student_id INTEGER,
                student_name CHAR(64),
                course_id INTEGER,
                course_name CHAR(64),
                grade FLOAT,
                semester CHAR(16)
            );""",
            """CREATE TABLE perf_index_test (
                pk_id INTEGER,
                indexed_col INTEGER,
                search_field CHAR(32),
                data_payload CHAR(100)
            );""",
        ]

        for i, sql in enumerate(tables, 1):
            result, exec_time, success = self.execute_sql(sql)
            if success:
                self.log(f"✅ 创建测试表 {i}/4 成功")
            else:
                self.log(
                    f"❌ 创建测试表 {i}/4 失败: {result.get('message', 'Unknown error')}",
                    "ERROR",
                )
                return False

        self.test_data_created = True
        return True

    def generate_test_data(self, table_name: str, record_count: int) -> List[str]:
        """生成测试数据"""
        data = []

        if table_name == "perf_small":
            for i in range(record_count):
                name = f"item_{i:06d}"
                value = round(random.uniform(0, 1000), 2)
                category = random.randint(1, 10)
                data.append(f"({i}, '{name}', {value}, {category})")

        elif table_name == "perf_medium":
            for i in range(record_count):
                title = f"Title_{i:06d}_{''.join(random.choices(string.ascii_letters, k=10))}"
                content = f"Content_{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"
                score = round(random.uniform(0, 100), 3)
                created_at = f"2024-01-{(i % 28) + 1:02d}"
                data.append(f"({i}, '{title}', '{content}', {score}, '{created_at}')")

        elif table_name == "perf_large":
            courses = [
                "Math",
                "Physics",
                "Chemistry",
                "Biology",
                "History",
                "English",
                "Computer Science",
            ]
            semesters = ["2023Fall", "2024Spring", "2024Fall"]

            for i in range(record_count):
                student_name = (
                    f"Student_{''.join(random.choices(string.ascii_letters, k=8))}"
                )
                course_id = random.randint(1, 100)
                course_name = random.choice(courses)
                grade = round(random.uniform(60, 100), 2)
                semester = random.choice(semesters)
                data.append(
                    f"({i}, '{student_name}', {course_id}, '{course_name}', {grade}, '{semester}')"
                )

        elif table_name == "perf_index_test":
            for i in range(record_count):
                indexed_col = i % 1000  # 创建重复值以测试索引效果
                search_field = f"search_{i % 100:03d}"
                payload = (
                    f"payload_{''.join(random.choices(string.ascii_letters, k=20))}"
                )
                data.append(f"({i}, {indexed_col}, '{search_field}', '{payload}')")

        return data

    def benchmark_insert_performance(self):
        """基准测试：插入性能"""
        self.log("=== 插入性能基准测试 ===")

        test_configs = [
            ("小表单条插入", "perf_small", 100, 1),
            ("小表批量插入", "perf_small", 1000, 50),
            ("中表批量插入", "perf_medium", 500, 25),
            ("大表批量插入", "perf_large", 1000, 100),
        ]

        for test_name, table_name, total_records, batch_size in test_configs:
            self.log(f"运行测试: {test_name}")

            total_time = 0
            successful_inserts = 0

            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_data = self.generate_test_data(table_name, end_idx - start_idx)

                # 调整ID以避免重复
                adjusted_data = []
                for i, data_str in enumerate(batch_data):
                    new_id = start_idx + i + 1000  # 偏移避免冲突
                    adjusted_data.append(data_str.replace(f"({i},", f"({new_id},", 1))

                sql = f"INSERT INTO {table_name} VALUES {', '.join(adjusted_data)};"

                result, exec_time, success = self.execute_sql(sql)
                total_time += exec_time

                if success:
                    successful_inserts += len(adjusted_data)
                else:
                    self.log(
                        f"批量插入失败: {result.get('message', 'Unknown error')}",
                        "ERROR",
                    )

            throughput = successful_inserts / total_time if total_time > 0 else 0

            perf_result = PerformanceResult(
                test_name=test_name,
                operation_type="INSERT",
                record_count=successful_inserts,
                execution_time=total_time,
                throughput=throughput,
                success=successful_inserts > 0,
            )

            self.results.append(perf_result)
            self.log(
                f"✅ {test_name}: {successful_inserts} 条记录, {total_time:.3f}s, {throughput:.2f} ops/sec"
            )

    def benchmark_select_performance(self):
        """基准测试：查询性能"""
        self.log("=== 查询性能基准测试 ===")

        queries = [
            ("全表扫描小表", "SELECT * FROM perf_small;"),
            ("条件查询小表", "SELECT * FROM perf_small WHERE category = 5;"),
            (
                "聚合查询小表",
                "SELECT COUNT(*), AVG(value), MAX(value) FROM perf_small;",
            ),
            ("全表扫描中表", "SELECT * FROM perf_medium;"),
            ("条件查询中表", "SELECT * FROM perf_medium WHERE score > 80;"),
            ("排序查询中表", "SELECT * FROM perf_medium ORDER BY score DESC;"),
            (
                "复杂条件查询",
                "SELECT * FROM perf_large WHERE grade > 85 AND course_id < 50;",
            ),
            (
                "JOIN查询",
                """
                SELECT p1.student_name, p2.title 
                FROM perf_large p1 
                JOIN perf_medium p2 ON p1.student_id = p2.id 
                LIMIT 100;
            """,
            ),
        ]

        for test_name, sql in queries:
            # 运行多次取平均值
            times = []
            for _ in range(5):
                result, exec_time, success = self.execute_sql(sql)
                if success:
                    times.append(exec_time)
                else:
                    self.log(
                        f"查询失败 {test_name}: {result.get('message', 'Unknown error')}",
                        "ERROR",
                    )
                    break

            if times:
                avg_time = statistics.mean(times)
                throughput = 1 / avg_time if avg_time > 0 else 0

                perf_result = PerformanceResult(
                    test_name=test_name,
                    operation_type="SELECT",
                    record_count=1,  # 单次查询
                    execution_time=avg_time,
                    throughput=throughput,
                    success=True,
                )

                self.results.append(perf_result)
                self.log(
                    f"✅ {test_name}: 平均 {avg_time:.3f}s ({min(times):.3f}-{max(times):.3f}s)"
                )

    def benchmark_update_performance(self):
        """基准测试：更新性能"""
        self.log("=== 更新性能基准测试 ===")

        updates = [
            ("单条更新", "UPDATE perf_small SET value = 999.99 WHERE id = 1001;"),
            ("批量更新", "UPDATE perf_small SET category = 99 WHERE category <= 5;"),
            (
                "条件更新",
                "UPDATE perf_medium SET score = score * 1.1 WHERE score < 70;",
            ),
            ("全表更新", "UPDATE perf_small SET name = CONCAT(name, '_updated');"),
        ]

        for test_name, sql in updates:
            result, exec_time, success = self.execute_sql(sql)

            if success:
                throughput = 1 / exec_time if exec_time > 0 else 0
                affected_rows = result.get("affected_rows", 0)

                perf_result = PerformanceResult(
                    test_name=test_name,
                    operation_type="UPDATE",
                    record_count=affected_rows,
                    execution_time=exec_time,
                    throughput=throughput,
                    success=True,
                )

                self.results.append(perf_result)
                self.log(f"✅ {test_name}: {exec_time:.3f}s, 影响 {affected_rows} 行")
            else:
                self.log(
                    f"更新失败 {test_name}: {result.get('message', 'Unknown error')}",
                    "ERROR",
                )

    def benchmark_concurrent_performance(self):
        """基准测试：并发性能"""
        self.log("=== 并发性能基准测试 ===")

        def concurrent_operations(thread_id: int, operation_count: int) -> List[float]:
            times = []
            for i in range(operation_count):
                sql = f"INSERT INTO perf_index_test VALUES ({thread_id * 1000 + i}, {i % 100}, 'thread_{thread_id}_{i}', 'concurrent_test_data');"
                _, exec_time, success = self.execute_sql(sql)
                if success:
                    times.append(exec_time)
            return times

        # 并发插入测试
        thread_counts = [1, 2, 4, 8]
        operations_per_thread = 50

        for num_threads in thread_counts:
            self.log(f"测试 {num_threads} 个并发线程...")

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                start_time = time.time()

                futures = []
                for thread_id in range(num_threads):
                    future = executor.submit(
                        concurrent_operations, thread_id, operations_per_thread
                    )
                    futures.append(future)

                all_times = []
                for future in as_completed(futures):
                    thread_times = future.result()
                    all_times.extend(thread_times)

                total_time = time.time() - start_time

            if all_times:
                total_operations = len(all_times)
                avg_time = statistics.mean(all_times)
                throughput = total_operations / total_time

                perf_result = PerformanceResult(
                    test_name=f"并发插入_{num_threads}线程",
                    operation_type="CONCURRENT_INSERT",
                    record_count=total_operations,
                    execution_time=total_time,
                    throughput=throughput,
                    success=True,
                )

                self.results.append(perf_result)
                self.log(
                    f"✅ {num_threads} 线程: {total_operations} 操作, {total_time:.3f}s, {throughput:.2f} ops/sec"
                )

    def benchmark_index_performance(self):
        """基准测试：索引性能"""
        self.log("=== 索引性能基准测试 ===")

        # 首先填充索引测试表
        self.log("填充索引测试数据...")
        test_data = self.generate_test_data("perf_index_test", 1000)

        batch_size = 100
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i : i + batch_size]
            sql = f"INSERT INTO perf_index_test VALUES {', '.join(batch)};"
            self.execute_sql(sql)

        # 测试索引查询性能
        index_queries = [
            (
                "等值查询_可能使用索引",
                "SELECT * FROM perf_index_test WHERE pk_id = 500;",
            ),
            (
                "范围查询_可能使用索引",
                "SELECT * FROM perf_index_test WHERE pk_id BETWEEN 100 AND 200;",
            ),
            (
                "非索引列查询",
                "SELECT * FROM perf_index_test WHERE search_field = 'search_050';",
            ),
            (
                "复合条件查询",
                "SELECT * FROM perf_index_test WHERE pk_id > 500 AND indexed_col < 100;",
            ),
        ]

        for test_name, sql in index_queries:
            times = []
            for _ in range(10):  # 多次运行取平均
                result, exec_time, success = self.execute_sql(sql)
                if success:
                    times.append(exec_time)

            if times:
                avg_time = statistics.mean(times)
                throughput = 1 / avg_time if avg_time > 0 else 0

                perf_result = PerformanceResult(
                    test_name=test_name,
                    operation_type="INDEX_QUERY",
                    record_count=1,
                    execution_time=avg_time,
                    throughput=throughput,
                    success=True,
                )

                self.results.append(perf_result)
                self.log(f"✅ {test_name}: 平均 {avg_time:.4f}s")

    def cleanup_test_data(self):
        """清理测试数据"""
        self.log("清理测试数据...")

        tables = ["perf_small", "perf_medium", "perf_large", "perf_index_test"]
        for table in tables:
            result, _, success = self.execute_sql(f"DROP TABLE {table};")
            if success:
                self.log(f"✅ 删除表 {table}")
            else:
                self.log(f"❌ 删除表 {table} 失败", "ERROR")

    def generate_performance_report(self):
        """生成性能报告"""
        self.log("=== 生成性能报告 ===")

        if not self.results:
            self.log("没有性能测试结果", "WARNING")
            return

        print("\n" + "=" * 80)
        print("📊 CS307数据库性能基准测试报告")
        print("=" * 80)

        # 按操作类型分组报告
        operation_types = {}
        for result in self.results:
            op_type = result.operation_type
            if op_type not in operation_types:
                operation_types[op_type] = []
            operation_types[op_type].append(result)

        for op_type, results in operation_types.items():
            print(f"\n📈 {op_type} 性能:")
            print("-" * 60)

            for result in results:
                if result.success:
                    print(f"• {result.test_name}")
                    print(f"  执行时间: {result.execution_time:.3f}s")
                    print(f"  吞吐量: {result.throughput:.2f} ops/sec")
                    if result.record_count > 1:
                        print(f"  处理记录: {result.record_count} 条")
                    print()

        # 性能统计汇总
        total_tests = len(self.results)
        successful_tests = sum(1 for r in self.results if r.success)
        avg_throughput = statistics.mean(
            [r.throughput for r in self.results if r.success]
        )

        print(f"\n📋 汇总统计:")
        print(f"总测试数: {total_tests}")
        print(f"成功测试: {successful_tests}")
        print(f"平均吞吐量: {avg_throughput:.2f} ops/sec")

        # 找出性能最好和最差的测试
        if successful_tests > 0:
            fastest = max(
                [r for r in self.results if r.success], key=lambda x: x.throughput
            )
            slowest = min(
                [r for r in self.results if r.success], key=lambda x: x.throughput
            )

            print(
                f"\n🏆 最快操作: {fastest.test_name} ({fastest.throughput:.2f} ops/sec)"
            )
            print(
                f"🐌 最慢操作: {slowest.test_name} ({slowest.throughput:.2f} ops/sec)"
            )

        self.save_performance_report()

    def save_performance_report(self):
        """保存性能报告到文件"""
        report_file = f"performance_report_{int(time.time())}.json"

        report_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_tests": len(self.results),
                "successful_tests": sum(1 for r in self.results if r.success),
                "avg_throughput": (
                    statistics.mean([r.throughput for r in self.results if r.success])
                    if self.results
                    else 0
                ),
            },
            "results": [
                {
                    "test_name": r.test_name,
                    "operation_type": r.operation_type,
                    "record_count": r.record_count,
                    "execution_time": r.execution_time,
                    "throughput": r.throughput,
                    "success": r.success,
                    "error_message": r.error_message,
                }
                for r in self.results
            ],
        }

        try:
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            self.log(f"性能报告已保存到: {report_file}")
        except Exception as e:
            self.log(f"保存报告失败: {e}", "ERROR")

    def run_all_benchmarks(self):
        """运行所有性能基准测试"""
        start_time = time.time()

        try:
            self.log("🚀 开始CS307数据库性能基准测试")

            # 创建测试表
            if not self.create_test_tables():
                self.log("无法创建测试表，退出测试", "ERROR")
                return

            # 运行各项性能测试
            self.benchmark_insert_performance()
            self.benchmark_select_performance()
            self.benchmark_update_performance()
            self.benchmark_concurrent_performance()
            self.benchmark_index_performance()

            # 清理
            self.cleanup_test_data()

        except KeyboardInterrupt:
            self.log("测试被用户中断", "WARNING")
        except Exception as e:
            self.log(f"测试过程中发生错误: {e}", "ERROR")
        finally:
            total_time = time.time() - start_time
            self.log(f"性能测试完成，总耗时: {total_time:.2f}s")
            self.generate_performance_report()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="CS307数据库性能基准测试工具")
    parser.add_argument(
        "--url", default="http://localhost:8080", help="数据库HTTP API地址"
    )
    parser.add_argument(
        "--test",
        choices=["insert", "select", "update", "concurrent", "index", "all"],
        default="all",
        help="指定要运行的测试类型",
    )

    args = parser.parse_args()

    benchmark = PerformanceBenchmark(args.url)

    # 检查数据库连接
    try:
        result, _, success = benchmark.execute_sql("HELP;")
        if not success:
            print(f"❌ 无法连接到数据库: {args.url}")
            return
        print(f"✅ 成功连接到数据库: {args.url}")
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return

    # 创建测试表
    if not benchmark.create_test_tables():
        return

    # 根据参数运行指定测试
    try:
        if args.test == "insert":
            benchmark.benchmark_insert_performance()
        elif args.test == "select":
            benchmark.benchmark_select_performance()
        elif args.test == "update":
            benchmark.benchmark_update_performance()
        elif args.test == "concurrent":
            benchmark.benchmark_concurrent_performance()
        elif args.test == "index":
            benchmark.benchmark_index_performance()
        else:
            benchmark.run_all_benchmarks()
    finally:
        benchmark.cleanup_test_data()
        benchmark.generate_performance_report()


if __name__ == "__main__":
    main()
