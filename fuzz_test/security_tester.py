#!/usr/bin/env python3
"""
SQL注入和安全性测试模块
========================

专门测试数据库对恶意SQL输入的抵抗能力和安全性
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SecurityTestCase:
    name: str
    sql: str
    expected_behavior: str  # 'blocked', 'error', 'safe_execution'
    description: str


class SecurityTester:
    """SQL注入和安全性测试器"""

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL并返回结果"""
        try:
            params = {"sql": sql}
            async with self.session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return {"status": "error", "message": f"HTTP {response.status}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_sql_injection_test_cases(self) -> List[SecurityTestCase]:
        """获取SQL注入测试用例"""
        return [
            # 经典SQL注入尝试
            SecurityTestCase(
                "Classic OR Injection",
                "SELECT * FROM users WHERE id = 1 OR 1=1;",
                "error",
                "尝试通过OR 1=1绕过WHERE条件",
            ),
            SecurityTestCase(
                "Union-based Injection",
                "SELECT name FROM users UNION SELECT password FROM admin;",
                "error",
                "尝试通过UNION获取额外数据",
            ),
            SecurityTestCase(
                "Comment Injection",
                "SELECT * FROM users WHERE name = 'admin'--';",
                "error",
                "使用注释符绕过后续条件",
            ),
            SecurityTestCase(
                "Stacked Queries",
                "SELECT * FROM users; DROP TABLE users;",
                "error",
                "尝试执行多个语句",
            ),
            SecurityTestCase(
                "Time-based Blind Injection",
                "SELECT * FROM users WHERE id = 1 AND (SELECT COUNT(*) FROM users) > 0;",
                "safe_execution",
                "时间盲注尝试",
            ),
            # 特殊字符测试
            SecurityTestCase(
                "Special Characters",
                "SELECT * FROM test WHERE name = 'test'; DELETE FROM test; --';",
                "error",
                "包含分号和删除语句的输入",
            ),
            SecurityTestCase(
                "Null Byte Injection",
                "SELECT * FROM users WHERE name = 'admin'\\0';",
                "error",
                "空字节注入尝试",
            ),
            # 函数调用注入
            SecurityTestCase(
                "Function Injection",
                "SELECT * FROM users WHERE id = LOAD_FILE('/etc/passwd');",
                "error",
                "尝试调用系统函数",
            ),
            SecurityTestCase(
                "Hex Encoding",
                "SELECT * FROM users WHERE name = 0x61646D696E;",
                "safe_execution",
                "十六进制编码输入",
            ),
            # 极长输入
            SecurityTestCase(
                "Buffer Overflow Attempt",
                "SELECT * FROM users WHERE name = '" + "A" * 10000 + "';",
                "error",
                "尝试缓冲区溢出",
            ),
        ]

    def get_data_leakage_test_cases(self) -> List[SecurityTestCase]:
        """获取数据泄露测试用例"""
        return [
            SecurityTestCase(
                "Information Schema Access",
                "SELECT * FROM information_schema.tables;",
                "error",
                "尝试访问系统表",
            ),
            SecurityTestCase(
                "Error-based Information Disclosure",
                "SELECT * FROM non_existent_table_xyz123;",
                "error",
                "通过错误消息获取信息",
            ),
            SecurityTestCase(
                "Version Disclosure",
                "SELECT VERSION();",
                "error",
                "尝试获取数据库版本信息",
            ),
        ]

    async def run_security_tests(self) -> Dict[str, Any]:
        """运行所有安全测试"""
        logger.info("开始SQL注入和安全性测试")

        # 首先创建测试表
        await self.setup_test_environment()

        all_test_cases = (
            self.get_sql_injection_test_cases() + self.get_data_leakage_test_cases()
        )

        results = {
            "total_tests": len(all_test_cases),
            "passed_tests": 0,
            "failed_tests": 0,
            "test_results": [],
        }

        for test_case in all_test_cases:
            logger.info(f"执行安全测试: {test_case.name}")

            result = await self.execute_sql(test_case.sql)

            # 分析结果
            test_passed = self.evaluate_security_test(test_case, result)

            test_result = {
                "name": test_case.name,
                "description": test_case.description,
                "sql": test_case.sql,
                "expected": test_case.expected_behavior,
                "actual_status": result.get("status", "unknown"),
                "passed": test_passed,
                "response": result,
            }

            results["test_results"].append(test_result)

            if test_passed:
                results["passed_tests"] += 1
                logger.info(f"✅ {test_case.name} - 通过")
            else:
                results["failed_tests"] += 1
                logger.warning(f"❌ {test_case.name} - 失败")

        # 清理测试环境
        await self.cleanup_test_environment()

        return results

    def evaluate_security_test(
        self, test_case: SecurityTestCase, result: Dict[str, Any]
    ) -> bool:
        """评估安全测试结果"""
        status = result.get("status", "unknown")

        if test_case.expected_behavior == "error":
            # 期望出错，实际也出错 = 通过
            return status == "error"
        elif test_case.expected_behavior == "blocked":
            # 期望被阻止，实际出错或被阻止 = 通过
            return status in ["error", "blocked"]
        elif test_case.expected_behavior == "safe_execution":
            # 期望安全执行，实际成功但没有泄露数据 = 通过
            return status == "success" and not self.detect_data_leakage(result)

        return False

    def detect_data_leakage(self, result: Dict[str, Any]) -> bool:
        """检测是否有数据泄露"""
        # 检查返回的数据中是否包含敏感信息
        data = result.get("data", [])
        if not data:
            return False

        # 检查是否返回了过多的数据行
        if len(data) > 1000:
            return True

        # 检查是否包含系统信息
        for row in data:
            for key, value in row.items():
                if isinstance(value, str):
                    sensitive_keywords = [
                        "password",
                        "admin",
                        "root",
                        "system",
                        "schema",
                    ]
                    if any(
                        keyword in str(value).lower() for keyword in sensitive_keywords
                    ):
                        return True

        return False

    async def setup_test_environment(self):
        """设置测试环境"""
        try:
            # 创建测试表
            create_sql = """
            CREATE TABLE security_test_users (
                id INTEGER,
                name CHAR,
                email CHAR,
                role CHAR
            );
            """
            await self.execute_sql(create_sql)

            # 插入测试数据
            test_data = [
                (1, "admin", "admin@test.com", "administrator"),
                (2, "user1", "user1@test.com", "user"),
                (3, "guest", "guest@test.com", "guest"),
            ]

            for user_id, name, email, role in test_data:
                insert_sql = f"INSERT INTO security_test_users VALUES ({user_id}, '{name}', '{email}', '{role}');"
                await self.execute_sql(insert_sql)

        except Exception as e:
            logger.warning(f"测试环境设置失败: {e}")

    async def cleanup_test_environment(self):
        """清理测试环境"""
        try:
            drop_sql = "DROP TABLE security_test_users;"
            await self.execute_sql(drop_sql)
        except Exception as e:
            logger.warning(f"测试环境清理失败: {e}")


async def main():
    """主函数"""
    print("SQL注入和安全性测试")
    print("==================")

    async with SecurityTester() as tester:
        results = await tester.run_security_tests()

        # 生成报告
        print(f"\n安全测试完成:")
        print(f"总测试数: {results['total_tests']}")
        print(f"通过: {results['passed_tests']}")
        print(f"失败: {results['failed_tests']}")
        print(f"成功率: {results['passed_tests']/results['total_tests']:.1%}")

        # 详细结果
        print("\n详细结果:")
        for test in results["test_results"]:
            status = "✅" if test["passed"] else "❌"
            print(f"{status} {test['name']}: {test['description']}")
            if not test["passed"]:
                print(f"   期望: {test['expected']}, 实际: {test['actual_status']}")


if __name__ == "__main__":
    asyncio.run(main())
