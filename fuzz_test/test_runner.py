#!/usr/bin/env python3
"""
数据库Fuzz测试运行器
===================

统一的测试运行器，可以运行各种类型的测试
"""

import argparse
import asyncio
import sys
import time
import subprocess
import os
from pathlib import Path


def check_database_server():
    """检查数据库服务器是否运行"""
    import requests

    try:
        response = requests.get(
            "http://localhost:8080", params={"sql": "SHOW TABLES;"}, timeout=5
        )
        return response.status_code == 200
    except:
        return False


def start_database_server():
    """启动数据库服务器"""
    print("正在启动数据库服务器...")
    try:
        # 切换到项目根目录
        project_root = Path(__file__).parent.parent
        os.chdir(project_root)

        # 使用Maven运行数据库
        process = subprocess.Popen(
            ["mvn", "exec:java", "-Dexec.mainClass=edu.sustech.cs307.DBEntry"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # 等待服务器启动
        for i in range(30):  # 最多等待30秒
            time.sleep(1)
            if check_database_server():
                print("✅ 数据库服务器启动成功")
                return process
            print(f"等待数据库启动... ({i+1}/30)")

        print("❌ 数据库服务器启动超时")
        process.terminate()
        return None

    except Exception as e:
        print(f"❌ 启动数据库服务器失败: {e}")
        return None


async def run_comprehensive_fuzz_test():
    """运行综合Fuzz测试"""
    print("\n🧪 运行综合Fuzz测试...")
    try:
        from database_fuzz_tester import main as fuzz_main

        await fuzz_main()
        print("✅ 综合Fuzz测试完成")
        return True
    except Exception as e:
        print(f"❌ 综合Fuzz测试失败: {e}")
        return False


async def run_security_test():
    """运行安全测试"""
    print("\n🔒 运行安全测试...")
    try:
        from security_tester import main as security_main

        await security_main()
        print("✅ 安全测试完成")
        return True
    except Exception as e:
        print(f"❌ 安全测试失败: {e}")
        return False


async def run_performance_benchmark():
    """运行性能基准测试"""
    print("\n⚡ 运行性能基准测试...")
    try:
        from performance_benchmark import main as benchmark_main

        await benchmark_main()
        print("✅ 性能基准测试完成")
        return True
    except Exception as e:
        print(f"❌ 性能基准测试失败: {e}")
        return False


async def run_edge_case_test():
    """运行边界情况测试"""
    print("\n🎯 运行边界情况测试...")
    try:
        from edge_case_tester import main as edge_main

        await edge_main()
        print("✅ 边界情况测试完成")
        return True
    except Exception as e:
        print(f"❌ 边界情况测试失败: {e}")
        return False


async def run_data_integrity_test():
    """运行数据完整性测试"""
    print("\n🔍 运行数据完整性测试...")
    try:
        from data_integrity_tester import main as integrity_main

        await integrity_main()
        print("✅ 数据完整性测试完成")
        return True
    except Exception as e:
        print(f"❌ 数据完整性测试失败: {e}")
        return False


def generate_summary_report(results):
    """生成总结报告"""
    report = f"""
=== 数据库测试总结报告 ===
生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}

测试结果:
"""

    for test_name, success in results.items():
        status = "✅ 通过" if success else "❌ 失败"
        report += f"  {test_name}: {status}\n"

    total_tests = len(results)
    passed_tests = sum(results.values())
    success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0

    report += f"""
总测试数: {total_tests}
通过测试: {passed_tests}
失败测试: {total_tests - passed_tests}
成功率: {success_rate:.1f}%

建议:
"""

    if success_rate == 100:
        report += "  🎉 所有测试都通过了！数据库表现良好。\n"
    elif success_rate >= 80:
        report += "  👍 大部分测试通过，建议查看失败的测试详情。\n"
    else:
        report += "  ⚠️ 多个测试失败，建议仔细检查数据库实现。\n"

    # 保存报告
    with open("test_summary_report.txt", "w", encoding="utf-8") as f:
        f.write(report)

    return report


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="数据库Fuzz测试运行器")
    parser.add_argument(
        "--test-type",
        choices=["all", "fuzz", "security", "performance", "edge", "integrity"],
        default="all",
        help="选择要运行的测试类型",
    )
    parser.add_argument(
        "--no-server-check",
        action="store_true",
        help="跳过数据库服务器检查（假设服务器已运行）",
    )
    parser.add_argument(
        "--start-server", action="store_true", help="自动启动数据库服务器"
    )

    args = parser.parse_args()

    print("数据库Fuzz测试运行器")
    print("===================")

    # 检查数据库服务器状态
    if not args.no_server_check:
        if not check_database_server():
            if args.start_server:
                server_process = start_database_server()
                if not server_process:
                    print("❌ 无法启动数据库服务器，测试终止")
                    return 1
            else:
                print("❌ 数据库服务器未运行")
                print("请先启动数据库服务器，或使用 --start-server 参数自动启动")
                return 1
        else:
            print("✅ 数据库服务器正在运行")

    # 运行测试
    results = {}

    if args.test_type in ["all", "fuzz"]:
        results["综合Fuzz测试"] = await run_comprehensive_fuzz_test()

    if args.test_type in ["all", "security"]:
        results["安全测试"] = await run_security_test()

    if args.test_type in ["all", "performance"]:
        results["性能基准测试"] = await run_performance_benchmark()

    if args.test_type in ["all", "edge"]:
        results["边界情况测试"] = await run_edge_case_test()

    if args.test_type in ["all", "integrity"]:
        results["数据完整性测试"] = await run_data_integrity_test()

    # 生成总结报告
    summary_report = generate_summary_report(results)
    print(summary_report)

    print("测试报告已保存到: test_summary_report.txt")

    # 返回错误码
    all_passed = all(results.values())
    return 0 if all_passed else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"运行器发生错误: {e}")
        sys.exit(1)
