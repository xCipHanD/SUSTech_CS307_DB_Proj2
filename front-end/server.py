from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)


# 设置静态文件目录
@app.route("/")
def index():
    """主页路由，返回数据库管理界面"""
    return send_from_directory(".", "index.html")


@app.route("/favicon.ico")
def favicon():
    """处理favicon请求"""
    return "", 204


@app.errorhandler(404)
def not_found(error):
    """404错误处理"""
    return send_from_directory(".", "index.html")


if __name__ == "__main__":
    print("🚀 启动 CS307 数据库前端服务器...")
    print("📍 访问地址: http://localhost:3000")
    print("🗃️ 数据库API地址: http://localhost:8080")
    print("📄 确保你的数据库服务器(DBEntry)正在端口8080上运行")
    print("=" * 50)

    app.run(host="0.0.0.0", port=3000, debug=True, use_reloader=True)
