"""
Flask 应用创建
"""

from flask import Flask, jsonify
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


def create_app() -> Flask:
    """创建 Flask 应用实例"""
    app = Flask(__name__, template_folder=str(PROJECT_ROOT / "templates"))

    # 注册蓝图
    from game_analytics.routes.overview import bp as overview_bp
    from game_analytics.routes.query import bp as query_bp
    from game_analytics.routes.distribution import bp as distribution_bp
    from game_analytics.routes.retention import bp as retention_bp
    from game_analytics.routes.funnel import bp as funnel_bp

    app.register_blueprint(overview_bp)
    app.register_blueprint(query_bp)
    app.register_blueprint(distribution_bp)
    app.register_blueprint(retention_bp)
    app.register_blueprint(funnel_bp)

    return app


def make_response(data=None, code=200, msg="success"):
    """统一 API 返回格式"""
    return jsonify({"code": code, "data": data, "msg": msg}), code
