"""路由包"""

from .overview import bp as overview_bp
from .query import bp as query_bp
from .distribution import bp as distribution_bp
from .retention import bp as retention_bp
from .funnel import bp as funnel_bp

__all__ = ["overview_bp", "query_bp", "distribution_bp", "retention_bp", "funnel_bp"]
