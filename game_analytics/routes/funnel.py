"""漏斗分析相关路由"""

from flask import Blueprint
from datetime import date

from game_analytics.app import make_response
from game_analytics.repositories import repo

bp = Blueprint("funnel", __name__)


@bp.route("/api/funnel/default", methods=["GET"])
def get_default_funnel():
    """获取默认的用户转化漏斗"""
    default_events = ["match_start", "match_end", "skin_buy"]
    date_str = date.today().strftime("%Y-%m-%d")
    funnel_data = repo.get_default_funnel_data()

    return make_response(
        data={"event_sequence": default_events, "date": date_str, "funnel": funnel_data}
    )
