"""
ClickHouse 数据访问层
"""

import clickhouse_connect
from typing import List, Dict, Any, Tuple, Optional

from game_analytics.config import Config


class ClickHouseRepository:
    """ClickHouse 数据仓库"""

    def __init__(self):
        self.host = Config.CLICKHOUSE_HOST
        self.port = Config.CLICKHOUSE_PORT
        self.database = Config.CLICKHOUSE_DATABASE

    def _get_client(self):
        """获取 ClickHouse 客户端（每次查询创建新实例避免并发问题）"""
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def query(self, sql: str) -> Any:
        """执行 SQL 查询"""
        client = self._get_client()
        return client.query(sql)

    def insert(self, table: str, data: List[List[Any]]):
        """插入数据"""
        client = self._get_client()
        client.insert(table, data)

    def get_today_dau(self) -> int:
        """获取今日 DAU"""
        result = self.query(
            "SELECT uniq(user_id) FROM game_events WHERE toDate(event_time) = today()"
        )
        return result.result_rows[0][0]

    def get_today_match_count(self) -> int:
        """获取今日对局数"""
        result = self.query(
            "SELECT count(*) FROM game_events "
            "WHERE toDate(event_time) = today() AND event_name = 'match_end'"
        )
        return result.result_rows[0][0]

    def get_today_skin_sales(self) -> Tuple[int, float]:
        """获取今日皮肤销售 (数量, 收入)"""
        result = self.query(
            "SELECT count(*), sum(toFloat32(properties['price'])) "
            "FROM game_events "
            "WHERE event_name = 'skin_buy' AND toDate(event_time) = today()"
        )
        row = result.result_rows[0]
        return row[0], row[1] or 0

    def get_total_players(self) -> int:
        """获取总玩家数"""
        result = self.query("SELECT uniq(user_id) FROM game_events")
        return result.result_rows[0][0]

    def get_total_match_count(self) -> int:
        """获取总对局数"""
        result = self.query(
            "SELECT count(*) FROM game_events WHERE event_name = 'match_end'"
        )
        return result.result_rows[0][0]

    def get_total_skin_sales(self) -> Tuple[int, float]:
        """获取总皮肤销售 (数量, 收入)"""
        result = self.query(
            "SELECT count(*), sum(toFloat32(properties['price'])) "
            "FROM game_events WHERE event_name = 'skin_buy'"
        )
        row = result.result_rows[0]
        return row[0], row[1] or 0

    def get_level_distribution(self) -> List[Tuple]:
        """获取段位分布"""
        result = self.query(
            "SELECT level, uniq(user_id) FROM game_events GROUP BY level"
        )
        return result.result_rows

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行自定义 SQL"""
        result = self.query(sql)
        return {"columns": result.column_names, "rows": result.result_rows}

    def get_retention_data(self, date: str) -> Dict[str, Any]:
        """获取指定日期的留存数据

        Args:
            date: 日期字符串，格式 '2026-03-30'

        Returns:
            包含次日留存、三日留存、七日留存数据的字典
        """
        sql = f"""
        SELECT
            (SELECT count(DISTINCT user_id) FROM users WHERE first_login_date = '{date}') as total_new_users,
            (SELECT count(DISTINCT user_id) FROM users u
             ANY INNER JOIN game_events e ON u.user_id = e.user_id
             WHERE u.first_login_date = '{date}'
               AND e.event_name = 'login'
               AND toDate(e.event_time) = toDate('{date}') + 1) as day1_retained,
            (SELECT count(DISTINCT user_id) FROM users u
             ANY INNER JOIN game_events e ON u.user_id = e.user_id
             WHERE u.first_login_date = '{date}'
               AND e.event_name = 'login'
               AND toDate(e.event_time) = toDate('{date}') + 3) as day3_retained,
            (SELECT count(DISTINCT user_id) FROM users u
             ANY INNER JOIN game_events e ON u.user_id = e.user_id
             WHERE u.first_login_date = '{date}'
               AND e.event_name = 'login'
               AND toDate(e.event_time) = toDate('{date}') + 7) as day7_retained
        """
        result = self.query(sql)
        row = result.result_rows[0] if result.result_rows else [0, 0, 0, 0]
        total = row[0] if row[0] > 0 else 1
        return {
            "date": date,
            "total_new_users": row[0],
            "day1_retained": row[1],
            "day3_retained": row[2],
            "day7_retained": row[3],
            "day1_rate": round(row[1] * 100.0 / total, 2),
            "day3_rate": round(row[2] * 100.0 / total, 2),
            "day7_rate": round(row[3] * 100.0 / total, 2),
        }

    def get_default_funnel_data(self) -> Dict[str, Any]:
        """获取今日固定路径漏斗数据"""
        sql = """
        SELECT
            level,
            count() AS user_count
        FROM (
            SELECT
                user_id,
                windowFunnel(7200)(
                    event_time,
                    event_name = 'match_start',
                    event_name = 'match_end',
                    event_name = 'skin_buy'
                ) AS level
            FROM game_events
            WHERE event_name IN ('match_start', 'match_end', 'skin_buy')
              AND toDate(event_time) = today()
            GROUP BY user_id
        )
        GROUP BY level
        ORDER BY level
        """
        result = self.query(sql)

        level_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for level, user_count in result.result_rows:
            level_counts[int(level)] = user_count

        match_start_count = (
            level_counts[1] + level_counts[2] + level_counts[3]
        )
        match_end_count = level_counts[2] + level_counts[3]
        skin_buy_count = level_counts[3]

        return {
            "match_start_count": match_start_count,
            "match_end_count": match_end_count,
            "skin_buy_count": skin_buy_count,
            "match_start_rate": 100.0 if match_start_count > 0 else 0,
            "match_end_rate": round(
                match_end_count * 100.0 / match_start_count, 2
            )
            if match_start_count > 0
            else 0,
            "skin_buy_rate": round(skin_buy_count * 100.0 / match_end_count, 2)
            if match_end_count > 0
            else 0,
            "total_users": match_start_count,
            "level_distribution": level_counts,
        }

    def get_daily_retention_trend(self, days: int = 7) -> List[Dict[str, Any]]:
        """获取最近 N 天的留存趋势

        Args:
            days: 查询天数，默认 7 天

        Returns:
            每日留存数据列表
        """
        sql = f"""
        SELECT
            d.date,
            uniq(u.user_id) as total_new_users,
            uniqIf(e.user_id, toDate(e.event_time) = d.date + 1) as day1_retained
        FROM (
            SELECT toDate(today()) - number as date
            FROM numbers({days})
        ) d
        LEFT JOIN users u ON u.first_login_date = d.date
        LEFT JOIN game_events e ON u.user_id = e.user_id AND e.event_name = 'login'
        GROUP BY d.date
        ORDER BY d.date
        """
        result = self.query(sql)
        data = []
        for row in result.result_rows:
            total = row[1] if row[1] > 0 else 1
            rate = round(row[2] * 100.0 / total, 2) if row[2] > 0 else 0
            data.append(
                {
                    "date": str(row[0]),
                    "total_new_users": row[1],
                    "day1_retained": row[2],
                    "day1_rate": rate,
                }
            )
        return data


# 全局仓库实例
repo = ClickHouseRepository()
