"""
ClickHouse 数据访问层
"""

import clickhouse_connect
from typing import List, Dict, Any, Tuple, Optional

from game_analytics.config import Config


class ClickHouseRepository:
    """ClickHouse 数据仓库"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_client()
        return cls._instance

    def _init_client(self):
        """初始化 ClickHouse 客户端"""
        self.client = clickhouse_connect.get_client(
            host=Config.CLICKHOUSE_HOST,
            port=Config.CLICKHOUSE_PORT,
            database=Config.CLICKHOUSE_DATABASE,
        )

    def query(self, sql: str) -> clickhouse_connect.driver.ClientResult:
        """执行 SQL 查询"""
        return self.client.query(sql)

    def insert(self, table: str, data: List[List[Any]]):
        """插入数据"""
        self.client.insert(table, data)

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
        WITH 
            new_users AS (
                SELECT DISTINCT user_id 
                FROM users FINAL 
                WHERE first_login_date = '{date}'
            ),
            day1_active AS (
                SELECT DISTINCT user_id 
                FROM game_events 
                WHERE event_name = 'login' 
                  AND toDate(event_time) = toDate('{date}') + 1
            ),
            day3_active AS (
                SELECT DISTINCT user_id 
                FROM game_events 
                WHERE event_name = 'login' 
                  AND toDate(event_time) = toDate('{date}') + 3
            ),
            day7_active AS (
                SELECT DISTINCT user_id 
                FROM game_events 
                WHERE event_name = 'login' 
                  AND toDate(event_time) = toDate('{date}') + 7
            )
        SELECT 
            COUNT(n.user_id) as total_new_users,
            COUNTIf(d1.user_id != '') as day1_retained,
            COUNTIf(d3.user_id != '') as day3_retained,
            COUNTIf(d7.user_id != '') as day7_retained,
            ROUND(COUNTIf(d1.user_id != '') * 100.0 / COUNT(n.user_id), 2) as day1_rate,
            ROUND(COUNTIf(d3.user_id != '') * 100.0 / COUNT(n.user_id), 2) as day3_rate,
            ROUND(COUNTIf(d7.user_id != '') * 100.0 / COUNT(n.user_id), 2) as day7_rate
        FROM new_users n
        LEFT JOIN day1_active d1 ON n.user_id = d1.user_id
        LEFT JOIN day3_active d3 ON n.user_id = d3.user_id
        LEFT JOIN day7_active d7 ON n.user_id = d7.user_id
        """
        result = self.query(sql)
        row = result.result_rows[0] if result.result_rows else [0, 0, 0, 0, 0, 0, 0]
        return {
            "date": date,
            "total_new_users": row[0],
            "day1_retained": row[1],
            "day3_retained": row[2],
            "day7_retained": row[3],
            "day1_rate": row[4],
            "day3_rate": row[5],
            "day7_rate": row[6],
        }

    def get_daily_retention_trend(self, days: int = 7) -> List[Dict[str, Any]]:
        """获取最近 N 天的留存趋势

        Args:
            days: 查询天数，默认 7 天

        Returns:
            每日留存数据列表
        """
        sql = f"""
        WITH date_range AS (
            SELECT toDate(today()) - number as calc_date
            FROM numbers({days})
        ),
        new_users AS (
            SELECT first_login_date as date, count(*) as new_count
            FROM users FINAL
            WHERE first_login_date >= toDate(today()) - {days}
            GROUP BY first_login_date
        ),
        daily_retention AS (
            SELECT 
                toDate(event_time) - 1 as date,
                uniq(user_id) as retained_count
            FROM game_events
            WHERE event_name = 'login'
              AND toDate(event_time) >= toDate(today()) - {days} + 1
            GROUP BY toDate(event_time) - 1
        )
        SELECT 
            d.calc_date as date,
            COALESCE(n.new_count, 0) as total_new_users,
            COALESCE(r.retained_count, 0) as day1_retained,
            ROUND(COALESCE(r.retained_count, 0) * 100.0 / NULLIF(n.new_count, 0), 2) as day1_rate
        FROM date_range d
        LEFT JOIN new_users n ON d.calc_date = n.date
        LEFT JOIN daily_retention r ON d.calc_date = r.date
        ORDER BY d.calc_date
        """
        result = self.query(sql)
        data = []
        for row in result.result_rows:
            data.append(
                {
                    "date": str(row[0]),
                    "total_new_users": row[1],
                    "day1_retained": row[2],
                    "day1_rate": row[3] if row[3] is not None else 0,
                }
            )
        return data


# 全局仓库实例
repo = ClickHouseRepository()
