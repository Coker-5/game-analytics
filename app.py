from flask import Flask, jsonify, render_template, request
import clickhouse_connect

app = Flask(__name__)
ch = clickhouse_connect.get_client(host="localhost", port=8123, database="game")


def make_response(data=None, code=200, msg="success"):
    """
    统一 API 返回格式
    """
    return jsonify({
        "code": code,
        "data": data,
        "msg": msg
    }), code


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/overview")
def overview():
    # 查询：今日活跃玩家数、今日对局场次、今日皮肤销售额
    today_dau = ch.query(f"select uniq(user_id) from game_events where toDate(event_time) = today()")
    today_match = ch.query(f"select count(*) from game_events where toDate(event_time) = today() and event_name = 'match_end'")
    today_skin_sell = ch.query(f"SELECT count(*),sum(toFloat32(properties['price'])) AS total_revenue FROM game_events WHERE event_name = 'skin_buy' AND toDate(event_time) = today()")

    # 查询：总玩家数、总对局场次、总皮肤销售额
    total_players = ch.query(f"select uniq(user_id) from game_events")
    total_match = ch.query(f"select count(*) from game_events where event_name = 'match_end'")
    total_sell = ch.query(f"SELECT count(*),sum(toFloat32(properties['price'])) AS total_revenue FROM game_events WHERE event_name = 'skin_buy'")

    # 格式化结果
    result = {
        "today": {
            "dau": today_dau.result_rows[0][0],
            "match": today_match.result_rows[0][0],
            "skin_sell_count": today_skin_sell.result_rows[0][0],
            "revenue": today_skin_sell.result_rows[0][1] or 0
        },
        "total": {
            "players": total_players.result_rows[0][0],
            "match": total_match.result_rows[0][0],
            "skin_sell_count": total_sell.result_rows[0][0],
            "revenue": total_sell.result_rows[0][1] or 0
        }
    }

    return make_response(data=result)


@app.route("/api/query-sql", methods=["POST"])
def query_sql():
    # 执行自定义 SQL
    sql = request.json.get("sql")
    if not sql:
        return make_response(code=400, msg="SQL is required")
    
    try:
        query_result = ch.query(sql)
        result = {
            "columns": query_result.column_names,
            "rows": query_result.result_rows
        }
        return make_response(data=result)
    except Exception as e:
        return make_response(code=500, msg=str(e))


@app.route("/api/level-distribution")
def level_distribution():
    # 查询：各段位玩家分布

    level_distribution = ch.query(f"select level,uniq(user_id) from game_events group by level;")
    result = {
        "level_distribution": level_distribution.result_rows
    }
    return make_response(data=result)
    



if __name__ == "__main__":
    app.run(port=5000, debug=True)
