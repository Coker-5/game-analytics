"""
历史数据补充脚本 - 生成近两周约15万条事件数据直接写入 ClickHouse
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import random
from datetime import datetime, timedelta

from game_analytics.services import EventSimulator
from game_analytics.repositories import repo


SERVERS = EventSimulator.SERVERS
DEVICES = EventSimulator.DEVICES
RANKS = EventSimulator.RANKS
HEROES = EventSimulator.HEROES
DEFAULT_SKILL = EventSimulator.DEFAULT_SKILL
SKINS = EventSimulator.SKINS
SKIN_PRICES = EventSimulator.SKIN_PRICES
SEASONS = EventSimulator.SEASONS


VALID_TRANSITIONS = {
    "offline": ["login"],
    "online": ["match_start", "skin_buy", "battle_pass_buy", "logout"],
    "in_match": ["match_end"],
}


def get_next_event(current_status):
    """根据当前状态获取下一个事件"""
    possible_events = VALID_TRANSITIONS.get(current_status, ["login"])
    event_name = random.choice(possible_events)

    next_status = current_status
    if event_name == "login":
        next_status = "online"
    elif event_name == "logout":
        next_status = "offline"
    elif event_name == "match_start":
        next_status = "in_match"
    elif event_name == "match_end":
        next_status = "online"

    return event_name, next_status


def make_properties(event_name):
    """生成事件属性"""
    if event_name == "match_end":
        return {
            "hero": random.choice(HEROES),
            "win": str(random.choice([True, False])),
            "kills": str(random.randint(0, 15)),
            "deaths": str(random.randint(0, 10)),
            "assists": str(random.randint(0, 20)),
            "mvp": str(random.choice([True, False])),
        }
    elif event_name == "match_start":
        return {
            "hero": random.choice(HEROES),
            "summoner_skill": random.choice(DEFAULT_SKILL),
            "rune_level": str(random.randint(0, 150)),
        }
    elif event_name == "skin_buy":
        return {
            "skin_name": random.choice(SKINS),
            "price": str(random.choice(SKIN_PRICES)),
        }
    elif event_name == "battle_pass_buy":
        return {"season": random.choice(SEASONS)}
    else:
        return {}


def init_players(count=500):
    """初始化玩家列表"""
    return [
        {
            "user_id": f"u_{i:03d}",
            "server": random.choice(SERVERS),
            "device": random.choice(DEVICES),
            "level": random.choice(RANKS),
            "status": "offline",
        }
        for i in range(count)
    ]


def generate_event(player, event_time: datetime):
    """生成单个事件"""
    event_name, next_status = get_next_event(player["status"])
    player["status"] = next_status

    pay_amount = 0
    if event_name == "skin_buy":
        pay_amount = random.choice(SKIN_PRICES)
    elif event_name == "battle_pass_buy":
        pay_amount = 88

    return [
        event_time,
        event_name,
        player["user_id"],
        player["server"],
        player["device"],
        str(player["level"]),
        float(pay_amount),
        random.randint(0, 30),
        make_properties(event_name),
    ]


def generate_batch_events(players, start_time: datetime, count):
    """批量生成事件数据"""
    events = []
    current_time = start_time

    for _ in range(count):
        player = random.choice(players)
        event = generate_event(player, current_time)
        events.append(event)
        current_time += timedelta(seconds=random.randint(5, 30))

    return events, current_time


def main():
    """主函数"""
    print("=== 开始生成历史数据 ===")
    print("目标: 近两周约5万条事件数据")

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = today - timedelta(days=14)
    end_date = today

    print(
        f"时间范围: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
    )
    print(f"预计事件数: 50000 条\n")

    players = init_players(500)
    total_events = 0
    batch_size = 2000
    target_events = 50000

    total_seconds = (end_date - start_date).total_seconds()
    seconds_per_event = total_seconds / target_events
    current_time = start_date

    while total_events < target_events:
        remaining = target_events - total_events
        this_batch = min(batch_size, remaining)

        events, _ = generate_batch_events(players, current_time, this_batch)
        current_time += timedelta(seconds=seconds_per_event * this_batch)

        try:
            repo.insert("game_events", events)
            total_events += len(events)
            print(
                f"✓ 已写入 {len(events)} 条 | 累计 {total_events}/{target_events} | 时间: {current_time.strftime('%Y-%m-%d %H:%M')}"
            )
        except Exception as e:
            print(f"✗ 写入失败: {e}")
            continue

    print(f"\n=== 完成 ===")
    print(f"总计写入 {total_events} 条历史数据")


if __name__ == "__main__":
    main()
