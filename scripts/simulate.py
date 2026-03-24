"""
事件模拟器 - 持续生成游戏事件数据并发送到 Kafka
每小时生成 500 条数据
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import random
import time
import signal
from datetime import datetime, timedelta
from kafka import KafkaProducer

from game_analytics.models import Event
from game_analytics.services import EventSimulator
from game_analytics.config import Config


# ========== 模拟时间设置 ==========
SIMULATE_SPEED_UP = Config.SIMULATE_SPEED_UP
START_TIME = datetime.now()
current_sim_time = START_TIME

# 运行控制
running = True


def signal_handler(signum, frame):
    """处理退出信号"""
    global running
    print("\n接收到退出信号，正在停止...")
    running = False


# 注册信号处理
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_sim_time():
    """获取模拟时间"""
    global current_sim_time
    current_sim_time += timedelta(seconds=random.randint(10, 30))
    return current_sim_time.strftime("%Y-%m-%d %H:%M:%S")


def make_properties(event_name, **kwargs):
    """生成事件属性"""
    if event_name == "match_end":
        return {
            "hero": random.choice(EventSimulator.HEROES),
            "win": random.choice([True, False]),
            "kills": random.randint(0, 15),
            "deaths": random.randint(0, 10),
            "assists": random.randint(0, 20),
            "mvp": random.choice([True, False]),
        }
    elif event_name == "match_start":
        return {
            "hero": random.choice(EventSimulator.HEROES),
            "summoner_skill": random.choice(EventSimulator.DEFAULT_SKILL),
            "rune_level": random.randint(0, 150),
        }
    elif event_name == "skin_buy":
        return {
            "skin_name": random.choice(EventSimulator.SKINS),
            "price": random.choice(EventSimulator.SKIN_PRICES),
        }
    elif event_name == "battle_pass_buy":
        return {"season": random.choice(EventSimulator.SEASONS)}
    else:
        return {}


def make_event(player, event_name, **kwargs):
    """创建事件"""
    event_time_str = get_sim_time()
    return Event(
        event_time=event_time_str,
        event_name=event_name,
        user_id=player["user_id"],
        server=player["server"],
        device=player["device"],
        level=player["level"],
        pay_amount=kwargs.get("pay_amount", 0),
        duration=random.randint(0, 30),
        properties=make_properties(event_name, **kwargs),
    )


# ========== 状态机定义 ==========
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


def init_players(count=500):
    """初始化玩家列表"""
    return [
        {
            "user_id": f"u_{i:03d}",
            "server": random.choice(EventSimulator.SERVERS),
            "device": random.choice(EventSimulator.DEVICES),
            "level": random.choice(EventSimulator.RANKS),
            "status": "offline",
        }
        for i in range(count)
    ]


def generate_hourly_events(producer, players, events_per_hour=500):
    """生成一小时的事件数据 - 按真实时间间隔"""
    global current_sim_time

    hour_start = current_sim_time
    hour_end = hour_start + timedelta(hours=1)
    events_generated = 0

    print(f"时间范围: {hour_start.strftime('%H:%M')} - {hour_end.strftime('%H:%M')}")
    print("生成中...\n")

    while events_generated < events_per_hour and running:
        # 随机等待 5-20 秒（模拟真实玩家行为间隔）
        sleep_seconds = random.randint(5, 20)
        time.sleep(sleep_seconds)

        # 模拟时间也前进
        current_sim_time += timedelta(seconds=sleep_seconds)

        # 如果时间超出一小时，退出循环
        if current_sim_time >= hour_end:
            break

        # 随机选择一个在线或离线的玩家
        player = random.choice(players)
        event_name, next_status = get_next_event(player["status"])
        event = make_event(player, event_name)

        # 异步发送事件（不阻塞）
        producer.send(Config.KAFKA_TOPIC_EVENTS, value=event.to_json())

        events_generated += 1

        # 每 50 条打印一次进度
        if events_generated % 50 == 0:
            print(f"  已生成 {events_generated}/{events_per_hour} 条事件")

        # 更新玩家状态
        player["status"] = next_status

    return events_generated


def main():
    """主函数 - 持续运行"""
    global current_sim_time

    print("=== 游戏事件模拟器启动 ===")
    print(f"目标: 每小时生成 500 条事件（间隔 5-20 秒）")
    print("按 Ctrl+C 停止运行\n")

    # 初始化 Kafka 生产者
    producer = KafkaProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    # 初始化玩家列表
    players = init_players(500)

    total_events = 0
    current_sim_time = datetime.now()

    while running:
        hour_start = datetime.now()
        hour_events = 0

        print(f"\n{'=' * 50}")
        print(f"开始生成 {current_sim_time.strftime('%Y-%m-%d %H:%M')} 的事件数据")
        print(f"{'=' * 50}")

        try:
            # 生成 500 条事件（按真实时间间隔）
            hour_events = generate_hourly_events(producer, players, 500)
            total_events += hour_events

            elapsed = (datetime.now() - hour_start).total_seconds()
            print(f"\n✓ 本小时完成: {hour_events} 条事件")
            print(f"  实际耗时: {elapsed:.1f} 秒")
            print(f"  累计生成: {total_events} 条事件")

        except Exception as e:
            print(f"生成事件时出错: {e}")
            time.sleep(5)
            continue

        # 进入下一小时
        current_sim_time += timedelta(hours=1)

    print(f"\n模拟器已停止。累计生成 {total_events} 条事件")


if __name__ == "__main__":
    main()
