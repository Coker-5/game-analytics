from ast import main
import random
from dataclasses import dataclass, asdict
import json
from kafka import KafkaProducer
import time


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8")  # 把字符串转成字节
)

# ========== 基础数据 ==========
SERVERS = ["QQ第1区", "QQ第2区", "QQ第3区", "QQ第4区", "微信第1区", "微信第2区", "微信第3区", "微信第4区"]
DEVICES = ["iOS", "Android"]
SEASONS = ["S33", "S34", "S35", "S36", "S37"]
RANKS = ["黄金", "铂金", "钻石", "星耀", "王者"]
HEROES = ["李白", "妲己", "后羿", "貂蝉", "韩信", "孙悟空", "铠", "张飞"]
DEFAULT_SKILL = ["惩击", "终结", "狂暴", "疾跑", "治疗术", "干扰", "眩晕", "净化", "弱化", "闪现"]
SKINS = ["李白·千年之狐", "后羿·弈", "韩信·绝世天君", "孙悟空·地狱火", "貂蝉·仲夏夜之梦", "铠·青龙志", "妲己·热情桑巴", "张飞·虎魄", "鲁班七号·电玩小子", "小乔·天鹅之梦", "孙尚香·末日机甲", "吕布·天魔缭乱", "赵云·引擎之心"]
SKIN_PRICES = [38, 88, 188, 288, 388]

# ========== 事件模型 ==========
@dataclass
class Event:
    event_time: str
    event_name: str
    user_id: str
    server: str
    device: str
    level: str
    pay_amount: float
    duration: int
    properties: dict  

    def to_json(self):
        return json.dumps(asdict(self), ensure_ascii=False)

# ========== 每种事件对应的 properties 模板 ==========
def make_properties(event_name, **kwargs):
    if event_name == "match_end":
        return {
            "hero": random.choice(HEROES),
            "win": random.choice([True, False]),
            "kills": random.randint(0, 15),
            "deaths": random.randint(0, 10),
            "assists": random.randint(0, 20),
            "mvp": random.choice([True, False])
        }
    elif event_name == "match_start":
        return {
            "hero": random.choice(HEROES),              # 随机选择本场使用的英雄
            "summoner_skill": random.choice(DEFAULT_SKILL),  # 随机选择召唤师技能
            "rune_level": random.randint(0, 150),      # 随机生成铭文等级（0-150）            
        }
    elif event_name == "skin_buy":
        return {
            "skin_name": random.choice(SKINS),
            "price": random.choice(SKIN_PRICES)
        }
    elif event_name == "battle_pass_buy":
        return {
            "season": random.choice(SEASONS),
        }
    else:
        return {}  # login/logout/match_start 没有额外属性

# ========== 玩家列表 ==========
players = [
    {
        "user_id": f"u_{i:03d}",
        "server": random.choice(SERVERS),
        "device": random.choice(DEVICES),
        "level": random.choice(RANKS),
        "status": "offline"
    }
    for i in range(500)
]


# ========== 状态机定义 ==========
# 状态说明:
# offline: 离线
# online: 在线（大厅）
# in_match: 比赛中

VALID_TRANSITIONS = {
    "offline": ["login"],
    "online": ["match_start", "skin_buy", "battle_pass_buy", "logout"],
    "in_match": ["match_end"]
}

# 状态更新逻辑
def get_next_event(current_status):
    possible_events = VALID_TRANSITIONS.get(current_status, ["login"])
    event_name = random.choice(possible_events)
    
    # 确定下一个状态
    next_status = current_status
    if event_name == "login":
        next_status = "online"
    elif event_name == "logout":
        next_status = "offline"
    elif event_name == "match_start":
        next_status = "in_match"
    elif event_name == "match_end":
        next_status = "online"
    # skin_buy 和 battle_pass_buy 不改变 online 状态
    
    return event_name, next_status

from datetime import datetime, timedelta

# ========== 模拟时间设置 ==========
# 模拟从 3 天前开始，以加速倍率运行
SIMULATE_SPEED_UP = 60  # 1秒真实时间 = 60秒模拟时间
START_TIME = datetime.now() - timedelta(days=2)
current_sim_time = START_TIME

def get_sim_time():
    global current_sim_time
    # 每次调用模拟时间前进一点（随机 10~30 秒），模拟真实行为间隔
    current_sim_time += timedelta(seconds=random.randint(10, 30))
    return current_sim_time.strftime("%Y-%m-%d %H:%M:%S")

def make_event(player, event_name, **kwargs):
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
        properties=make_properties(event_name, **kwargs)
    )

if __name__ == "__main__":
    for i in range(10000):
        player = random.choice(players)
        
        # 根据玩家当前状态获取下一个合法事件
        event_name, next_status = get_next_event(player["status"])
        
        # 生成事件
        event = make_event(player, event_name)
        
        # 发送事件
        future = producer.send("tp_game_events", value=event.to_json())
        result = future.get(timeout=10)
        print(f"用户 {player['user_id']} 事件 {event_name}: {player['status']} -> {next_status}")
        
        # 更新玩家状态
        player["status"] = next_status

        # 模拟现实中的处理延迟，如果模拟速度很快，这里可以调小
        time.sleep(0.01)
