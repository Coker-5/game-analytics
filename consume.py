from kafka import KafkaConsumer
import clickhouse_connect
import json
from datetime import datetime

ch = clickhouse_connect.get_client(host="localhost", port=8123, database="game")

# 配置kafka消费者
consumer = KafkaConsumer(
    'tp_game_events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # 建议保留，防止漏读
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("正在等待接收消息...")

mes_list = []


for message in consumer:
    msg = message.value
    
    # 表结构: event_time (DateTime), event_name (String), user_id (String), server (String), 
    #        device (String), level (String), pay_amount (Float32), duration (UInt32), 
    #        properties (Map(String, String))
    try:
        # 处理 properties，确保所有键值对都是 (String, String) 以符合 Map(String, String)
        raw_properties = msg.get('properties', {})
        clean_properties = {str(k): str(v) for k, v in raw_properties.items()}
        
        row = [
            datetime.strptime(msg['event_time'], '%Y-%m-%d %H:%M:%S'),
            msg['event_name'],
            msg['user_id'],
            msg['server'],
            msg['device'],
            str(msg.get('level', '')),
            float(msg.get('pay_amount', 0)),
            int(msg.get('duration', 0)),
            clean_properties
        ]
        mes_list.append(row)
        print(f"解析成功: {row[0]} | {row[1]} | {row[2]}")
    except Exception as e:
        print(f"数据解析失败: {e}, 原始数据: {msg}")
        continue

    # 写入clickhouse
    if len(mes_list) >= 5000:
        try:
            # 修改为正确的表名
            ch.insert("game_events", mes_list)
            print(f"成功插入 {len(mes_list)} 条数据到 ClickHouse")
            mes_list = [] # 成功后清空列表
        except Exception as e:
            print(f"写入 ClickHouse 失败: {e}")
            # 如果写入失败，列表保留，下次循环重试
