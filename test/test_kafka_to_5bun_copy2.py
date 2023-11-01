from confluent_kafka import Consumer, KafkaError, Producer
import json
from datetime import datetime, timedelta
import pytz 
import pandas as pd
import os
import logging
import csv

# 로그 생성
logger = logging.getLogger()
# 로그의 출력 기준 설정
logger.setLevel(logging.INFO)
# log 출력 형식
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# log를 console에 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


##data 폴더 생성
try:
    if not os.path.exists("data"):
        os.mkdir("data")
except OSError:
    logger.info('Error: Creating directory. ' +  "data")


# Kafka 설정
kafka_conf = {
    #'bootstrap.servers': 'shtestdb.duckdns.org:9094',  # Kafka 브로커 서버 주소로 변경
    'bootstrap.servers': '43.201.197.216:9094',  # Kafka 브로커 서버 주소로 변경
    'group.id': 'convert_min1',
    'auto.offset.reset': 'earliest'
}

# Kafka Consumer 생성
consumer = Consumer(kafka_conf)

# 구독할 토픽
consumer.subscribe(['stock_tick'])


# Kafka Producer 생성
producer = Producer({'bootstrap.servers': '43.201.197.216:9094'})
output_topic = 'stock_5min_ticktt2'

# 종목별 봉 데이터를 저장할 딕셔너리
candlestick_data = {}
candlestick_first = {}


# 5분봉 주기
candlestick_interval = timedelta(minutes=5)

first_flag = True

def change_unix(timestamp):
    korea_tz = pytz.timezone('Asia/Seoul')
    utc_time = datetime.utcfromtimestamp(timestamp)
    korea_time = utc_time.replace(tzinfo=pytz.utc).astimezone(korea_tz)
    return korea_time.strftime("%Y-%m-%d %H:%M:%S")


def check_time_range(time_str):
    try:
        # 입력된 시간을 파싱하여 시간과 분을 추출
        time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

        # 분 단위로 변환
        total_minutes =  time_obj.minute
        tmp_min = 0
        
        # 분 단위로 범위를 확인하고 출력
        if 0 <= total_minutes < 5:
            tmp_min = 5
        elif 5 <= total_minutes < 10:
            tmp_min = 10
        elif 10 <= total_minutes < 15:
            tmp_min = 15
        elif 15 <= total_minutes < 20:
            tmp_min = 20
        elif 20 <= total_minutes < 25:
            tmp_min = 25
        elif 25 <= total_minutes < 30:
            tmp_min = 30
        elif 30 <= total_minutes < 35:
            tmp_min = 35
        elif 35 <= total_minutes < 40:
            tmp_min = 40
        elif 40 <= total_minutes < 45:
            tmp_min = 45
        elif 45 <= total_minutes < 50:
            tmp_min = 50
        elif 50 <= total_minutes < 55:
            tmp_min = 55
        elif 55 <= total_minutes:
            time_obj = time_obj + timedelta(hours=1)
            tmp_min = 0
        else:
            return "기타"
    except Exception as e:
       return "잘못된 형식의 입력입니다."
    
    return time_obj.replace(minute=tmp_min, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

def write_to_csv(message, filename):
    # 'a' 모드로 파일 열기 (없으면 새로 만들고, 있으면 끝에 추가)
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        # 메시지의 key들을 컬럼명으로 사용하려면 한 번만 쓰기
        if file.tell() == 0:
            writer.writerow(message.keys())
        # 메시지의 value들을 csv 파일에 쓰기
        writer.writerow(message.values())


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error while receiving message: {}'.format(msg.error()))
    else:
        # 메시지 값을 JSON으로 디코딩
        data = json.loads(msg.value())
        
        # 데이터에서 종목 식별자 추출
        stock_symbol = data['shcode']
        
        # 종목별로 봉 데이터를 저장할 리스트 초기화
        if stock_symbol not in candlestick_data:
            candlestick_data[stock_symbol] = []
        if stock_symbol not in candlestick_first:
            candlestick_first[stock_symbol] = True

        # 데이터의 시간 정보를 datetime 객체로 변환
        #timestamp = check_time_range(change_unix(data['system_time']))
        timestamp = change_unix(data['system_time']) 
        timestamp = check_time_range(str(timestamp))
        #print(timestamp)

        # 해당 종목의 봉 데이터 리스트에서 마지막 봉 가져오기
        last_candlestick = candlestick_data[stock_symbol][-1] if candlestick_data[stock_symbol] else None
        

        #print(last_candlestick)
        # 새로운 봉을 시작해야 하는지 확인
        if last_candlestick is None or datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") > datetime.strptime(last_candlestick['timestamp'], "%Y-%m-%d %H:%M:%S"):

            
            
            if candlestick_first[stock_symbol] == False:
                message = {
                            'symbol': stock_symbol,
                            'timestamp': last_candlestick['timestamp'],
                            'open': last_candlestick['open'],
                            'high': last_candlestick['high'],
                            'low': last_candlestick['low'],
                            'close': last_candlestick['close']
                        }
                producer.produce(output_topic, key=stock_symbol, value=json.dumps(message))
                producer.flush()
                #print("send kafka")
                #write_to_csv(message, f'./data/{stock_symbol}.csv')
                
                
            # 새로운 봉 데이터 생성
            new_candlestick = {
                'timestamp': timestamp,
                'open': data['price'],
                'high': data['price'],
                'low': data['price'],
                'close': data['price']
            }
            #print(new_candlestick)
            candlestick_data[stock_symbol].append(new_candlestick)
            #print(candlestick_data[stock_symbol])
            candlestick_first[stock_symbol] = False
        else:
            # 기존 봉 데이터 업데이트
            last_candlestick['high'] = max(last_candlestick['high'], data['price'])
            last_candlestick['low'] = min(last_candlestick['low'], data['price'])
            last_candlestick['close'] = data['price']




# Kafka Consumer 종료
consumer.close()
producer.close()