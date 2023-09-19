import redis
import json
import pandas as pd


data = []

def process_message(msg):
    global data
    data.append(msg)
    pd.DataFrame(data).to_csv("data.csv")


redis_client = redis.Redis(host="127.0.0.1", port="6379", db=0, password="p@ss$12E45")
sub = redis_client.pubsub()
sub.subscribe('ib-tick')


for raw_message in sub.listen():
    if raw_message["type"] != "message":
        continue
    # print(raw_message)
    message = json.loads(raw_message["data"])
    process_message(message)
    print(message)