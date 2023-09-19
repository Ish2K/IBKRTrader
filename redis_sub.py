import redis
import json
import pandas as pd

data = []
symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META", "TSLA", "NVDA", "PYPL", "ADBE", "NFLX"]

def process_message(msg):
    global data
    data.append(msg)
    pd.DataFrame(data).to_csv("data.csv")


redis_client = redis.Redis(host="127.0.0.1", port="6379", db=0, password="p@ss$12E45")
sub = redis_client.pubsub()
for symbol in symbols:
    sub.subscribe(symbol)


for raw_message in sub.listen():
    if raw_message["type"] != "message":
        continue
    # print(raw_message)
    message = json.loads(raw_message["data"])

    msg = {
        "action": "BUY",
        "quantity": 100,
        "symbol": message["Symbol"],
        "price": message["BidPrice"],
        "order_type": "LMT"
    }
    redis_client.publish("order-execution", json.dumps(msg))
    # process_message(message)
    print(message)