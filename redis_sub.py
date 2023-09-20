import redis
import json
import pandas as pd

data = []
symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META", "TSLA", "NVDA", "PYPL", "ADBE", "NFLX"]
stock_status = {}

def process_message(msg):
    global data
    data.append(msg)
    pd.DataFrame(data).to_csv("data.csv")


redis_client = redis.Redis(host="127.0.0.1", port="6379", db=0, password="p@ss$12E45")
sub = redis_client.pubsub()
for symbol in symbols:
    sub.subscribe(symbol)
    sub.subscribe(symbol + "-status")
    stock_status[symbol] = None


for raw_message in sub.listen():
    if raw_message["type"] != "message":
        continue
    # print(raw_message)
    message = json.loads(raw_message["data"])
    if("status" in message):
        stock_status[message["Symbol"]] = message
        print(message)

        if(message["Action"]=="SELL"):
            if(message["RemainingQuantity"]==0):
                stock_status[message["Symbol"]] = None
        continue
    
    message["BidPrice"] = float(message["BidPrice"])
    message["AskPrice"] = float(message["AskPrice"])

    if(stock_status[message["Symbol"]] is None):
        # print(message["BidPrice"], type(message["BidPrice"]))
        msg = {
            "action": "BUY",
            "quantity": 100,
            "symbol": message["Symbol"],
            "price": round(message["BidPrice"] - (0.0001*message["BidPrice"]) , 2),
            "order_type": "LMT"
        }
        redis_client.publish("order-execution", json.dumps(msg))
        stock_status[message["Symbol"]] = -1
        # process_message(message)
        # print(message)
    
    else:
        if(stock_status[message["Symbol"]]==-1):
            continue
        if(stock_status[message["Symbol"]]["Action"]=="SELL"):
            continue
        else:
            if(stock_status[message["Symbol"]]["RemainingQuantity"]>0):
                print("Order is not filled yet")
                print("Remaining Quantity: ", stock_status[message["Symbol"]]["RemainingQuantity"])
                continue
            if(float(message["AskPrice"])<float(stock_status[message["Symbol"]]["AvgFillPrice"])):
                print("AskPrice is less than average fill price")
                print("Ask Price: ", message["AskPrice"])
                print("Avg Fill Price: ", stock_status[message["Symbol"]]["AvgFillPrice"])
                continue
        msg = {
            "action": "SELL",
            "quantity": float(stock_status[message["Symbol"]]["FilledQuantity"]),
            "symbol": message["Symbol"],
            "price": round(message["AskPrice"] + (0.0001*message["AskPrice"]) , 2),
            "order_type": "LMT"
        }
        print(msg)
        redis_client.publish("order-execution", json.dumps(msg))
        stock_status[message["Symbol"]] = -1
        # process_message(message)
        # print(message)
