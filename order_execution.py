from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import *
from ibapi.contract import *
# from ContractSamples import ContractSamples
from decimal import Decimal
import datetime
import redis
import json

redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, password="trading")
sub = redis_client.pubsub()
sub.subscribe( 'order-execution')


class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.conId = None
        self.contract = None
        self.nextValidId = None
        
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
        self.simplePlaceOid = self.nextOrderId()

    def buy_order(self, contract, order):
        self.placeOrder(self.simplePlaceOid, contract, order)
        print("Order placed")
        self.simplePlaceOid += 1

    def sell_order(self, contract, order):
        self.placeOrder(self.simplePlaceOid, contract, order)
        print("Order placed")
        self.simplePlaceOid += 1
    
app = TestApp()
for raw_message in sub.listen():
    if raw_message["type"] != "message":
        continue
    # print(raw_message)
    message = json.loads(raw_message["data"])["action"]
    if(message):
        if(message.lower()=="buy"):
            app.buy_order()
        elif(message.lower()=="sell"):
            app.sell_order()