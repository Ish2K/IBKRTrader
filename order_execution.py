from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import OrderId
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.order_state import OrderState

from ibapi.contract import *
from ibapi.order import *

# from ContractSamples import ContractSamples
from decimal import Decimal
import datetime
import redis
import json
import sys

redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, password="p@ss$12E45")
sub = redis_client.pubsub()
sub.subscribe( 'order-execution')

class Trader(EClient, EWrapper):
    def __init__(self, contract: Contract, order: Order):
        EClient.__init__(self, self)
        self.conId = None
        self.contract = contract
        self.order = order
        self.nextValidId = None
        self.permId2ord = {}
        self.simplePlaceOid = 1
        
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
        self.simplePlaceOid = self.nextOrderId()
        self.placeOrder(self.simplePlaceOid, self.contract, self.order)
        print("Order placed")
        self.simplePlaceOid += 1
    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, "", errorCode, "", errorString)

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)

        print("OpenOrder. PermId:", order.permId, "ClientId:", order.clientId, " OrderId:", orderId,
              
                "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
                "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
                "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty, 
                "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status,
                "MinTradeQty:", order.minTradeQty, "MinCompeteSize:", order.minCompeteSize,
                "MidOffsetAtWhole:", order.midOffsetAtWhole,"MidOffsetAtHalf:" ,order.midOffsetAtHalf,
                "FAGroup:", order.faGroup, "FAMethod:", order.faMethod)
        order.contract = contract
        self.permId2ord[order.permId] = order
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", str(filled),
            "Remaining:", str(remaining), "AvgFillPrice:", str(avgFillPrice),
            "PermId:", str(permId), "ParentId:", str(parentId), "LastFillPrice:",
            str(lastFillPrice), "ClientId:", str(clientId), "WhyHeld:",
            whyHeld, "MktCapPrice:", str(mktCapPrice))

def main():
    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.primaryExchange = "NASDAQ"

    order = Order()
    order.action = "BUY"
    order.totalQuantity = 100
    order.orderType = "LMT"
    order.lmtPrice = 169.00

    app = Trader(contract=contract, order=order)

    app.connect("127.0.0.1", 7497, 1000)
    app.run()
    # print("App is Running!")
    # for raw_message in sub.listen():
    #     if raw_message["type"] != "message":
    #         continue
    #     # print(raw_message)
    #     message = json.loads(raw_message["data"])
    #     if(message):
    #         order = Order()
    #         order.action = message["action"].upper()
    #         order.totalQuantity = message["quantity"]
    #         order.orderType = message["order_type"].upper()
    #         order.lmtPrice = message["price"]
    #         contract = Contract()
    #         contract.symbol = message["symbol"]
    #         contract.secType = "STK"
    #         contract.exchange = "SMART"
    #         contract.currency = "USD"
    #         contract.primaryExchange = "NASDAQ"
    #         app.trade(contract=contract, order=order)
main()
        