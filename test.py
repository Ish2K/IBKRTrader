from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import *
from ibapi.contract import *
# from ContractSamples import ContractSamples
from decimal import Decimal
import datetime
import redis
import json
# import ast

redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, password="p@ss$12E45")

class TestApp(EClient, EWrapper):
    def __init__(self, symbols: list):
        EClient.__init__(self, self)
        self.conId = None
        self.contract = None
        self.symbols = symbols
        self.reqCounter = 10
        self.reqTickerMapper = {}

    def nextValidId(self, orderId:int):
        print("id", orderId)

        for symbol in self.symbols:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            contract.primaryExchange = "NASDAQ"

            self.reqContractDetails(self.reqCounter, contract)
            self.reqCounter += 1
            print(f"Contract details for {symbol} requested")
            print("Requesting PnL...")

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, "", errorCode, "", errorString)

    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        print("contractDetail: ", reqId, " ", contractDetails)

        # get conId
        self.conId = contractDetails.contract.conId
        self.contract = contractDetails.contract

    def contractDetailsEnd(self, reqId:int):
        print("Contract Details Loaded!")
        print("Contract ID: ", self.conId)
        self.reqPnLSingle(self.reqCounter, "DU7224701", "", self.conId)
        self.reqCounter += 1
        self.reqTickByTickData(self.reqCounter, self.contract, "BidAsk", 0, True)
        self.reqTickerMapper[self.reqCounter] = self.contract.symbol
        self.reqCounter += 1
        # self.reqMktDepth(12, self.contract, 5, False, [])
    
    def pnlSingle(self, reqId: int, pos: Decimal, dailyPnL: float,
              unrealizedPnL: float, realizedPnL: float, value: float):
        # super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        print("Daily PnL Single. ReqId:", reqId, "Position:", str(pos),
            "DailyPnL:", str(dailyPnL), "UnrealizedPnL:", str(unrealizedPnL),
            "RealizedPnL:", str(realizedPnL), "Value:", str(value))
    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float,
                     bidSize: Decimal, askSize: Decimal, tickAttribBidAsk):
        super().tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk)
        # print("BidAsk. ReqId:", reqId,
        #       "Symbol:", self.reqTickerMapper[reqId], 
        #     "Time:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d-%H:%M:%S"),
        #     "BidPrice:", str(bidPrice), "AskPrice:", str(askPrice),
        #     "BidSize:", str(bidSize), "AskSize:", str(askSize),
        #     "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)
        msg = {
                "Time": datetime.datetime.fromtimestamp(time).strftime("%Y%m%d-%H:%M:%S"),
                "Symbol": self.reqTickerMapper[reqId],
                "BidPrice": str(bidPrice),
                "AskPrice": str(askPrice),
                "BidSize": str(bidSize),
                "AskSize": str(askSize),
                "BidPastLow": str(tickAttribBidAsk.bidPastLow),
                "AskPastHigh": str(tickAttribBidAsk.askPastHigh)
                }
        redis_client.publish(self.reqTickerMapper[reqId], json.dumps(msg))

    def updateMktDepthL2(self, reqId: TickerId, position: int, marketMaker: str,
                         operation: int, side: int, price: float, size: Decimal, isSmartDepth: bool):
        super().updateMktDepthL2(reqId, position, marketMaker, operation, side,  price, size, isSmartDepth)
        print("UpdateMarketDepthL2. ReqId:", reqId, "Position:", position, "MarketMaker:", marketMaker, "Operation:",
              operation, "Side:", side, "Price:", str(price), "Size:", str(size), "isSmartDepth:", isSmartDepth)

        # redis part, uncomment when redis to ready to use
        # msg = {
        #         "Time": datetime.datetime.fromtimestamp(time).strftime("%Y%m%d-%H:%M:%S"),
        #         "BidPrice": str(bidPrice),
        #         "AskPrice": str(askPrice),
        #         "BidSize": str(bidSize),
        #         "AskSize": str(askSize),
        #         "BidPastLow": str(tickAttribBidAsk.bidPastLow),
        #         "AskPastHigh": str(tickAttribBidAsk.askPastHigh)
        #         }
        # redis_client.publish("ib-tick", json.dumps(msg))
def main():
    app = TestApp(["GOOG", "AAPL", "MSFT", "AMZN", "META", "TSLA", "NVDA", "PYPL", "ADBE", "NFLX"])

    app.connect("127.0.0.1", 7497, 4)
    app.run()

if __name__ == "__main__":
    main()