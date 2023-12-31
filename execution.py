from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *

import threading
import time
import redis
import json

redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, password="p@ss$12E45")
sub = redis_client.pubsub()
orderIdMap = {}
sub.subscribe( 'order-execution')

class IBapi(EWrapper, EClient):
	def __init__(self):
		EClient.__init__(self, self)

	def nextValidId(self, orderId: int):
		super().nextValidId(orderId)
		self.nextorderId = orderId
		print('The next valid order id is: ', self.nextorderId)

	def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
		print('orderStatus - orderid:', orderId, "Symbol", orderIdMap[orderId]["Symbol"], 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)
		msg = {
			"status": status,
			"RemainingQuantity": remaining,
			"FilledQuantity": filled,
			"Symbol": orderIdMap[orderId]["Symbol"],
			"Action": orderIdMap[orderId]["Action"],
			"AvgFillPrice": avgFillPrice
		}
		redis_client.publish(orderIdMap[orderId]["Symbol"] + "-status", json.dumps(msg))
	
	def openOrder(self, orderId, contract, order, orderState):
		print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action, order.orderType, order.totalQuantity, orderState.status)
		orderIdMap[orderId] = {
			"Symbol": contract.symbol,
			"Action": order.action
		}

	def execDetails(self, reqId, contract, execution):
		print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)


def run_loop():
	app.run()

#Function to create FX Order contract
def FX_order(symbol):
	contract = Contract()
	contract.symbol = symbol[:3]
	contract.secType = 'CASH'
	contract.exchange = 'IDEALPRO'
	contract.currency = symbol[3:]
	return contract

def Stock_order(symbol):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    contract.primaryExchange = "NASDAQ"
    return contract

app = IBapi()
app.connect('127.0.0.1', 7497, 13)

app.nextorderId = None

#Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

#Check if the API is connected via orderid
while True:
	if isinstance(app.nextorderId, int):
		print('connected')
		break
	else:
		print('waiting for connection')
		time.sleep(1)


counter = 0
for raw_message in sub.listen():
	if raw_message["type"] != "message":
		continue
	# print(raw_message)
	message = json.loads(raw_message["data"])
	if(message):
		order = Order()
		order.action = message["action"].upper()
		order.totalQuantity = message["quantity"]
		order.orderType = message["order_type"].upper()
		order.lmtPrice = message["price"]
		order.eTradeOnly = False
		order.firmQuoteOnly = False

		app.placeOrder(app.nextorderId, Stock_order(message["symbol"]), order)
		app.nextorderId += 1
		# counter += 1
		# if(counter > 100):
		# 	break

#Create order object
# order = Order()
# order.action = 'BUY'
# order.totalQuantity = 10
# order.orderType = 'LMT'
# order.lmtPrice = '169.5'
# order.eTradeOnly = False
# order.firmQuoteOnly = False

#Place order
# app.placeOrder(app.nextorderId, Stock_order('AAPL'), order)
#app.nextorderId += 1

time.sleep(3)

#Cancel order 
print('cancelling order')
app.cancelOrder(app.nextorderId)

time.sleep(3)
app.disconnect()