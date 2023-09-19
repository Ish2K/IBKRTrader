from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
import pandas as pd
import datetime
import pytz
import threading
import time
import pickle
import asyncio
import os
import numpy as np
from datetime import timedelta
from decimal import Decimal
import sys

class IBapi(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.edate = 0
        self.marketopen = 0
        self.datacollector = 0

        self.close = 0
        self.localsymbol = 'ESU3'

        self.stochobs = 14
        self.band = 1

        self.stochbuy = 10
        self.stochsell = 90
        self.nqtick = 400
        self.pimbsell = -0.9
        self.pimbbuy = 0.9

        # get PnL
        self.pnldata = pd.DataFrame([], columns=['RealizedPnL'], index=[0])
        self.contractid = 0

        # get all positions
        self.all_positions = pd.DataFrame([], columns=['Position'], index=[0])

        utc_now = datetime.datetime.now(pytz.utc)
        timezone = pytz.timezone('America/New_York')
        localized_now = utc_now.astimezone(timezone)
        self.now_aware = pd.to_datetime(localized_now)

        data = {'DateTime': [self.now_aware]}
        self.tick = pd.DataFrame(data)
        self.tick['Close']=0
        self.tick['Bid']=0
        self.tick['Ask']=0
        self.tick['Bid_size']=0
        self.tick['Ask_size']=0

        self.signal = ''
        self.pos = 0
        self.prof = 0
        self.goal = 100
        self.lotsize = 1

    def tickPrice(self, reqId, tickType, price, attrib):
        super().tickPrice(reqId, tickType, price, attrib)

        utc_now = datetime.datetime.now(pytz.utc)
        timezone = pytz.timezone('America/New_York')
        timestamp = utc_now.astimezone(timezone)

        if tickType == 4: #Close
            self.tick.loc[len(self.tick)] = [timestamp,price,0,0,0,0]

        if tickType ==1: #Bid
            self.tick.loc[len(self.tick)] = [timestamp,0,price,0,0,0]

        if tickType ==2: #Ask
            self.tick.loc[len(self.tick)] = [timestamp,0,0,price,0,0]

        df = self.tick
        #print("Saving Tickprice Data...")
        df.to_csv("tickdata.csv")

    def tickSize(self, reqId, tickType, size):
        super().tickSize(reqId, tickType, size)

        utc_now = datetime.datetime.now(pytz.utc)
        timezone = pytz.timezone('America/New_York')
        timestamp = utc_now.astimezone(timezone)

        if tickType ==0: #Bid size
            self.tick.loc[len(self.tick)] = [timestamp,0,0,0,size,0]

        if tickType ==3: #Ask size
            self.tick.loc[len(self.tick)] = [timestamp,0,0,0,0,size]

        df = self.tick
        #print("Saving Ticksize Data...")
        df.to_csv("tickdata.csv")

    def Fut_contract(self, symbol, local_symbol='ESU3', secType='FUT', exchange='CME', currency='USD'):  # change here
        ''' custom function to create contract '''
        contract = Contract()
        contract.symbol = symbol
        contract.localSymbol = local_symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

    def contractDetails(self, reqId, contractDetails):
        contract = contractDetails.contract
        self.contractid = contract.conId
        #print('contractid:',self.contractid)
        #return self.contractid

    def pnlSingle(self, reqId: int, pos: Decimal, dailyPnL: float, unrealizedPnL: float, realizedPnL: float,
                  value: float):
        # super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        index = str(reqId)
        self.pnldata.loc[index] = realizedPnL
        #print(self.pnldata)
        return self.pnldata.loc[index]

    def position(self, account:str, contract:Contract, pos:float, avgCost:float):
        ''' Read information about open positions '''
        if contract.localSymbol =='ESU3':
            index = str(contract.localSymbol)
            self.all_positions.loc[index] = pos

async def wait():
    await asyncio.sleep(0.5)

app = IBapi()
FUT_contract = app.Fut_contract('ES')

def run_loop():
    app.run()

def getquote():
    app.reqMarketDataType(1)
    livedata = app.reqMktData(1, FUT_contract, '', False, False, [])

def strategy():
    df = pd.read_csv("tickdata.csv")
    df = df.drop('Unnamed: 0', 1)
    df = df.iloc[1:, :]
    df = df.replace(0, np.nan).ffill()
    df['DateTime'] = pd.to_datetime(df['DateTime'], infer_datetime_format=True).dt.tz_convert(
        'America/New_York')
    pd.set_option('display.max_columns', 10000)
    pd.set_option('display.width', 10000)

    utc_now = datetime.datetime.now(pytz.utc)
    timezone = pytz.timezone('America/New_York')
    localized_now = utc_now.astimezone(timezone)
    edate = pd.to_datetime(localized_now)

    rate = app.band
    obs = app.stochobs
    freq = str(rate) + 'min'

    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df = df.set_index('DateTime')
    df['rollingmax'] = df['Close'].rolling(freq, min_periods=1).max()
    df['rollingmin'] = df['Close'].rolling(freq, min_periods=1).min()

    startdate = edate - timedelta(minutes=rate * obs)
    my_list = [*range(0, obs)]
    stoch_df = pd.DataFrame({'Close Prices': my_list})
    stoch_df.index = pd.date_range(startdate, edate, freq='1min', closed='right')
    stoch_df.index.name = 'DateTime'
    stoch_df.index = pd.to_datetime(stoch_df.index)
    stoch_df = stoch_df.sort_index().reset_index()
    stoch_df = pd.merge_asof(stoch_df, df, on='DateTime', direction='backward')
    stoch_df['highesthigh'] = stoch_df['rollingmax'].max()
    stoch_df['lowestlow'] = stoch_df['rollingmin'].min()
    stoch_df['%K'] = abs(
        (stoch_df['Close'] - stoch_df['lowestlow']) / (stoch_df['highesthigh'] - stoch_df['lowestlow'])) * 100
    stoch_df['%D'] = stoch_df['%K'].rolling(window=3).mean()

    NQTICK = pd.read_csv("NQTICK.csv")
    NQTICK = NQTICK.iat[0,3]

    signal_df = stoch_df.tail(1)
    columns_to_drop = ['Close Prices', 'rollingmax', 'rollingmin', 'highesthigh', 'lowestlow']
    signal_df = signal_df.drop(columns=columns_to_drop)
    signal_df['pimb'] = (
                (signal_df['Bid_size'] - signal_df['Ask_size']) / (signal_df['Bid_size'] + signal_df['Ask_size']))
    signal_df['NQTICK'] = NQTICK

    pkbuy = signal_df['%K'] <= app.stochbuy
    pdbuy = signal_df['%D'] <= app.stochbuy
    imbbuy = (signal_df['pimb'] >= app.pimbbuy)
    NQTICKbuy = NQTICK <= -400

    pksell = signal_df['%K'] >= app.stochsell
    pdsell = signal_df['%D'] >= app.stochsell
    imbsell = (signal_df['pimb'] <= app.pimbsell)
    NQTICKsell = NQTICK >= 400

    signal_df['stochk'] = np.where(pkbuy,'BUY',
                                  np.where(pksell,'SELL','NO TRADE'))
    signal_df['stochd'] = np.where(pdbuy, 'BUY',
                                   np.where(pdsell, 'SELL', 'NO TRADE'))
    signal_df['imb%'] = np.where(imbbuy,'BUY',
                                 np.where(imbsell,'SELL','NO TRADE'))

    signal_df['signal'] = np.where(pkbuy & pdbuy & imbbuy,'BUY',
                                   np.where(pkbuy & pdbuy & NQTICKbuy,'BUY',
                                            np.where(imbbuy & NQTICKbuy,'BUY',
                                                     np.where(pksell & pdbuy & imbsell,'SELL',
                                                              np.where(pksell & pdbuy & NQTICKsell,'SELL',
                                                                       np.where(imbsell & NQTICKsell,'SELL','NO TRADE'))))))

    print(signal_df)
    app.signal = signal_df.iat[0,13]
    app.close = signal_df.iat[0,1]

def clock():
    utc_now = datetime.datetime.now(pytz.utc)
    timezone = pytz.timezone('America/New_York')
    localized_now = utc_now.astimezone(timezone)
    app.edate = pd.to_datetime(localized_now)

    current_datetime = pd.Timestamp.now()
    current_date = current_datetime.date()
    datestr = str(current_date)
    tradestart = ' 9:30:00'
    tradestart_str = datestr + tradestart
    app.marketopen = pd.to_datetime(tradestart_str, utc=False).tz_localize('America/New_York')

def conid():
    app.reqContractDetails(1,FUT_contract)
    #print(app.contractid)

def pnl(): #ERROR 1 102 Duplicate ticker id
    app.reqPnLSingle(1, "DU3918796", "", app.contractid)
    SinglePnL = app.pnldata
    app.prof = SinglePnL['RealizedPnL'].sum()

def position():
    app.reqPositions()
    CurPos = app.all_positions
    app.pos = CurPos['Position'].sum()

def buyorder():
    print('buy')

def sellorder():
    print('sell')

def notrade():
    print('no trade')

def read_data(contract):
    print("Reading data...")
    getquote()

def scalping_algo():
    print("scalping algo is run here")
    while(True):
        if(os.path.exists("tickdata.csv")):
            while(True):
                try:
                    clock()
                    current_datetime = pd.Timestamp.now()
                    current_date = current_datetime.date()
                    datestr = str(current_date)
                    tradestart = ' 9:30:00'
                    tradestart_str = datestr+tradestart
                    marketopen = pd.to_datetime(tradestart_str,utc=False).tz_localize('America/New_York')
                    data_collector = marketopen - timedelta(minutes=app.band * app.stochobs) #fix here

                    conid()

                    if (app.edate > data_collector) & (app.edate > app.marketopen):
                        pnl()
                        if app.prof <= app.goal:
                            position()
                            if app.pos < app.lotsize:
                                strategy()
                                trade = app.signal
                                if trade == 'BUY':
                                    buyorder()
                                elif trade == 'SELL':
                                    sellorder()
                                elif trade == 'NO TRADE':
                                    notrade()
                            else:
                                print('HOLD')
                        else:
                            print('reached daily goal')
                            # sys.exit() # get help to shut down
                            break
                    elif (app.edate > data_collector) & (app.edate < app.marketopen):
                        print('collecting data... waiting for market open')
                    else:
                        print('waiting for data collection to start')
                except Exception as e:
                    print(str(e))
                    continue
            asyncio.run(wait())

app.connect('127.0.0.1', 7497, 998)

# Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
reading_data = threading.Thread(target=read_data, args=(FUT_contract,)) #get this error: No columns to parse from file
scalping_thread = threading.Thread(target=scalping_algo, daemon=True) #get help when no data

api_thread.start()
reading_data.start()
scalping_thread.start()