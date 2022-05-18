from datetime import datetime
import threading
import uuid
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
import numpy as np
from pandas.core.frame import DataFrame
from client import send_message
from config import API_KEY, API_SECRET, exchange_pairs
import pandas as pd
import time
import os
import errno
import concurrent.futures

FILE_NAME = 'RSI-1M-Direct-Max'
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'drop_count', 'total', 'closed', 'buy_zscore', 'sell_zscore'])
ordersList = {}

INTERVAL = '15m'
H_HISTORY = Client.KLINE_INTERVAL_15MINUTE
PART = '-'
temp = False
tempTime = None
c_df = pd.DataFrame(columns=['s', 'status', 'price', 'buy'])

avg_df = pd.DataFrame(columns=['s', 'AVG'])


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, dropRate, buyZscore):

        self.id = id
        self.type = type
        self.symbol = symbol
        self.interval = interval
        self.buyPrice = buyPrice
        self.sellPrice = sellPrice
        self.amount = amount
        self.startDate = startDate
        self.dropRate = dropRate
        self.total = buyPrice
        self.rate = None
        self.endDate = startDate
        self.drop_count = 1
        self.isSold = False
        self.sellZscore = None
        self.buyZscore = buyZscore
        self.isHold = False
        self.hold = buyPrice
        self.high = 0
        self.low = 0


def readHistory(i):

    global c_df

    print('start reading history of ' + str(i) + ' USDT pairs...')

    klines = client.get_historical_klines(
        symbol=i, interval=H_HISTORY, start_str="10 days ago")

    data = pd.DataFrame(klines)

    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    data['DIF_20'] = 0
    data['DIF_48'] = 0
    data['DIF_84'] = 0

    try:
        c_df = c_df.append(
            {'s': i, 'status': False, 'price': data.iloc[-1, 4], 'buy': False}, ignore_index=True)
    except Exception as e:
        print(e)

    kilne_tracker[i] = data

    print(i + ' is loaded...')


def calcVWAP(symbol, msg, inte):

    high = pd.to_numeric(kilne_tracker[symbol]['High'])
    low = pd.to_numeric(kilne_tracker[symbol]['Low'])
    close = pd.to_numeric(kilne_tracker[symbol]['Close'])
    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    value1 = ((high + low + close) / 3 * volume).rolling(inte).sum()

    value2 = volume.rolling(inte).sum()

    kilne_tracker[symbol]['DIF_' + str(inte)] = value1 / value2


def checkCross(symbol, msg):

    try:
        check = c_df.loc[c_df['s'] == symbol, 'status'][0]
        vwap20 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('DIF_20')]
        vwap48 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('DIF_48')]
        vwap84 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('DIF_84')]

        if vwap20 >= vwap48 and vwap48 >= vwap84 and check == False:

            c_df.loc[c_df['s'] == symbol,
                     'price'] = pd.to_numeric(msg['k']['c'])
            c_df.loc[c_df['s'] == symbol, 'status'] = True

    except:
        # print('.')
        pass


def buy(symbol, time):

    try:
        check = c_df.loc[c_df['s'] == symbol, 'status'][0]
        check_buy = c_df.loc[c_df['s'] == symbol, 'buy'][0]

        if check == True and check_buy == False:

            c_df.loc[c_df['s'] == symbol, 'buy'] = True

            order = Order(
                id=uuid.uuid1(),
                type='rsi',
                symbol=symbol,
                interval=INTERVAL,
                buyPrice=kilne_tracker[symbol].iloc[-1]['Close'],
                sellPrice=kilne_tracker[symbol].iloc[-1]['Close'] +
                (kilne_tracker[symbol].iloc[-1]['Close'] * 0.05),
                amount=500,
                startDate=time,
                dropRate=5,
                buyZscore=None
            )
            ordersList.append(order)

            msg = {
                'id': order.id,
                'symbol': order.symbol,
                'type': order.type,
                'interval': order.interval,
                'amount': order.amount,
                'startDate': order.startDate,
                'endDate': order.endDate,
                'buy': order.buyPrice,
                'sell': order.sellPrice,
                'drop_count': order.drop_count,
                'total': order.total,
                'closed': order.isSold,
                'growth/drop': order.rate,
                'buy_zscore': order.buyZscore,
                'sell_zscore': order.sellZscore
            }
            global excel_df
            excel_df = excel_df.append(msg, ignore_index=True)

            excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv', header=False)

            message = '--- RSI ---\n' + 'Id: ' + str(order.id) + '\nOrder: Buy\n' + 'Symbol: ' + \
                str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nBuy price: ' + \
                str(order.buyPrice) + '\nFrom: ' + \
                str(order.startDate)

            send_message(message)
    except Exception as s:

        # print('error buy ' + str(symbol))
        # print(s)
        pass


def checkSell(rate, order, price, time):

    vwap20 = kilne_tracker[order.symbol].iloc[-1,
                                              kilne_tracker[order.symbol].columns.get_loc('DIF_20')]
    vwap48 = kilne_tracker[order.symbol].iloc[-1,
                                              kilne_tracker[order.symbol].columns.get_loc('DIF_48')]

    order.sellPrice = price
    order.endDate = time
    order.rate = rate
    order.sellZscore = kilne_tracker[order.symbol].iloc[-1]['rsi_14']

    # difference = (order.endDate - order.startDate)
    # total_seconds = difference.total_seconds()
    # hours = divmod(total_seconds, 60)[0]

    if vwap20 < vwap48:

        c_df.loc[c_df['s'] == order.symbol, 'status'] = False
        c_df.loc[c_df['s'] == order.symbol, 'buy'] = False

        order.isSold = True
        ordersList[order.symbol]['date'] = datetime.now()

        message = '--- RSI ---\n' + 'Order: Sell\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + str(time) + '\nSupport price: ' + str(price) + '\ngrowth/drop: ' + str(rate) + '\nRSI: ' + \
            str(kilne_tracker[order.symbol].iloc[-1]['rsi_14'])

        send_message(message)

    excel_df.loc[excel_df['id'] == order.id, 'sell'] = order.sellPrice
    excel_df.loc[excel_df['id'] == order.id, 'endDate'] = order.endDate
    excel_df.loc[excel_df['id'] == order.id, 'closed'] = order.isSold
    excel_df.loc[excel_df['id'] == order.id, 'buy'] = order.buyPrice
    excel_df.loc[excel_df['id'] == order.id, 'drop_count'] = order.drop_count
    excel_df.loc[excel_df['id'] == order.id, 'sell_zscore'] = order.sellZscore
    excel_df.loc[excel_df['id'] == order.id, 'growth/drop'] = order.rate

    excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')


def sell(s, time, price):

    list = ordersList
    p = float(price)

    try:
        for i in list:

            if i.isSold == False and i.symbol == s:

                rate = ((float(price) - float(i.buyPrice)) /
                        float(i.buyPrice)) * 100

                checkSell(rate, i, p, time)

    except Exception as e:
        # print(e)
        pass


def updateFrame(symbol, msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    check = np.where(
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Date')] == time, True, False)

    if check == True:

        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Open')] = float(msg['k']['o'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('High')] = float(msg['k']['h'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Low')] = float(msg['k']['l'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Close')] = float(msg['k']['c'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Volume')] = float(msg['k']['v'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol]
                                   .columns.get_loc('Quote_Volume')] = float(msg['k']['q'])
    else:
        kilne_tracker[symbol] = kilne_tracker[symbol].append({
            'Date': time,
            'Open': msg['k']['o'],
            'High': msg['k']['h'],
            'Low': msg['k']['l'],
            'Close': msg['k']['c'],
            'Volume': msg['k']['v'],
            'Quote_Volume': msg['k']['q'],
        }, ignore_index=True)

    calcVWAP(symbol=symbol, msg=msg, inte=20)
    calcVWAP(symbol=symbol, msg=msg, inte=48)
    calcVWAP(symbol=symbol, msg=msg, inte=84)

    checkCross(symbol, msg)
    buy(symbol, time)
    sell(symbol, time, msg['k']['c'])


def handle_socket(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    close = msg['k']['c']
    symbol = msg['s']

    updateFrame(symbol=symbol, msg=msg)


def realtime(msg):

    if 'data' in msg:
        handle_socket(msg['data'])

    else:
        stream.stream_error = True


class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@kline_'+INTERVAL)
        self.multiplex = self.bm.start_multiplex_socket(
            callback=realtime, streams=self.multiplex_list)

        # monitoring the error
        stop_trades = threading.Thread(
            target=stream.restart_stream, daemon=True)
        stop_trades.start()

    def restart_stream(self):
        while True:
            time.sleep(1)
            if self.stream_error == True:
                self.bm.stop_socket(self.multiplex)
                time.sleep(5)
                self.stream_error = False
                self.multiplex = self.bm.start_multiplex_socket(
                    callback=realtime, streams=self.multiplex_list)


send_message('NEW')

t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(readHistory, exchange_pairs)

t2 = time.perf_counter()

print(f'Finished in {t2 - t1} seconds')

stream = Stream()

print(f'Start Streaming....')

stream.start()
stream.bm.join()
