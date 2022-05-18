from datetime import datetime
from os import close
import threading
import time
from binance.streams import ThreadedWebsocketManager
from models.node import Node
from numpy import e, sqrt
import numpy as np
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import AsyncClient, Client
from client import send_message
import pandas as pd
import matplotlib.pyplot as plt
import concurrent.futures
import csv
import uuid

# mydict = {}
ordersList = {}
money = 0
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endVate', 'buy', 'sell', 'growth/drop', 'drop_count', 'total', 'closed', 'buy_zscore', 'sell_zscore'])


FILE_NAME = 'RSI-5M-RSI-22'
INTERVAL = '5m'
DESC = ' 5M range'
H_HISTORY = Client.KLINE_INTERVAL_5MINUTE


# mydict = None


def readFile():
    try:
        global mydict

        with open(f'results/db.csv', mode='r') as infile:
            reader = csv.reader(infile)
            # with open('coors_new.csv', mode='w') as outfile:
            # writer = csv.writer(outfile)
            mydict = {rows[1]: {'status': rows[2], 'vwap': rows[3]}
                      for rows in reader}
    except Exception as e:

        # print(mydict)
        print(e)


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


def setDatafFame(symbol):

    kilne_tracker[symbol]['Close'] = pd.to_numeric(
        kilne_tracker[symbol]['Close'])

    kilne_tracker[symbol]['rsi_14'] = get_rsi(
        kilne_tracker[symbol]['Close'], 14)

    # kilne_tracker[symbol].to_csv(
    #     f'rsi/'+symbol+'@data.csv', index=False, header=True)


def readHistory(i):

    print('start reading history of ' + str(i) + ' USDT pairs...')

    try:

        klines = client.get_historical_klines(
            symbol=i, interval=H_HISTORY, start_str="1 hours ago")

        data = pd.DataFrame(klines)

        data[0] = pd.to_datetime(data[0], unit='ms')

        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                        'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

        data = data.drop(columns=['IGNORE',
                                  'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

        # data = data.set_index('Date')
        data['Close'] = pd.to_numeric(
            data['Close'], errors='coerce')
        data['rsi_14'] = get_rsi(data['Close'], 14)

        data['isBuy'] = True
        data['stopLoseDate'] = None

        kilne_tracker[i] = data

        print(i + ' is loaded...')

    except:

        print(i + ' caused error')

    print('Done.')
    print('Start Streaming ---- RSI ----.')


def get_rsi(close, lookback):
    ret = close.diff()
    up = []
    down = []
    for i in range(len(ret)):
        if ret[i] < 0:
            up.append(0)
            down.append(ret[i])
        else:
            up.append(ret[i])
            down.append(0)

    up_series = pd.Series(up)
    down_series = pd.Series(down).abs()

    up_ewm = up_series.ewm(com=lookback - 1, adjust=False).mean()
    down_ewm = down_series.ewm(com=lookback - 1, adjust=False).mean()

    rs = up_ewm/down_ewm

    rsi = 100 - (100 / (1 + rs))

    rsi_df = pd.DataFrame(rsi).rename(
        columns={0: 'rsi'}).set_index(close.index)

    rsi_df = rsi_df.dropna()

    return rsi_df[3:]


def checkSell(holdrate, rate, order, price, time):

    global money
    order.sellPrice = price
    order.endDate = time
    order.rate = rate
    order.sellZscore = kilne_tracker[order.symbol].iloc[-1]['rsi_14']

    difference = (order.endDate - order.startDate)
    total_seconds = difference.total_seconds()
    hours = divmod(total_seconds, 60)[0]

    if rate <= -3:

        order.isSold = True
        value = order.amount * (rate / 100)
        money += value
        ordersList[order.symbol]['isBuy'] = False
        # ordersList[order.symbol]['isShut'] = True
        ordersList[order.symbol]['date'] = datetime.now()

        message = '--- RSI ---\n' + 'Order: run away\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + str(time) + '\nSupport price: ' + str(price) + '\ngrowth/drop: ' + str(rate) + '\nRSI: ' + \
            str(kilne_tracker[order.symbol].iloc[-1]['rsi_14'])

        send_message(message)
        print(money)

    elif order.sellZscore >= 70.0:

        order.isSold = True
        value = order.amount * (rate / 100)
        money += value
        ordersList[order.symbol]['isBuy'] = False
        # ordersList[order.symbol]['isShut'] = True
        ordersList[order.symbol]['date'] = datetime.now()

        message = '--- RSI ---\n' + 'Order: Sell\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + str(time) + '\nSupport price: ' + str(price) + '\ngrowth/drop: ' + str(rate) + '\nRSI: ' + \
            str(kilne_tracker[order.symbol].iloc[-1]['rsi_14'])

        send_message(message)
        print(money)

    excel_df.loc[excel_df['id'] == order.id, 'sell'] = order.sellPrice
    excel_df.loc[excel_df['id'] == order.id, 'endDate'] = order.endDate
    excel_df.loc[excel_df['id'] == order.id, 'closed'] = order.isSold
    excel_df.loc[excel_df['id'] == order.id, 'buy'] = order.buyPrice
    excel_df.loc[excel_df['id'] == order.id, 'drop_count'] = order.drop_count
    excel_df.loc[excel_df['id'] == order.id, 'sell_zscore'] = order.sellZscore
    excel_df.loc[excel_df['id'] == order.id, 'growth/drop'] = order.rate

    excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')


def sell(s, time, price):

    list = ordersList['list']
    p = float(price)

    # try:
    for i in list:

        if i.isSold == False and i.symbol == s:

            rate = ((float(price) - float(i.buyPrice)) /
                    float(i.buyPrice)) * 100
            holdrate = ((float(price) - float(i.buyPrice)) /
                        float(i.buyPrice)) * 100

            checkSell(holdrate, rate, i, p, time)

        # print(money)
    # except Exception as e:
    #     print(e)


def buy(symbol, time):

    # try:

    rsi = kilne_tracker[symbol].iloc[-1,
                                     kilne_tracker[symbol].columns.get_loc('rsi_14')]

    # status = mydict[symbol]['status']
    # vwap = pd.to_numeric(mydict[symbol]['vwap'])

    list = [x for x in ordersList['list'] if x.isSold == False]

    # if ordersList[symbol]['isShut'] == True:
    #     diff = time - ordersList[symbol]['date']
    #     total_seconds = diff.total_seconds()
    #     hours = divmod(total_seconds, 3600)[0]
    #     if hours >= 6:
    #         ordersList[symbol]['isShut'] = False

    # if rsi <= 30.0 and ordersList[symbol]['isBuy'] == False and len(list) < 20 and ordersList[symbol]['isShut'] == False and status == 'True':
    if rsi <= 30.0 and ordersList[symbol]['isBuy'] == False and len(list) < 20:
        # print(vwap)
        # print(status)

        ordersList[symbol]['isBuy'] = True
        ordersList
        order = Order(
            id=uuid.uuid1(),
            type='rsi',
            symbol=symbol,
            interval=INTERVAL + DESC,
            buyPrice=kilne_tracker[symbol].iloc[-1]['Close'],
            sellPrice=kilne_tracker[symbol].iloc[-1]['Close'] +
            (kilne_tracker[symbol].iloc[-1]['Close'] * 0.05),
            amount=500,
            startDate=time,
            dropRate=5,
            buyZscore=rsi
        )
        ordersList['list'].append(order)

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

        message = '--- RSI POS NEG ---\n' + 'Id: ' + str(order.id) + '\nOrder: Buy\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nBuy price: ' + \
            str(order.buyPrice) + '\nFrom: ' + \
            str(order.startDate) + '\nRSI: ' + str(rsi)
        send_message(message)
    # except:
    #     pass


def init():
    ordersList['list'] = []
    for pair in exchange_pairs:
        ordersList[pair] = {}
        ordersList[pair]['isBuy'] = False
        ordersList[pair]['isShut'] = False


def realtime(msg):

    if 'data' in msg:
        # Your code
        handle_socket(msg['data'])

    else:
        stream.stream_error = True


def updateFrame(symbol, msg):

    try:

        # readFile()

        time = pd.to_datetime(msg['k']['t'], unit='ms')
        symbol = msg['s']

        check = np.where(
            kilne_tracker[symbol].iloc[-1]['Date'] == time, True, False)

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

            buy(symbol, time)
            kilne_tracker[symbol] = kilne_tracker[symbol].append({
                'Date': time,
                'Open': msg['k']['o'],
                'High': msg['k']['h'],
                'Low': msg['k']['l'],
                'Close': msg['k']['c'],
                'Volume': msg['k']['v'],
                'Quote_Volume': msg['k']['q'],
            }, ignore_index=True)

        setDatafFame(symbol=symbol)

    except Exception as e:

        print(e)
        print('Error while updating data...')


def handle_socket(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    close = msg['k']['c']
    symbol = msg['s']

    updateFrame(symbol, msg)

    sell(s=symbol, time=time, price=close)


init()

t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(readHistory, exchange_pairs)

t2 = time.perf_counter()


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


# readFile()

stream = Stream()

stream.start()

stream.bm.join()
