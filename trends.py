from audioop import reverse
import threading
import time
from unittest import case
from binance import Client
import numpy
import pandas
from client import send_message
from config import API_KEY, API_SECRET, exchange_pairs
import concurrent.futures
from binance.client import AsyncClient, Client
from datetime import date, datetime
from binance.streams import ThreadedWebsocketManager
import keyboard

client = Client(api_key=API_KEY, api_secret=API_SECRET)

points = {}


def getData(symbol):

    tickers = client.aggregate_trade_iter(
        symbol=symbol, start_str='1 hour ago')

    data = pandas.DataFrame(tickers)
    data.columns = ['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M']
    data['T'] = pandas.to_datetime(data['T'], unit='ms')

    data = data.assign(DIFF=lambda x: (
        pandas.to_numeric(x['p']) * pandas.to_numeric(x['q'])))

    for i, value in data.iterrows():
        if value['m'] == True:
            data.iloc[i, data.columns.get_loc('DIFF')] = -1 * value['DIFF']
    print(data)
    column = pandas.to_datetime(
        data.iloc[-1, data.columns.get_loc('T')], unit='ms')

    day = int(column.day)
    hour = int(column.hour)
    minute = int(column.minute)

    points[symbol] = {}
    points[symbol] = {
        day: {
            hour: {
                minute: 0,
            },
        },
    }
    print(points)

    try:
        for index, msg in data.iterrows():
            time = pandas.to_datetime(msg['T'], unit='ms')
            qty = pandas.to_numeric(msg['q'])
            type = msg['m']
            price = pandas.to_numeric(msg['p'])
            value = qty * price

            day = time.day
            hour = time.hour
            minute = time.minute

            check_day = numpy.where(
                list(points[symbol].keys())[-1] == day, True, False)

            if check_day == True:
                check_hour = numpy.where(
                    list(points[symbol][day].keys())[-1] == hour, True, False)

                if check_hour == True:
                    check_minute = numpy.where(
                        list(points[symbol][day][hour].keys())[-1] == minute, True, False)

                    if check_minute == True:

                        if type == True:
                            points[symbol][day][hour][minute] -= value
                        else:
                            points[symbol][day][hour][minute] += value

                    else:

                        points[symbol][day][hour][minute] = 0

                else:

                    points[symbol][day][hour] = {
                        minute: 0
                    }
            else:

                points[symbol][day] = {
                    hour: {
                        minute: 0
                    }
                }
    except Exception as e:
        print(e)


t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(getData, exchange_pairs)

t2 = time.perf_counter()


def realtime(msg):
    if 'data' in msg:
        
        handle_socket_message(msg['data'])

    else:
        stream.stream_error = True


def handle_socket_message(msg):

    time = pandas.to_datetime(msg['T'], unit='ms')
    qty = pandas.to_numeric(msg['q'])
    price = pandas.to_numeric(msg['p'])
    symbol = msg['s']
    type = msg['m']
    value = qty * price

    day = time.day
    hour = time.hour
    minute = time.minute
    global points

    check_day = numpy.where(
        list(points[symbol].keys())[0] == day, True, False)

    if check_day == True:
        check_hour = numpy.where(
            list(points[symbol][day].keys())[0] == hour, True, False)

        if check_hour == True:
            check_minute = numpy.where(
                list(points[symbol][day][hour].keys())[-1] == minute, True, False)

            if check_minute == True:

                if type == True:
                    print('sell')
                    points[symbol][day][hour][minute] -= value
                else:
                    print('buy')
                    points[symbol][day][hour][minute] += value

            else:

                points[symbol][day][hour][minute] = 0

        else:

            _list = sum(list(points[symbol][day][hour-1].values()))

            send_message(symbol + '\n\n1 HOUR CH: ' + str(_list), '-756403201')

            points[symbol][day][hour] = {
                minute: 0
            }
    else:

        points[symbol][day] = {
            hour: {
                minute: 0
            }
        }

class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@aggTrade')
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
                time.sleep(1)
                self.stream_error = False
                self.multiplex = self.bm.start_multiplex_socket(
                    callback=realtime, streams=self.multiplex_list)


stream = Stream()
stream.start()
stream.bm.join()