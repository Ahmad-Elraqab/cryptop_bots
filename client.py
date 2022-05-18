from config import exchange_pairs, telegram_bot_id
import pandas as pd
import requests

# STREAM API


kilne_tracker = {}
kilne_tracker2 = {}
data = pd.DataFrame(kilne_tracker)
current_volume = 0


# def handle_socket_message(msg):
#     # print(msg)
#     current_volume = float(msg['k']['q'])
#     symbol = msg['s']

#     if symbol in kilne_tracker:
#         value = kilne_tracker[symbol]
#         kilne_tracker[symbol] = current_volume

#         if current_volume > 3 * value:
#             print(current_volume)

#     else:
#         kilne_tracker[symbol] = current_volume


def handle_socket_message_30m(msg):
    volume = float(msg['k']['q'])
    rate = float(msg['k']['c']) / float(msg['k']['o'])
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    if symbol in kilne_tracker:
        if time in kilne_tracker[symbol]:
            kilne_tracker[symbol][time]['volume'] = volume
            kilne_tracker[symbol][time]['rate'] = rate

        else:
            getVolume(symbol=symbol)
            kilne_tracker[symbol][time] = {}
            kilne_tracker[symbol][time]['volume'] = volume
            kilne_tracker[symbol][time]['rate'] = rate

    else:

        kilne_tracker[symbol] = {}


def getVolume(symbol):
    # print(list(kilne_tracker[symbol].values())[-1])
    # print(len(list(kilne_tracker[symbol].values())))
    time_stamp = list(kilne_tracker[symbol])

    if len(time_stamp) >= 3:
        data1 = kilne_tracker[symbol][time_stamp[-1]]['volume']
        rate1 = kilne_tracker[symbol][time_stamp[-1]]['rate']
        data2 = kilne_tracker[symbol][time_stamp[-2]]['volume']
        rate2 = kilne_tracker[symbol][time_stamp[-2]]['rate']
        data3 = kilne_tracker[symbol][time_stamp[-3]]['volume']
        rate3 = kilne_tracker[symbol][time_stamp[-3]]['rate']

        rate1 + rate2 + rate3
        if rate1 >= 1.015 and rate2 >= 1 and rate3 >= 1 and data1 > data2 and data1 >= 350000:

            message = "VOLUME METHOD " + symbol + " = " + str(data1)
            send_message(message)


def handle_socket_message(msg):

    value = float(msg['k']['c']) / float(msg['k']['o'])
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    if symbol in kilne_tracker2:

        if time in kilne_tracker2[symbol]:
            kilne_tracker2[symbol][time] = value
        elif (value >= exchange_pairs[symbol]['rate']):
            kilne_tracker2[symbol][time] = value
            print(value)
            message = symbol + " = " + str(value)
            send_message(message)
        else:
            kilne_tracker2[symbol][time] = value
            print(value)

    else:

        kilne_tracker2[symbol] = {}


def get_url(url, data, telegram_chat_id, files=None):
    url = "https://api.telegram.org/bot1805612273:AAGq8OiJBDy1ktg-4MGEVj-7r4H3jE_2nis/sendMessage?chat_id=" + \
        telegram_chat_id+"&text="+data

    requests.request("GET", url)


def send_message(text, telegram_chat_id, files=None):
    url = "https://api.telegram.org/" + telegram_bot_id + "/sendMessage"
    get_url(url, text, telegram_chat_id, files)
