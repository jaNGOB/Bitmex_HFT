#
# Jan Gobeli
# 02.2021
# This file contains functions used to analyze tick data fetched by the Bitmex websocket.
#

import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt


def get_lvl_color(value, max_, min_, colmap):
    """
    This function creates the colors for the orderbook plots. If the size of the the sum of orders 
    is big, it will be bright/visible otherwise dark to blend in with the background of the plot.

    :param value: (float) the size of the orders waiting to be executed at a specific price level.
    :param max_: (float) the maximum size of orders waiting to be executed at one price level.
    :param lower_thres: (float) lower threshold to when the size is too small and should not be displayed. 
    :param upper_thres: (float) upper threshold of when the color should be as bright/visible as possible.

    :return: RGB color code used for the plot.
    """

    if value == None:
        return (0, 0, 0)

    elif value <= min_:
        return (0, 0, 0)
    
    elif value >= max_:
        return colmap(1.0)

    lvl = value / max_
    return colmap(lvl)

    """
    lvl = value / max_

    jet = cm.get_cmap('jet', 12)

    if lvl >= upper_thres:
        return (0, 1, 1)

    elif lvl >= mid_thres or lvl < upper_thres:
        lvl_inside = lvl / upper_thres
        return (0, lvl_inside, lvl_inside)

    elif lvl > lower_thres or lvl < mid_thres:
        lvl_inside = lvl / mid_thres
        return (lvl_inside, lvl_inside, 0)
        
    else:
        return (0, 0, 0)
    """

def create_orderbook(tmp):
    """
    Recreate the orderbook based on the initial push of the Bitmex websocket.

    :param tmp: pandas dataframe containing price levels and sizes.

    :return orderbook: dictionary of the orderbook.
    """
    orderbook = {'bid': {},'ask': {}}

    for n in range(len(tmp)):
        if tmp.side[n] == 'Buy':
            orderbook['bid'][float(tmp.price[n])] = int(tmp['size'][n])
        else:
            orderbook['ask'][float(tmp.price[n])] = int(tmp['size'][n])
    return orderbook


def get_bid_ask(tmp, orderbook):
    """
    Create three lists containing the best bids, asks and timestamp
    over the whole dataframe (tmp). 

    :param tmp: dataframe of changes/orders.
    :param orderbook: (dict) inital orderbook created.

    :return orderbook: final orderbook after all changes.
    :return best_bid: list of all best bids.
    :return best_ask: list of all best asks.
    :return time: list of all timestamps.
    """
    best_bid = [max(orderbook['bid'])]
    best_ask = [min(orderbook['ask'])]
    time = [tmp.index[0]]
    for n in tqdm(range(len(tmp))):
        side = tmp.side[n]
        price = tmp.price[n]
        tmp_size = tmp['size'][n]
        tmp_time = tmp.index[n]
        if side == 'Buy':
            if price > best_bid[-1]:
                orderbook['bid'][price] = tmp_size
                best_bid.append(price)
                best_ask.append(best_ask[-1])
                time.append(tmp_time)
            elif price == best_bid[-1] and tmp_size != None:
                orderbook['bid'][price] = tmp_size
                best_bid.append(price)
                best_ask.append(best_ask[-1])
                time.append(tmp_time)
            elif price == best_bid[-1] and tmp_size == None:
                del orderbook['bid'][price]
                best_bid.append(max(orderbook['bid']))
                best_ask.append(best_ask[-1])
                time.append(tmp_time)
            elif tmp_size == None:
                del orderbook['bid'][price]
            else:
                orderbook['bid'][price] = tmp_size
        else:
            if price < best_ask[-1]:
                orderbook['ask'][price] = tmp_size
                best_ask.append(price)
                best_bid.append(best_bid[-1])
                time.append(tmp_time)
            elif price == best_ask[-1] and tmp_size != None:
                orderbook['ask'][price] = tmp_size
                best_ask.append(price)
                best_bid.append(best_bid[-1])
                time.append(tmp_time)
            elif price == best_ask[-1] and tmp_size == None:
                del orderbook['ask'][price]
                best_ask.append(min(orderbook['ask']))
                best_bid.append(best_bid[-1])
                time.append(tmp_time)
            elif tmp_size == None:
                del orderbook['ask'][price]
            else:
                orderbook['ask'][price] = tmp_size
    return orderbook, best_bid, best_ask, time


def create_levels(df, price_lvl, max_size, min_size, end, colmap):
    """
    Create all necessary lists to plot the levels and their size in the orderbook.

    :param price_lvl: (list) unique price values within the orderbook.
    :param df: (dataframe) events captured by the Bitmex websocket.
    :param max_size: (int) max size of orders waiting on a price level.
    :param end: (datetime) last datetime object in the df.

    :return y: (list) price levels.
    :return x_start: (list) datetimes of when a new order is entered and tracked.
    :return x_end: (list) datetimes of when the above order ends.
    :return col: (list) RGB colors of what color should be allocated.
    """

    y = []
    x_start = []
    x_end = []
    col = []

    for n in tqdm(range(len(price_lvl))):
        plvl = price_lvl[n]

        tmp = df[df.price == price_lvl[n]]
        if len(tmp) > 1:
            for t in range(len(tmp)):
                if t == len(tmp)-1:
                    y.append(plvl)
                    x_start.append(tmp.index[t])
                    x_end.append(end)
                    col.append(get_lvl_color(tmp['size'][t], max_size, min_size, colmap))
                else:
                    y.append(plvl)
                    x_start.append(tmp.index[t])
                    x_end.append(tmp.index[t+1])
                    col.append(get_lvl_color(tmp['size'][t], max_size, min_size, colmap))
        else:
            y.append(plvl)                
            x_start.append(tmp.index[0])
            x_end.append(end)
            col.append(get_lvl_color(tmp['size'][0], max_size, min_size, colmap))
    if len(y) == len(x_start) == len(x_end) == len(col):
        return y, x_start, x_end, col
    else:
        print('Something went wrong! Lengths do not match. Check the size of the inputs.')
