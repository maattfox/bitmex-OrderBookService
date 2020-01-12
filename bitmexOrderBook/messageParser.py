# -*- coding: utf-8 -*-
"""
Created on.

@author: Matt Fox
"""

import json
import datetime
import logging

from bitmexOrderBook.orderBook import OrderBook
from bitmexOrderBook.quote import Quotes
from bitmexOrderBook.trade import Trades


class MessageParser(object):

    def __init__(self):

        self.logger = logging.getLogger('__main__.' + __name__)
        self.logger.info("Initializing MessageParser.")

        self.quotes = Quotes()
        self.trades = Trades()
        self.orderbook = OrderBook("XBTUSD", 0, 1000000, 0.5)


        self.msgCounter = 0


    def parse(self, messages):
        self.msgCounter += 1

        message = json.loads(messages)

        try:
            if message["msg"]["table"] == "orderBookL2":
                self.__parseOrderBook(message)

            if message["msg"]["table"] == "quote":
                self.__parseQuote(message)

            if message["msg"]["table"] == "trade":
                self.__parseTrade(message)

        except KeyError:
            pass


    def __parseQuote(self, message):

        for quote in message['msg']['data']:
            timestamp = datetime.datetime.strptime(quote['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            bidPrice = quote['bidPrice']
            bidSize = quote['bidSize']

            askPrice = quote['askPrice']
            askSize = quote['askSize']

            self.quotes.insert(ut=timestamp, bidPrice=bidPrice, askPrice=askPrice, bidSize=bidSize, askSize=askSize)


    def __parseTrade(self, message):

        for trade in message['msg']['data']:
            timestamp = datetime.datetime.strptime(trade['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()

            side = trade["side"]
            size = trade["size"]
            price = trade["price"]

            self.trades.insert(ut=timestamp, side=side, size=size, price=price)


    def __parseOrderBook(self, message):

        timestamp = message['t']

        if message['msg']['action'] == "insert":
            for item in message['msg']['data']:
                side = item["side"]
                size = item["size"]
                price = item["price"]

                self.orderbook.insert(timestamp=timestamp, side=side, price=price, size=size)

        if message['msg']['action'] == "update":
            for item in message['msg']['data']:
                side = item["side"]
                size = item["size"]
                id = item["id"]

                self.orderbook.update(timestamp=timestamp, side=side, size=size, lobID=id)

        if message['msg']['action'] == "delete":
            for item in message['msg']['data']:
                side = item["side"]
                id = item["id"]

                self.orderbook.delete(timestamp=timestamp, side=side, lobID=id)
