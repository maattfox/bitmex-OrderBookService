# -*- coding: utf-8 -*-
"""
Created on.

@author: Matt Fox
"""

from pymongo import MongoClient
import redis
import logging


class OrderBook(object):
    """Stores orderbook data in redis store and mongodb collection

        Stores orderbook data using a redis store for low-latency seek and
        find. Update, Delete and Insert requests are stored in a Mongodb
        collection.

        Attributes:
            logger: Logging object
            instrument: Instrument name, e.g. "XBTUSD"
            start_time: Start time unixtime
            instrumentId: BitMEX instrument Id
            max_price: Max price of instrument on BitMEX
            price_granularity: Minimum price fluctuation of instrument

            dbClient: PyMongoDB interface
            db: MongoDB reference
            insertsCollection: MongoDB collection reference
            updatesCollection: MongoDB collection reference
            deletesCollection: MongoDB collection reference

            redisBids: Redis Bids DB reference
            redisAsks: Redis Asks DB reference

    """

    def __init__(self, instrument, start_time, max_price, price_granularity):
        """Intialises orderbook class

        Creates a new order book to replicate trading activity in BitMex.
        Limit Order Book (LOB) is aggregated at the price level, i.e. orders
        placed, executed etc are not know, only the size is updated at each level.

        Bids and asks are created as a list, where list index is to
        price * (1/price granularity), price granularity for bitcoin for
        example is $0.5 every increment.
        """

        self.logger = logging.getLogger('__main__.' + __name__)
        self.logger.info("Initializing OrderBook.")

        self.instrument = instrument
        self.start_time = start_time
        # Bitmex treats prices as ids,
        self.instrumentId = 8800000000
        self.max_price = max_price
        self.price_granularity = price_granularity
        self.index_granularity = int(1 / self.price_granularity)

        # Setup MongoDB Collections
        self.logger.info("Setting up OB-INSERTS MongoDB Collection")
        self.dbClient = MongoClient('mongodb', 27017)
        self.db = self.dbClient.bitmexOrderBook

        # Check if collections already exist, else drop all and create new collection
        if "ob_insert" in self.db.list_collection_names():
            self.db.ob_insert.drop()

        self.insertsCollection = self.db.ob_insert

        self.logger.info("Setting up OB-UPDATES MongoDB Collection")

        if "ob_update" in self.db.list_collection_names():
            self.db.ob_update.drop()

        self.updatesCollection = self.db.ob_update

        self.logger.info("Setting up OB-INSERTS MongoDB Collection")

        if "ob_delete" in self.db.list_collection_names():
            self.db.ob_delete.drop()

        self.deletesCollection = self.db.ob_delete


        # Setup Redis in memory DB
        self.logger.info("Creating OrderBook Redis DBs")

        self.redisBids = redis.Redis(host='redis', port=6379, db=0)
        self.redisAsks = redis.Redis(host='redis', port=6379, db=1)

        # flush all existing databases
        self.redisBids.flushall()
        self.redisAsks.flushall()

        # set key values for price and size
        self.logger.info("Setting up OrderBook ASKS Redis DB")
        pipe = self.redisAsks.pipeline()
        for index in range(self.max_price * int(1 / price_granularity)):
            pipe.lpush(index, "0")
        pipe.execute()

        self.logger.info("Setting up OrderBook BIDS Redis DB")

        pipe = self.redisBids.pipeline()
        for index in range(self.max_price * int(1 / price_granularity)):
            pipe.lpush(index, "0")
        pipe.execute()


    def insert(self, timestamp, side, price, size):
        """ Inserts a new size at a empty price level into the LOB."""
        if side == 'Sell':
            self.redisAsks.set(str(int(price * self.index_granularity)), str(size))

        if side == 'Buy':
            self.redisBids.set(str(int(price * self.index_granularity)), str(size))

        data = {"timestamp": timestamp,
                "side": side,
                "price": price,
                "size": size}

        self.insertsCollection.insert_one(data)


    def update(self,timestamp, side, size, lobID):
        """Updates the size at a price level in the LOB"""
        price = (8800000000 - lobID) / 100

        if side == 'Sell':
            self.redisAsks.set(str(int(price * self.index_granularity)), str(size))

        if side == 'Buy':
            self.redisBids.set(str(int(price * self.index_granularity)), str(size))

        data = {"timestamp": timestamp,
                "side": side,
                "price": price,
                "size": size}

        self.updatesCollection.insert_one(data)


    def delete(self,timestamp, side, lobID):
        """Removes size at a price level in the LOB"""
        price = (8800000000 - lobID) / 100

        if side == 'Sell':
            self.redisAsks.set(str(int(price * self.index_granularity)), str(0))

        if side == 'Buy':
            self.redisBids.set(str(int(price * self.index_granularity)), str(0))


        data = {"timestamp": timestamp,
                "side": side,
                "price": price}

        self.deletesCollection.insert_one(data)
