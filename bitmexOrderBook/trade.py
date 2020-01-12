# -*- coding: utf-8 -*-
"""
Created on.

@author: Matt Fox
"""

from pymongo import MongoClient
import logging


class Trades(object):
    """Stores trade in mongodb collection

    Attributes:
        logger: Logging object
        dbClient: PyMongoDB interface
        db: MongoDB database reference
        tradesCollection: MongoDB collection reference
        count: iterator to count how many trades have been stored

    """

    def __init__(self):
        """Inits Trade Class"""

        self.logger = logging.getLogger('__main__.' + __name__)
        self.logger.info("Initializing Trades.")

        self.logger.info("Setting up Trades MongoDB Collection")
        self.dbClient = MongoClient()
        self.db = self.dbClient.bitmexOrderBook

        if "trades" in self.db.list_collection_names():
            self.db.trades.drop()

        self.tradesCollection = self.db.trades

        self.count = 0


    def insert(self, ut, side, price, size):
        """ Inserts item into mongoDB collection"""

        data = {"timestamp": ut,
                "side": side,
                "price": price,
                "size": size}

        self.tradesCollection.insert_one(data)

        self.count += 1