# -*- coding: utf-8 -*-
"""
Created on.

@author: Matt Fox
"""

from pymongo import MongoClient
import logging


class Quotes(object):
    """Stores trade in mongodb collection

        Attributes:
            logger: Logging object
            dbClient: PyMongoDB interface
            db: MongoDB database reference
            quoteCollection: MongoDB collection reference
            count: iterator to count how many trades have been stored
    """

    def __init__(self):
        """Inits Quote Class"""

        self.logger = logging.getLogger('__main__.' + __name__)
        self.logger.info("Initializing Quotes.")

        self.logger.info("Setting up Quotes MongoDB Collection")
        self.dbClient = MongoClient()
        self.db = self.dbClient.bitmexOrderBook

        if "quotes" in self.db.list_collection_names():
            self.db.quotes.drop()

        self.quoteCollection = self.db.quotes

        self.count = 0


    def insert(self, ut, bidPrice, askPrice, bidSize, askSize):
        """ Inserts item into mongoDB collection"""

        spread = askPrice - bidPrice

        data = {"timestamp": ut,
                "bidPrice": bidPrice,
                "bidSize": bidSize,
                "askPrice": askPrice,
                "askSize": askSize,
                "spread": spread}

        self.quoteCollection.insert_one(data)

        self.count += 1


