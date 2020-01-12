# -*- coding: utf-8 -*-
"""
Created on.

@author: Matt Fox
"""

import websocket
import threading
import traceback
from queue import Queue
from time import sleep
import time
import json
import logging
import urllib
import math


#
class BitMEXWebsocket:
    """Basic implementation of connecting to BitMEX websocket for streaming realtime data.

    Attributes:
        logger: logging object
        queue: queue object for passing data between threads
        endpoint: URL for websocket
        symbol: Instrument from BitMEX
        data: list to hold historical data
        keys: keys stored in data
        exited: Boolean for closing websocket gracefully
        counter: Increments on each message recieved
    """

    def __init__(self, endpoint, symbol, queue, api_key=None, api_secret=None):
        '''Connect to the websocket and initialize data stores.'''
        self.logger = logging.getLogger('__main__.' + __name__)
        self.logger.info("Initializing WebSocket.")

        self.queue = queue

        self.endpoint = endpoint
        self.symbol = symbol

        self.data = {}
        self.keys = {}
        self.exited = False

        self.counter = 0

        # Connect to websocket
        wsURL = self.__get_url()
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS.')

        # Connected. Wait for data
        self.__wait_for_symbol(symbol)
        self.logger.info('Got all market data. Starting.')


    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()


    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.info("Starting thread")

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.info("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while (not self.ws.sock or not self.ws.sock.connected) and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')


    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''

        self.logger.info("Not authenticating.")
        return []


    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        symbolSubs = ["instrument","orderBookL2", "quote", "trade"]

        subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]


        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(urlParts)


    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            sleep(0.1)


    def __send_command(self, command, args=None):
        '''Send a raw command.'''
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))


    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        self.counter += 1

        self.queue.put('{"t": '+str(time.time())+', "msg": '+json.dumps(message)+' }')
        try:
            table = message.get("table")
            action = message.get("action")
            if action:
                if table not in self.data:
                    self.data[table] = []
        except:
            self.logger.error(traceback.format_exc())


    def __on_error(self, error):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            raise websocket.WebSocketException(error)


    def __on_open(self):
        '''Called when the WS opens.'''
        self.logger.info("Websocket Opened.")


    def __on_close(self):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')