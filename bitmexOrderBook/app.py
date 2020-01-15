# -*- coding: utf-8 -*-
"""
Created on.

@author: Matt Fox
"""

# Python Modules
import logging
from queue import Queue

# Module files
from .messageParser import MessageParser
from .connection import BitMEXWebsocket



def run():
    """ Entry point to the service."""

    i = 0

    # Create websocket connection and run in thread.
    ws = BitMEXWebsocket(endpoint="wss://www.bitmex.com/realtime", symbol="XBTUSD", queue=queue)

    # Run forever
    while (ws.ws.sock.connected):
        # If message exists in queue, parse message
        if queue.qsize() > 0:
            msg = queue.get_nowait()
            parser.parse(msg)
            i += 1

        # Log number of messages recieved by websocket and parsed
        if i >= 100:
            i = 0
            logger.info("msgDelta: {} counter: {} wsCounter: {} trade: {} quotes: {}".format((ws.counter - parser.msgCounter),
                                                                                                parser.msgCounter,
                                                                                                ws.counter,
                                                                                                parser.trades.count,
                                                                                                parser.quotes.count))


def setup_logger():
    """ Prints logger info to terminal"""

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()

    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    logger = setup_logger()

    parser = MessageParser()
    queue = Queue()

    # Run Service
    run()

