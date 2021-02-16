#
# Jan Gobeli
# 02.2021
# Subscription to the Bitmex orderBook L2 websocket for XBTUSD.
# 

import websocket
import threading
import traceback
import datetime
import pandas as pd
from dateutil.parser import parse
from data.database import DataBase
from time import sleep
import logging
import json

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


class BitmexBTCWebsocket:

    def __init__(self):
        
        self.first = True

        self.db = DataBase(True)

        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")

        self.logger.info("Connecting to Bitmex")
        self._connect()
        self.logger.debug('Connected to WS.')


    def exit(self):
        '''
        Call to close the websocket.
        '''
        self.ws.close()
        
    def _connect(self):
        """
        Connect to the L2 Orderbook of XBTUSD from Bitmex. 
        Callbacks are defined and the websocket is opened.
        """
        self.logger.debug("Starting thread")
        self.ws = websocket.WebSocketApp('wss://www.bitmex.com/realtime',
                                         on_message=self._on_message,
                                         on_close=self._on_close,
                                         on_open=self._on_open,
                                         on_error=self._on_error)

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.setDaemon(True)
        self.wst.start()
        self.logger.debug("Started thread")

        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException(
                'Couldn\'t connect to WS! Exiting.')

    def _on_message(self, message):
        """
        Callback from open websocket if a message arrives from the exchange.
        This message is usually a tick and is first unpacked using json.
        It is then passed to the database where the tick will be processed further and saved.

        :params message: Incoming message from Bitmex.
        """

        message = json.loads(message)
        # self.logger.info(message)
        
        if 'subscribe' in message:
            self.logger.info("Subscribed to %s." % message['subscribe'])
        else:
            self.db.new_tick(message)

    def _on_open(self):
        """
        Websocket callback when it is opened.
        """
        self.ws.send('{"op":"subscribe", "args":["trade:XBTUSD", "orderBookL2:XBTUSD"]}')
        self.logger.debug("Websocket Opened.")

    def _on_close(self):
        """
        Websocket callback when it is closed.
        """
        self.logger.info('Websocket Closed')

    def _on_error(self, error):
        """
        Called on fatal websocket errors.
        """
        self.logger.error("Error : %s" % error)
