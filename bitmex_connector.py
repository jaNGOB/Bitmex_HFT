import websocket
import threading
import traceback
import datetime
import pandas as pd
from dateutil.parser import parse
from database import DataBase
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
        '''Call this to exit - will close websocket.'''
        self.ws.close()
        
    def _connect(self):
        self.logger.debug("Starting thread")
        self.ws = websocket.WebSocketApp('wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD',
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

        message = json.loads(message)
        # self.logger.info(message)
        
        if 'subscribe' in message:
            self.logger.info("Subscribed to %s." % message['subscribe'])

        self.db.new_tick(message)

    def _on_open(self):
        self.logger.debug("Websocket Opened.")

    def _on_close(self):
        self.logger.info('Websocket Closed')

    def _on_error(self, error):
        'Called on fatal websocket errors. We exit on these.'
        self.logger.error("Error : %s" % error)
    
"""
class BitmexBTCWebsocket:

    def __init__(self):

        self.first = True
        self.create_safefile()
        self.counter = 0

        self.mapper = {}

        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")

        self.logger.info("Connecting to Bitmex")
        self._connect()
        self.logger.debug('Connected to WS.')

        self.data = pd.DataFrame()

    def quote(self, since):
        return self.data[since:]

    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.ws.close()

    def create_safefile(self):
        self.filename = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv')
        self.w = open(self.filename,'w')
        
    def _connect(self):
        self.logger.debug("Starting thread")
        self.ws = websocket.WebSocketApp('wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD',
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

        message = json.loads(message)
        self.logger.debug(message)
        time = datetime.datetime.now()

        self.counter += 1
        #self.logger.info(self.counter)

        if self.counter > 10000: 
            self.w.close()
            self.exit()
        
        if 'subscribe' in message:
            self.logger.info("Subscribed to %s." % message['subscribe'])

        try:
            if message['action'] == 'partial':
                for n in range(len(message['data'])):
                    text = '{time}, {action}, {id}, {side}, {size}, {price} \n'.format(time=time, action=message['action'], 
                                                                                id=int(message['data'][n]['id']), 
                                                                                side=str(message['data'][n]['side']), 
                                                                                size=int(message['data'][n]['size']), 
                                                                                price=float(message['data'][n]['price']))
                    self.w.write(text)
                    #self.data = self.data.append(pd.DataFrame([list(message['data'][n].values())], index=[time]))
                    #self.mapper[message['data'][n]['id']] = message['data'][n]['price']

            if message['action'] == 'update':
                for n in range(len(message['data'])):
                    text = '{time}, {action}, {id}, {side}, {size}, \n'.format(time=time, action=message['action'], 
                                                                                id=int(message['data'][n]['id']), 
                                                                                side=str(message['data'][n]['side']), 
                                                                                size=int(message['data'][n]['size']))
                    self.w.write(text)
                    #price = self.mapper[message['data'][n]['id']]
                    #temp = pd.DataFrame([list(message['data'][n].values())], index=[time])
                    #temp['price'] = price
                    #self.data = self.data.append(temp)

            if message['action'] == 'insert':
                for n in range(len(message['data'])):
                    text = '{time}, {action}, {id}, {side}, {size}, {price} \n'.format(time=time, action=message['action'], 
                                                                                id=int(message['data'][n]['id']), 
                                                                                side=str(message['data'][n]['side']), 
                                                                                size=int(message['data'][n]['size']), 
                                                                                price=float(message['data'][n]['price']))
                    self.w.write(text)
                    #append(pd.DataFrame([list(message['data'][n].values())], index=[time]))
                    #self.mapper[message['data'][n]['id']] = message['data'][n]['price']

            if message['action'] == 'delete':
                for n in range(len(message['data'])):
                    text = '{time}, {action}, {id}, {side}, {size}, \n'.format(time=time, action=message['action'], 
                                                                                id=int(message['data'][n]['id']), 
                                                                                side=str(message['data'][n]['side']), 
                                                                                size=0)
                    self.w.write(text)
                    #price = self.mapper[message['data'][n]['id']]
                    ###temp = pd.DataFrame([list(message['data'][n].values())], index=[time])
                    ##temp['price'] = price
                    #self.data = self.data.append(temp)

        except:
            self.logger.error(traceback.format_exc())

        self.logger.debug("Subscribed to %s." % message)

    def _on_open(self):
        self.logger.debug("Websocket Opened.")

    def _on_close(self):
        self.logger.info('Websocket Closed')

    def _on_error(self, error):
        'Called on fatal websocket errors. We exit on these.'
        self.logger.error("Error : %s" % error)
"""
