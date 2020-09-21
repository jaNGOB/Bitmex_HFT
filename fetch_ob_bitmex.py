import websocket
import threading
import traceback
import datetime
from time import sleep
import json
import csv

class BitmexBookData():
	def __init__(self):
		
		self.book = {}
		self.raw = {}
		
		self.L2 = {}
		self.L2['Buy'] = {}
		self.L2['Sell'] = {}
		
		self.counter = 0
		
		self._connect()

		
	def _connect(self):
		websocket.enableTrace(True)
		self.ws = websocket.WebSocketApp('wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD',
                                         on_message=self._on_message,
                                         on_close=self._on_close,
                                         on_open=self._on_open,
										 on_error=self._on_error)
		
		self.ws.run_forever()
		
	def _updateOrderbook(self, time):
		self.book[time] = self.L2
		self.book[time] = self.L2
		
		
	def _on_message(self, message):
		time = datetime.datetime.now()
		
		self.counter += 1
		
		if self.counter > 10000:
			print('### 500 datapoints recorded ###')
			self.save_data()
		
		message = json.loads(message)
		table, action = message.get('table'), message.get('action')
		
		if action:
			for item in message['data']:
				if action in ('partial', 'insert'):
					for item in message['data']:
						self.L2[item["side"]][item['price']] = item['size']          
						
				elif action == 'update':
					for item in message['data']:
						pr = self.get_price_from_ID(item['id'])
						self.L2[item["side"]][pr] = item['size']
						self.raw[time] = [action, item, pr]
						
				elif action == 'delete':  
					for item in message['data']:
						pr = self.get_price_from_ID(item['id'])
						del self.L2[item["side"]][pr]
						self.raw[time] = [action, item, pr]
						
		self._updateOrderbook(time)
		
				
	def get_price_from_ID(self, id):
		return 1000000 - (id % 100000000) / 100  
		
		
	def save_data(self):
		self.filename = 'Orderbook'+datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv')
		self.rawname = 'Rawdata'+datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv')
		
		with open(self.filename, 'w') as csv_file:  
			writer = csv.writer(csv_file)
			for key, value in self.book.items():
				writer.writerow([key, value])
		
		with open(self.rawname, 'w') as csv_file:  
			writer = csv.writer(csv_file)
			for key, value in self.raw.items():
				writer.writerow([key, value])
				
		self.ws.close()	
		
		
	def _on_open(self):
		print('### websocket connected ###')
		
		
	def _on_close(self):
		print('### websocket closed ###')
		
		
	def _on_error(self, error):
		print(error)

	
if __name__=="__main__":
	
	BitmexBookData()
