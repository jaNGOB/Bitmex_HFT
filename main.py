from bitmex_connect import BitmexBTCWebsocket
from dateutil.parser import parse
from signal import signal, SIGINT
from time import sleep
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

btc_ws = BitmexBTCWebsocket()


def handler(sig, frame):
	"""
	Handler function which allows for Ctrl+C stopping of the program. 
	This will send the exit command to the websocket and closes it.
	"""
	logger.info(" This is the end !")
	btc_ws.exit()
	exit(0)

def main():
	"""
	Main function which starts up the websocket and runs indefinitely until 
	stopped by Ctrl+C or Fatal Error.
	"""
	logger.info('Warming up the Engine')
	first = True
	while btc_ws.ws.sock.connected:
		if first:
			logger.info('Base values initiated, establishing connection to the exchange.')
			logger.info('Connection established!')
			logger.info('May the force be with us')
			first = False
			
if __name__ == '__main__':
	signal(SIGINT, handler)
	main()
