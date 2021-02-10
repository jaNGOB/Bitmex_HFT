from arctic import Arctic, TICK_STORE
import datetime as dt
import pytz as tz
import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


class DataBase(object):
    def __init__(self, no_db):
        """
        Initialize the object by creating empty lists, and defining a location to store the data.
        This can be changed but is naturally allocated to localhost. 

        :param no_db: Bool if there is already a database initiated or if one should be created.
        """

        self.logger = logging.getLogger(__name__)

        self.data = list()
        self.counter = 0
        self.batch_size = 10000
        
        self.key_mapper = {}

        self.store = Arctic('localhost')

        if no_db:
            self.create_db()
        
        self.connect_db()

    def create_db(self):
        """
        Create a new database library if no_db = True.
        """
        self.store.initialize_library('Tick_store', lib_type=TICK_STORE)
        self.logger.info('New db created.')

    def connect_db(self):
        """
        Connect to a existing library called Tick_store by default.
        """
        self.library = self.store['Tick_store']
        self.logger.info('Connection to db established.')

    def new_tick(self, tick):
        """
        Process incoming ticks from Bitmex.
        Save them into a list called "data" and if the batch size is reached,
        write them to the arctic library.

        :param tick: incoming tick from Bitmex which can contain multiple trades/changes at once.
        """
        action = tick['action']
        sub_ticks = tick['data']
        timestamp = dt.datetime.now(tz=tz.utc)
        self.counter += 1
        #self.logger.info(self.key_mapper)

        if action == 'partial' or action == 'insert':
            if len(sub_ticks) > 1:
                for n in range(len(sub_ticks)):
                    temp = sub_ticks[n]
                    temp['index'] = timestamp
                    self.data.append(temp)
                    self.key_mapper[temp['id']] = temp['price']
            else:
                temp = sub_ticks[0]
                temp['index'] = timestamp
                self.data.append(temp)
                self.key_mapper[temp['id']] = temp['price']
        else:
            if len(sub_ticks) > 1:
                for n in range(len(sub_ticks)):
                    temp = sub_ticks[n]
                    temp['index'] = timestamp
                    temp['price'] = self.key_mapper[temp['id']]
                    self.data.append(temp)
            else:
                temp = sub_ticks[0]
                temp['index'] = timestamp
                temp['price'] = self.key_mapper[temp['id']]
                self.data.append(temp)
                self.key_mapper[temp['id']] = temp['price']
            
        if self.counter % self.batch_size == 0:
            self.logger.info('{} Ticks Stored'.format(self.counter))
            self.library.write('BTCUSD', self.data)
            self.data.clear()
