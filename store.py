import collections
import json
import threading

import redis

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class Store(with_metaclass(MetaSingleton, object)):
    BrokerCls = None
    DataCls = None

    @classmethod
    def getdata(cls, *args, **kwargs):
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super().__init__()
        self.notifs = collections.deque()  # store notifications for cerebro
        self._env = None
        self.broker = None
        self.datas = list()
        self._cash = 0.0
        self._value = 0.0

    def start(self, data=None, broker=None):
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker

    def stop(self):
        pass

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def streaming_prices(self, dataname):
        q = queue.Queue()
        kwargs = {'q': q, 'dataname': dataname}
        t = threading.Thread(target=self._t_streaming_prices, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_streaming_prices(self, dataname, q):
        conn = redis.Redis(host='localhost', port=6379, db=0)
        pubsub = conn.pubsub()
        pubsub.subscribe([dataname])
        for data in pubsub.listen():
            if data['type'] == 'message':
                q.put(json.loads(data['data'].decode('utf-8')))

    def get_cash(self):
        return self._cash

    def get_value(self):
        return self._value

    def order_create(self, order, stopside=None, takeside=None, **kwargs):
        return order

    def order_cancel(self, order):
        return order

    def _transaction(self, trans):
        pass
