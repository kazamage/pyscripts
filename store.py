import collections
import json
import threading

import redis
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass


class MetaSingleton(MetaParams):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super().__call__(*args, **kwargs))
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

    def start(self, data=None, broker=None):
        if data is None and broker is None:
            return
        if data is not None:
            self._env = data._env
            self.datas.append(data)
        elif broker is not None:
            self.broker = broker

    def stop(self):
        pass

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
