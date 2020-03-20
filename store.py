import collections
import random
import threading
import time
from datetime import datetime, timedelta

import backtrader as bt
import pandas as pd
import pytz
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass


def calc_random_price(cp, rng, rng2flg, rng2p, rng2):
    while True:
        rng2judg = random.randint(0, rng2p - 1)
        rndnum = random.randint(0, 1)
        if rng2flg == 1 and rng2judg == 0:
            rng = rng2
        rndwdt = random.randint(0, rng)
        if rndnum == 0:
            varnum = cp - rndwdt
        elif rndnum == 1:
            varnum = cp + rndwdt
        if varnum > 0:
            break
    return varnum


def create_ohlc_data(an, ap):
    tempdf = pd.DataFrame(columns=['open', 'high', 'low', 'close'])
    cnt = 1
    while True:
        if len(ap) == 0:
            break
        wlst = ap[0:an]
        del ap[0:an]
        oprc = wlst[0]
        hprc = max(wlst)
        lprc = min(wlst)
        cprc = wlst[-1]
        tempdf = tempdf.append(
            pd.DataFrame({'open': [oprc], 'high': [hprc], 'low': [lprc], 'close': [cprc]}, index=[cnt]))
        cnt = cnt + 1
    tempdf = tempdf[['open', 'high', 'low', 'close']]
    return tempdf


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

    def streaming_prices(self, dataname, timeframe, compression, price):
        q = queue.Queue()
        kwargs = {'q': q,
                  'dataname': dataname,
                  'timeframe': timeframe,
                  'compression': compression,
                  'price': price}
        t = threading.Thread(target=self._t_streaming_prices, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_streaming_prices(self, q, dataname, timeframe, compression, price):
        varrng = 5
        actrng2flg = 1
        rng2prob = 100
        varrng2 = 10
        aggnum = 10
        cprice = price
        if timeframe == bt.TimeFrame.Seconds:
            tkwargs = {'seconds': compression}
            rkwargs = {'microsecond': 0}
        elif timeframe == bt.TimeFrame.Minutes:
            tkwargs = {'minutes': compression}
            rkwargs = {'second': 0, 'microsecond': 0}
        else:
            tkwargs = rkwargs = {}
        if tkwargs:
            dt = datetime.now(pytz.utc)
            dt = dt.replace(**rkwargs)
        while True:
            aftprice = [cprice]
            i = 0
            for i in range(1, aggnum):
                aftprice.append(calc_random_price(cprice, varrng, actrng2flg, rng2prob, varrng2))
                cprice = aftprice[-1]
            ohlc = create_ohlc_data(aggnum, aftprice)
            if tkwargs:
                dt += timedelta(**tkwargs)
                dt = dt.replace(**rkwargs)
                epoch = float(int(dt.timestamp()))
            else:
                epoch = float(int(time.time()))
            data = {'datetime': epoch,
                    'open': ohlc.loc[1, 'open'] / 100,
                    'high': ohlc.loc[1, 'high'] / 100,
                    'low': ohlc.loc[1, 'low'] / 100,
                    'close': ohlc.loc[1, 'close'] / 100,
                    'volume': 0.0,
                    'openinterest': 0.0}
            q.put(data)
            time.sleep(1)
