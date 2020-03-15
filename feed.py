from datetime import datetime

from backtrader import date2num
from backtrader.feed import DataBase
from backtrader.utils.py3 import queue, with_metaclass

import store


class MetaData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        store.Store.DataCls = cls


class Data(with_metaclass(MetaData, DataBase)):
    params = (
        ('qcheck', 0.5),
    )

    _store = store.Store

    _ST_START, _ST_LIVE, _ST_OVER = range(3)

    def islive(self):
        return True

    def __init__(self, **kwargs):
        self.o = self._store(**kwargs)

    def setenvironment(self, env):
        super().setenvironment(env)
        env.addstore(self.o)

    def start(self):
        super().start()
        self.prevdt = 0.0
        self.qlive = queue.Queue()
        self._state = self._ST_OVER
        self.o.start(data=self)
        self._start_finish()
        self._state = self._ST_START
        self._st_start()

    def _st_start(self):
        self.qlive = self.o.streaming_prices(self.p.dataname)
        self._state = self._ST_LIVE
        return True

    def stop(self):
        super().stop()
        self.o.stop()

    def haslivedata(self):
        return bool(self.qlive)

    def _load(self):
        if self._state == self._ST_OVER:
            return False
        while True:
            if self._state == self._ST_LIVE:
                try:
                    msg = self.qlive.get(timeout=self._qcheck)
                except queue.Empty:
                    return None
                if self._laststatus != self.LIVE:
                    if self.qlive.qsize() <= 1:  # very short live queue
                        self.put_notification(self.LIVE)
                ret = self._load_tick(msg)
                if ret:
                    return True
                continue

    def _load_tick(self, msg):
        dtobj = datetime.utcfromtimestamp(msg['datetime'])
        dt = date2num(dtobj)
        if dt <= self.prevdt:
            return False
        self.prevdt = dt
        self.lines.datetime[0] = dt
        self.lines.open[0] = msg['open']
        self.lines.high[0] = msg['high']
        self.lines.low[0] = msg['low']
        self.lines.close[0] = msg['close']
        self.lines.volume[0] = msg['volume']
        self.lines.openinterest[0] = msg['openinterest']
        return True
