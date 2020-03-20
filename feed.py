from datetime import datetime

import backtrader as bt
from backtrader.utils.py3 import queue, with_metaclass

import store


def _next(self, datamaster=None, ticks=True):
    if not self.is_data0() and self.runtime_datas:
        data0 = self.runtime_datas[0]
        if hasattr(data0, 'last_return') and data0.last_return is None:
            return None

    if len(self) >= self.buflen():
        if ticks:
            self._tick_nullify()

        # not preloaded - request next bar
        ret = self.load()
        if not ret:
            # if load cannot produce bars - forward the result
            return ret

        if datamaster is None:
            # bar is there and no master ... return load's result
            if ticks:
                self._tick_fill()
            return ret
    else:
        self.advance(ticks=ticks)

    # a bar is "loaded" or was preloaded - index has been moved to it
    if datamaster is not None:
        # there is a time reference to check against
        if self.lines.datetime[0] > datamaster.lines.datetime[0]:
            # can't deliver new bar, too early, go back
            self.rewind()

            if len(datamaster) == 0:
                return False
        else:
            if ticks:
                self._tick_fill()

    else:
        if ticks:
            self._tick_fill()

    # tell the world there is a bar (either the new or the previous
    return True


class DataClone(bt.DataClone):
    def start(self):
        super().start()
        datas = self.getenvironment().datas
        self.runtime_datas = sorted(datas, key=lambda x: (x._timeframe, x._compression))

    def is_data0(self):
        return self.runtime_datas and self.runtime_datas[0] is self

    def next(self, datamaster=None, ticks=True):
        return _next(self, datamaster=datamaster, ticks=ticks)

    def _check(self, forcedata=None):
        if not self.is_data0() and self.runtime_datas:
            data0 = self.runtime_datas[0]
            if hasattr(data0, 'last_return') and data0.last_return is None:
                return
        super()._check(forcedata=forcedata)


class DataBase(bt.DataBase):
    def islive(self):
        return True

    def start(self):
        super().start()
        datas = self.getenvironment().datas
        self.runtime_datas = sorted(datas, key=lambda x: (x.p.timeframe, x.p.compression))

    def is_data0(self):
        return self.runtime_datas and self.runtime_datas[0] is self

    def next(self, datamaster=None, ticks=True):
        return _next(self, datamaster=datamaster, ticks=ticks)

    def clone(self, **kwargs):
        return DataClone(dataname=self, **kwargs)

    def copyas(self, _dataname, **kwargs):
        d = DataClone(dataname=self, **kwargs)
        d._dataname = _dataname
        d._name = _dataname
        return d


class MetaData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        store.Store.DataCls = cls


class Data(with_metaclass(MetaData, DataBase)):
    params = (
        ('qcheck', 0.5),
        ('price', 110.0),
    )

    _store = store.Store

    _ST_START, _ST_LIVE, _ST_OVER = range(3)

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
        self.o = self._store(**kwargs)
        self.qlive = None
        self._state = None
        self.last_return = None
        self.prev_dt = float('nan')
        self.prev_msg = None

    def setenvironment(self, env):
        super().setenvironment(env)
        env.addstore(self.o)

    def start(self):
        super().start()
        self._state = self._ST_OVER
        self.o.start(data=self)
        self._start_finish()
        self._state = self._ST_START
        self._st_start()

    def _st_start(self):
        self.qlive = self.o.streaming_prices(self.p.dataname,
                                             self.p.timeframe,
                                             self.p.compression,
                                             self.p.price)
        self._state = self._ST_LIVE
        return True

    def stop(self):
        super().stop()
        self.o.stop()

    def haslivedata(self):
        return bool(self.qlive)

    def _load(self):
        if self._state == self._ST_OVER:
            self.last_return = False
            return self.last_return
        is_data0 = self.is_data0()
        while True:
            if self._state == self._ST_LIVE:
                try:
                    msg = self.qlive.get(timeout=self._qcheck)
                    # msg = self.qlive.get()
                except queue.Empty:
                    msg = None
                if not msg:
                    if is_data0 or not self.prev_msg:
                        self.last_return = None
                        return self.last_return
                    msg = self.prev_msg
                if self._laststatus != self.LIVE:
                    if self.qlive.qsize() <= 1:  # very short live queue
                        self.put_notification(self.LIVE)
                result = self._load_tick(msg)
                if result:
                    self.last_return = True
                    return self.last_return

    def _load_tick(self, msg):
        dtobj = datetime.utcfromtimestamp(msg['datetime'])
        dt = bt.date2num(dtobj)
        if dt <= self.prev_dt:
            return False
        self.lines.datetime[0] = dt
        self.lines.open[0] = msg['open']
        self.lines.high[0] = msg['high']
        self.lines.low[0] = msg['low']
        self.lines.close[0] = msg['close']
        self.lines.volume[0] = msg['volume']
        self.lines.openinterest[0] = msg['openinterest']
        self.prev_dt = dt
        self.prev_msg = msg
        return True
