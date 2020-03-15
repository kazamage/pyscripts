import collections

from backtrader import (BrokerBase, BuyOrder, SellOrder)
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

import store


class MetaBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        store.Store.BrokerCls = cls


class Broker(with_metaclass(MetaBroker, BrokerBase)):
    def __init__(self, **kwargs):
        super().__init__()
        self.o = store.Store(**kwargs)
        self.orders = collections.OrderedDict()
        self.notifs = collections.deque()
        self.positions = collections.defaultdict(Position)
        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0

    def start(self):
        super().start()
        self.o.start(broker=self)

    def stop(self):
        super().stop()
        self.o.stop()

    def getcash(self):
        return self.cash

    def getvalue(self, datas=None):
        return self.value

    def getposition(self, data, clone=True):
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    def orderstatus(self, order):
        o = self.orders[order.ref]
        return o.status

    def buy(self, owner, data,
            size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0,
            trailamount=None, trailpercent=None,
            parent=None, transmit=True,
            **kwargs):
        order = BuyOrder(owner=owner, data=data,
                         size=size, price=price, pricelimit=plimit,
                         exectype=exectype, valid=valid, tradeid=tradeid,
                         trailamount=trailamount, trailpercent=trailpercent,
                         parent=parent, transmit=transmit)
        order.addinfo(**kwargs)
        order.addcomminfo(self.getcommissioninfo(data))
        oref = order.ref
        self.orders[oref] = order
        return order

    def sell(self, owner, data,
             size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0,
             trailamount=None, trailpercent=None,
             parent=None, transmit=True,
             **kwargs):
        order = SellOrder(owner=owner, data=data,
                          size=size, price=price, pricelimit=plimit,
                          exectype=exectype, valid=valid, tradeid=tradeid,
                          trailamount=trailamount, trailpercent=trailpercent,
                          parent=parent, transmit=transmit)
        order.addinfo(**kwargs)
        order.addcomminfo(self.getcommissioninfo(data))
        oref = order.ref
        self.orders[oref] = order
        return order

    def submit(self, order):
        return order

    def cancel(self, order):
        return order

    def notify(self, order):
        self.notifs.append(order.clone())

    def get_notification(self):
        if not self.notifs:
            return None
        return self.notifs.popleft()

    def next(self):
        self.notifs.append(None)
