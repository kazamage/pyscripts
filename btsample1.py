import os

import backtrader as bt

import broker
import feed
import store


class TestStrategy(bt.Strategy):
    def __init__(self):
        super().__init__()
        self.count = 60
        self.f = None

    def start(self):
        datapath = os.path.join(os.path.dirname(__file__), 'ohlc.csv')
        self.f = open(datapath, 'w')
        self.f.write('Date,Time,Open,High,Low,Close,Volume,OpenInterest\n')

    def stop(self):
        if self.f is not None:
            self.f.close()

    def mkrecord(self, data):
        dt = data.datetime.datetime()
        return f'{dt.date()},{dt.time()},{data.open[0]:.2f},{data.high[0]:.2f},{data.low[0]:.2f},{data.close[0]:.2f},{data.volume[0]},{data.openinterest[0]}'

    def next(self):
        rec1 = self.mkrecord(self.datas[0])
        rec2 = self.mkrecord(self.datas[1])
        self.f.write(f'{rec1}\n')
        print(f'{self.datas[0]._name} {rec1}', flush=True)
        print(f'{self.datas[1]._name} {rec2}', flush=True)
        self.count -= 1
        if self.count == 0:
            self.env.runstop()


def runstrat():
    cerebro = bt.Cerebro()
    s = store.Store()
    b = s.getbroker()
    cerebro.setbroker(b)
    # timeframe = bt.TimeFrame.Seconds
    timeframe = bt.TimeFrame.Minutes
    d = s.getdata(dataname='USDJPY', timeframe=timeframe, compression=1, price=11040)
    cerebro.adddata(d)
    rekwargs = dict(
        name='RE5MIN',
        timeframe=timeframe,
        compression=5,
    )
    cerebro.resampledata(d, **rekwargs)
    cerebro.addstrategy(TestStrategy)
    # cerebro.run(exactbars=1)
    cerebro.run()


if __name__ == '__main__':
    runstrat()
