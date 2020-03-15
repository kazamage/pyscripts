import os

import backtrader as bt
import backtrader.feeds as btfeeds


class TestStrategy(bt.Strategy):
    def mkrecord(self, data):
        dt = data.datetime.datetime()
        return f'{dt.date()},{dt.time()},{data.open[0]:.2f},{data.high[0]:.2f},{data.low[0]:.2f},{data.close[0]:.2f},{data.volume[0]},{data.openinterest[0]}'

    def next(self):
        rec1 = self.mkrecord(self.datas[0])
        rec2 = self.mkrecord(self.datas[1])
        print(f'{self.datas[0]._name} {rec1}', flush=True)
        print(f'{self.datas[1]._name} {rec2}', flush=True)


def runstrat():
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TestStrategy)
    datapath = os.path.join(os.path.dirname(__file__), 'ohlc.csv')
    data = btfeeds.BacktraderCSVData(dataname=datapath, name='USDJPY')
    cerebro.adddata(data)
    cerebro.resampledata(data, name='RE5MIN', timeframe=bt.TimeFrame.Seconds, compression=5)
    cerebro.run()


if __name__ == '__main__':
    runstrat()
