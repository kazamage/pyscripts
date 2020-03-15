import backtrader as bt
import feed
import broker
import store


class TestStrategy(bt.Strategy):
    def next(self):
        for d in self.datas:
            print(dir(d))
            for l in d.lines:
            #     # print(dir(l.lines))
                print(dir(l))


def runstrategy():
    cerebro = bt.Cerebro()
    s = store.Store()
    b = s.getbroker()
    cerebro.setbroker(b)
    d = s.getdata(dataname='USDJPY')
    cerebro.adddata(d)
    # rekwargs = dict(
    #     timeframe=bt.TimeFrame.Seconds,
    #     compression=5,
    #     bar2edge=False,
    #     adjbartime=False,
    #     rightedge=False,
    #     takelate=False,
    # )
    # cerebro.resampledata(d, **rekwargs)
    cerebro.addstrategy(TestStrategy)
    cerebro.run(exactbars=1)


if __name__ == '__main__':
    runstrategy()
