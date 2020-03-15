import json
import random
import time
from datetime import datetime

import pandas as pd
import redis

varrng = 5
actrng2flg = 1
rng2prob = 100
varrng2 = 10
aggnum = 10


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


def main():
    cprice = 10511
    conn = redis.Redis(host='localhost', port=6379, db=0)
    while True:
        aftprice = [cprice]
        i = 0
        for i in range(1, aggnum):
            aftprice.append(calc_random_price(cprice, varrng, actrng2flg, rng2prob, varrng2))
            cprice = aftprice[-1]
        ohlc = create_ohlc_data(aggnum, aftprice)
        dt = datetime.utcnow().replace(microsecond=0)
        data = {'datetime': dt.timestamp(),
                'open': ohlc.loc[1, 'open'] / 100,
                'high': ohlc.loc[1, 'high'] / 100,
                'low': ohlc.loc[1, 'low'] / 100,
                'close': ohlc.loc[1, 'close'] / 100,
                'volume': 0.0,
                'openinterest': 0.0}
        conn.publish("USDJPY", json.dumps(data))
        time.sleep(1)


if __name__ == '__main__':
    main()
