import time
from tkinter import N
from requests_oauthlib import OAuth2Session
import requests
import numpy as np
from flask import Flask, request, redirect
from flask.json import jsonify
import os
from utils import logger

app = Flask(__name__)
__SESSION = {}

from utils import logger
import ib_insync as ibi
from queue import Queue

from indicators import Indicator_UTC_No1

class IBClient(object):
    def __init__(self, host, port, clientid, max_size):
        """
        负责与IB的交互
            1. 订阅行情数据
            2. 订单操作
        """
        logger.info("Start to initializing IBC with Host[{}] Port[{}] and Cid[{}] Msize[{}]".format(host,port,clientid, max_size))
        self._is_connected = False
        self._ib = ibi.IB()
        try:
            self._ib.connect(host, port=port, clientId=clientid)
            if self._ib.isConnected:
                self._is_connected = True
        except Exception as e:
            logger.exception("Error when trying to connect to IB: [{}]".format(str(e)))
        
        # 已经正常链接，可以开始正常干活儿了
        self._q = {
            'bsize': np.array([]),
            'bid': np.array([]),
            'asize': np.array([]),
            'ask': np.array([]),
            'high': np.array([]),
            'low': np.array([]),
            'close': np.array([])
        }
        self._events = []
        self._ind_threads = [Indicator_UTC_No1(name="UTCNo1", ibc=self)]
        self._indicators = {}
        self._contract = None

    def getQ(self):
        return self._q

    def unsubscribe(self):
        if self.checkConnected():
            self._ib.cancelMktData(self._contract)
            for evt in self._events:
                self._events.remove(evt)

    def checkConnected(self):
        return self._is_connected
    def subscribe(self, symbol, exchange, last_trade_date):
        """
        订阅对应exchange的symbol行情数据
        """
        if not self._is_connected:
            logger.warn("IBC Not Connected, Please Check")
            return False
        else:
            logger.info("Start to subscribe with Symbol[{}] and Exchange[{}] and LastTradeDate[{}]".format(symbol, exchange, last_trade_date))
            try:
                # TODO: 这里写死了期货，后续可改其他
                c = ibi.Contract(symbol=symbol, exchange=exchange, primaryExchange=exchange, lastTradeDateOrContractMonth=last_trade_date, secType="FUT")
                self._ib.qualifyContracts(c)
                self._ib.reqMktData(self._contract, '', False, False)
                def onPendingTickers(tickers):
                    for t in tickers:
                        if t.bidSize > 0 or t.askSize > 0:
                            # 有新的tick来了，直接放到queue里面
                            while len(self._q['bid']) >= self._max_size:
                                for xx in self._q:
                                    x = self._q[xx].pop()
                                    logger.info("Q[{}] Full, removing Top Tick[{}]".format(xx, x))
                            self._q['bsize'].append(t.bidSize)
                            self._q['bid'].append(t.bid)
                            self._q['asize'].append(t.askSize)
                            self._q['ask'].append(t.ask)
                            self._q['high'].append(t.high)
                            self._q['low'].append(t.low)
                            self._q['close'].append(t.close)

                self._ib.pendingTickersEvent += onPendingTickers
                self._events.append(onPendingTickers)
                self._contract = c
                return True
            except Exception as e:
                logger.exception("Error when Subscribe[{}], Please Check".format(str(e)))
                return False
    def get_lastest_ba(self):
        # bsize, bid, asize, ask, high, low, close
        res = []
        if len(self._q['bid'])> 0:
            keys = ['bsize','bid','asize','ask','high','low','close']
            for k in keys:
                res.append(self._q[k][-1])

        return res


    def set_indicator(self, name, value):
        logger.info("Setting Indicator[{}] with Value[{}]".format(name,value))
        self._indicators[name] = value
    def get_indicator(self, name):
        return self._indicators[name]
    
    def _start_all_indicators(self):
        for ind in self._ind_threads:
            ind.start()

    def start(self):
        self._start_all_indicators()

# 全局变量
__IBC = IBClient(host='127.0.0.1', port=4002, clientid=991, max_size=10000)

@app.route("/subscribe/<date>")
def subscribe_to_symbol(date):
    logger.info("Got Subscribe request for Date[{}]".format(date))
    __IBC.unsubscribe() # 退订其他
    return __IBC.subscribe("CL","NYMEX",date)

@app.route("/indicators/<name>", methods=['GET'])
def get_indicator(name):
    logger.info("Got Request for Indicator[{}]".format(name))
    return jsonify(__IBC.get_indicator(name))
    
@app.route("/latest_bar", methods=['GET'])
def get_latest_bar():
    logger.info("Got LatestBar Request")
    return jsonify(__IBC.get_lastest_ba())

if __name__ == "__main__":
    # This allows us to use a plain HTTP callback
    __IBC.start()
    app.run(host="0.0.0.0", port=25000)