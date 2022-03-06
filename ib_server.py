import time
import requests
import numpy as np
from flask import Flask, request, redirect
from flask.json import jsonify
import datetime
import pandas as pd
import os
from utils.trade_common import logger
from utils.influxdb_utils import write_dataframe

from sanic import Sanic
from sanic.response import text,json
import asyncio
import threading, subprocess

app = Sanic(__name__)
from server import init_server

import ib_insync as ibi
from indicators import Indicator_UTC_No1

class IBClient(object):
    def __init__(self, host, port, clientid, max_size):
        """
        负责与IB的交互
            1. 订阅行情数据
            2. 订单操作
        """
        logger.info("Start to initializing IBC with Host[{}] Port[{}] and Cid[{}] Msize[{}]".format(host,port,clientid, max_size))
        self._max_size = max_size
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
            'close': np.array([]),
            'ts': np.array([])
        }
        self._events = []
        self._ind_threads = [Indicator_UTC_No1(name="UTCNo1", ibc=self)]
        self._indicators = {}
        self._contract = None
        for sthr in self._ind_threads:
            sthr.start()

    # 用于内部调用
    def getQ(self):
        return self._q
    def set_indicator(self, name, value):
        #logger.info("Setting Indicator[{}] with Value[{}]".format(name,value))
        self._indicators[name] = value

    """
    行情相关接口
    """
    async def checkConnected(self):
        return '000', self._is_connected, "OK"
    async def unsubscribe(self):
        if self.checkConnected():
            self._ib.cancelMktData(self._contract)
            for evt in self._events:
                self._events.remove(evt)
            return '000', None, "OK"
        else:
            return '100', None, "Not Connected, Please check"
    async def subscribe(self, symbol, exchange, last_trade_date):
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
                await self._ib.qualifyContractsAsync(c)
                self._ib.reqMktData(c, '', False, False)
                def onPendingTickers(tickers):
                    for t in tickers:
                        if t.bidSize > 0 or t.askSize > 0:
                            # 有新的tick来了，直接放到queue里面
                            while len(self._q['bid']) >= self._max_size:
                                for xx in self._q:
                                    x = self._q[xx][0]
                                    logger.info("Q[{}] Full, removing Top Tick[{}]".format(xx, x))
                                    self._q[xx] = self._q[xx][1:]
                            self._q['bsize'] = np.append(self._q['bsize'], t.bidSize)
                            self._q['bid'] = np.append(self._q['bid'], t.bid)
                            self._q['asize'] = np.append(self._q['asize'], t.askSize)
                            self._q['ask'] = np.append(self._q['ask'], t.ask)
                            self._q['high'] = np.append(self._q['high'], t.high)
                            self._q['low'] = np.append(self._q['low'], t.low)
                            self._q['close'] = np.append(self._q['close'], t.close)

                            # write tick data to influx for displaying grafana
                            df = pd.DataFrame({
                                'ts': [t.time - datetime.timedelta(hours=8)],
                                'bsize': t.bidSize,
                                'asize': t.askSize,
                                'bid': t.bid,
                                'ask': t.ask,
                                'high': t.high,
                                'low': t.low,
                                'close': t.close,
                                'ts': t.time
                            })
                            write_dataframe(data=df, measurement="IB_CL_TICK")
                    #logger.info("Append[{}] with Length[{}]".format(t, len(self._q['bsize'])))
                self._ib.pendingTickersEvent += onPendingTickers
                self._events.append(onPendingTickers)
                self._contract = c
                return "000", "Success", "OK"
            except Exception as e:
                logger.exception("Error when Subscribe[{}], Please Check".format(str(e)))
                return "101", None, "Failed"
    async def get_latest_bar(self):
        # bsize, bid, asize, ask, high, low, close, ts
        res = []
        if self._is_connected:
            if len(self._q['bid'])> 0:
                keys = ['bsize','bid','asize','ask','high','low','close', 'ts']
                for k in keys:
                    res.append(self._q[k][-1])
            return '000', res, "OK"
        else:
            return "100", None, "Not Connected, Please check"
    async def get_indicator(self, name):
        return '000', self._indicators[name], "OK"

    """
    交易相关接口
    """
    async def place_order(self, price, amt, direction, order_type):
        """
        返回的是一个orderId, int型，
        """
        logger.info("Placing Order with Price[{}] Amt[{}] Direction[{}] and OrderType[{}]".format(price, amt, direction, order_type))
        if order_type == 'limit':
            order = ibi.LimitOrder(direction, totalQuantity=amt, lmtPrice=price)
        else:
            order = ibi.MarketOrder(action=direction, totalQuantity=amt)
        cur_trade = self._ib.placeOrder(contract=self._contract, order=order)
        self._order_mappings[cur_trade.order.orderId] = cur_trade
        return cur_trade.order.orderId

    async def cancel_order(self, order_id):
        cur_trade = self._order_mappings[order_id]
        if cur_trade is not None:
            self._ib.cancelOrder(cur_trade.order)
    async def check_order(self, order_id):
        cur_trade = self._order_mappings[order_id]
    
if __name__ == "__main__":
    # This allows us to use a plain HTTP callback
    init_server(IBClient(host='127.0.0.1',port=4002, clientid=992, max_size=1000))
