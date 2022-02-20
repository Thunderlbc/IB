"""
主策略所在

结构如下：
    1. trade.py: 负责拿到报价信息、指标信息后决策是否下单
    2. ib_server.py: 实时维护最新报价信息及各项指标计算，供trade获取，并应trade请求操作订单
"""
from utils import logger
import requests