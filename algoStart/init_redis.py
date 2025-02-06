# -*- coding: utf-8 -*-
"""
@Create on  2025/1/21 19:13
@file: init_redis.py
@author: Jerry
"""
from algoBroker.brokerMgr import BrokerMgr
from algoUtils.loggerUtil import generate_logger

logger = generate_logger()

redis_host = BrokerMgr.get_wsl_ip()  # u could set ur redis host here!
config_port = 6001  # config port
node_port = 9001  # node port

# download data from binance data server
symbols = 'doge_usdt'  # this could be a list of symbols like ['btc_usdt', 'eth_usdt']
start_dt = '2025-01-01'
end_dt = '2025-01-21'
BrokerMgr.download_trades(symbols, start_dt, end_dt)

# sync data from folder to redis
BrokerMgr.sync_redis(redis_host, config_port, node_port)
