# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 8:51
@File: signal_grid.py
@Author: Jingyuan
"""
import time
from algoBroker.brokerMgr import BrokerMgr
from algoUtils.dateUtil import local_datetime_timestamp

param = {
    '_signal_method_name': 'Grid',
    '_signal_method_param': {'_grid': 0.001},
    '_data_type': 'trade',
    '_lag': 0.1,
    '_start_timestamp': local_datetime_timestamp('2024-10-22 00:00:00'),
    '_end_timestamp': local_datetime_timestamp('2024-10-24 10:00:00'),
}

# BrokerMgr.start_signal_task(
#     **param,
#     _symbols='btc_usdt|binance_future',
#     _file_name='grid_signals'
# )

symbols = ['btc_usdt|binance_future', 'eth_usdt|binance_future']
tasks = [BrokerMgr.prepare_signal_task(_symbols=symbol, **param) for symbol in symbols]
BrokerMgr.submit_cluster_tasks('test', tasks, True)
