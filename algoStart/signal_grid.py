# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 8:51
@File: signal_grid.py
@Author: Jingyuan
"""
import time
from algoBroker.brokerMgr import BrokerMgr

BrokerMgr.start_signal_task(
    _signal_method_name='Grid',
    _signal_method_param={
        '_grid': 0.001
    },
    _data_type='trade',
    _symbols='btc_usdt|binance_future',
    _lag=0.1,
    _start_timestamp=time.time() - 60 * 60 * 24 * 2,
    _end_timestamp=time.time() - 60 * 60 * 1,
    _file_name='grid_signals'
)
