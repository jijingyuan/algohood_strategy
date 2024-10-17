# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 15:25
@file: signal_ma.py
@author: Jerry
"""
import time

from algoBroker.brokerMgr import BrokerMgr

BrokerMgr.start_signal_task(
    _signal_method_name='MA',
    _signal_method_param={
        '_short_term': 5,
        '_long_term': 15
    },
    _data_type='trade',
    _symbols=[
        'btc_usdt|binance_future',
        # 'eth_usdt|binance_future',
    ],
    _lag=60,
    _start_timestamp=time.time() - 60 * 60 * 24 * 5,
    _end_timestamp=time.time(),
    _file_name='ma_signals',
)
