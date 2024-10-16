# -*- coding: utf-8 -*-
"""
@Create on  2024/10/2 18:28
@file: execute_taker.py
@author: Jerry
"""
import time

from algoBroker.brokerMgr import BrokerMgr, SignalType

BrokerMgr.start_execute_task(
    _execute_method='Taker',
    _execute_param={},
    _data_type='trade',
    _signal_type=SignalType.ISOLATED,
    _signal_file='ma_signals',
)
