# -*- coding: utf-8 -*-
"""
@Create on  2024/10/18 8:28
@file: execute_grid.py
@author: Jerry
"""
from algoBroker.brokerMgr import BrokerMgr, SignalType

BrokerMgr.start_execute_task(
    _execute_method='NeedleReverse',
    _execute_param={
        '_direction': 'short',
        '_holding_spread': 0.001,
        '_trigger_grid': 0.003,
        '_profit_grid': 0.002,
        '_stop_grid': 0.002,
        '_trigger_expire': None,
        '_stop_delay': 1
    },
    _data_type='trade',
    _signal_type=SignalType.CONSECUTIVE,
    _signal_file='grids'
)
