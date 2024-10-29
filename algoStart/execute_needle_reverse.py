# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 10:04
@File: execute_needle_reverse.py
@Author: Jingyuan
"""
from algoBroker.brokerMgr import BrokerMgr, SignalType

# BrokerMgr.start_execute_task(
#     _execute_method='NeedleReverse',
#     _execute_param={
#         '_direction': 'long',
#         '_holding_spread': 0.0001,
#         '_trigger_grid': 0.003,
#         '_profit_grid': 0.002,
#         '_stop_grid': 0.002,
#         '_trigger_expire': None,
#         '_stop_delay': 1
#     },
#     _data_type='trade',
#     _signal_type=SignalType.CONSECUTIVE,
#     _signal_file='grid_signals',
# )

tasks = [
    BrokerMgr.prepare_execute_task(
        _exec_method_name='NeedleReverse',
        _exec_method_param={
            '_direction': 'long',
            '_holding_spread': 0.0001,
            '_trigger_grid': 0.003,
            '_profit_grid': 0.002,
            '_stop_grid': 0.002,
            '_trigger_expire': None,
            '_stop_delay': 1
        },
        _data_type='trade'
    )
]

BrokerMgr.submit_exec_tasks('exec_test', tasks, '1730167713735313_grids', SignalType.CONSECUTIVE)
