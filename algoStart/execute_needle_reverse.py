# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 10:04
@File: execute_needle_reverse.py
@Author: Jingyuan
"""
import asyncio
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

loop = asyncio.get_event_loop()
file = BrokerMgr.get_abstract_given_file_name('1732496745283229_grids').to_dict('records')
if file:
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

    coro = BrokerMgr.submit_exec_tasks(
        _task_name='exec_test',
        _tasks=tasks,
        _signal_ids=[v['result_id'] for v in file],
        _signal_type=SignalType.CONSECUTIVE
    )
    loop.run_until_complete(coro)
