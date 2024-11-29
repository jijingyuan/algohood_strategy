# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 10:04
@File: execute_needle_reverse.py
@Author: Jingyuan
"""
import asyncio
from algoBroker.brokerMgr import BrokerMgr, SignalType

loop = asyncio.get_event_loop()

file = BrokerMgr.get_abstract_given_file_name('1732845052275290_grids').to_dict('records')
if file:
    tasks = [
        BrokerMgr.prepare_execute_task(
            _execute_name='needle_{}'.format(direction),
            _execute_method_name='NeedleReverse',
            _execute_method_param={
                '_direction': direction,
                '_holding_spread': 0.0001,
                '_trigger_grid': 0.003,
                '_profit_grid': 0.002,
                '_stop_grid': 0.002,
                '_trigger_expire': None,
                '_stop_delay': 1
            },
            _data_type='trade'
        )
        for direction in ['long', 'short']
    ]

    coro = BrokerMgr.submit_execute_tasks(
        _task_name='exec_test',
        _tasks=tasks,
        _signal_ids=[v['result_id'] for v in file],
        _signal_type=SignalType.CONSECUTIVE
    )
    loop.run_until_complete(coro)
