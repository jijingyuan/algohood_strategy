# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 8:51
@File: signal_grid.py
@Author: Jingyuan
"""
import asyncio

from algoBroker.brokerMgr import BrokerMgr
from algoUtils.dateUtil import local_datetime_timestamp

loop = asyncio.get_event_loop()

symbols = ['btc_usdt|binance_future', 'eth_usdt|binance_future']
tasks = [
    BrokerMgr.prepare_signal_task(
        _signal_name='grid_{}'.format(symbol).replace('|', '_'),
        _signal_method_name='Grid',
        _signal_method_param={'_grid': 0.001},
        _data_type='trade',
        _symbols=symbol,
        _lag=0.1,
        _start_timestamp=local_datetime_timestamp('2024-10-24 00:00:00'),
        _end_timestamp=local_datetime_timestamp('2024-10-30 01:00:00'),
    )
    for symbol in symbols
]
coro = BrokerMgr.submit_signal_tasks(
    _task_name='grids',
    _tasks=tasks,
    _update_codes=False,
    _use_cluster=True
)

loop.run_until_complete(coro)
