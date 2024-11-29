# -*- coding: utf-8 -*-
"""
@Create: 2024/11/29 10:33
@File: signal_fast_trend.py
@Author: Jingyuan
"""
import asyncio

from algoBroker.brokerMgr import BrokerMgr
from algoUtils.dateUtil import local_datetime_timestamp

loop = asyncio.get_event_loop()

symbols = ['btc_usdt|binance_future', 'eth_usdt|binance_future']
tasks = [
    BrokerMgr.prepare_signal_task(
        _signal_name='fast_trend_{}'.format(symbol).replace('|', '_'),
        _signal_method_name='FastTrend',
        _signal_method_param={'_time_th': 1},
        _data_type='trade',
        _symbols=symbol,
        _lag=0.1,
        _start_timestamp=local_datetime_timestamp('2024-10-24 00:00:00'),
        _end_timestamp=local_datetime_timestamp('2024-10-30 01:00:00'),
    )
    for symbol in symbols
]
coro = BrokerMgr.submit_signal_tasks(
    _task_name='fast_trend',
    _tasks=tasks,
    _update_codes=False,
    _use_cluster=False
)

loop.run_until_complete(coro)
