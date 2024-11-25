# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 16:22
@file: target_condition_ret.py
@author: Jerry
"""
import asyncio
from algoBroker.brokerMgr import BrokerMgr

loop = asyncio.get_event_loop()

# BrokerMgr.start_target_task(
#     _target_method_name='ConditionRet',
#     _target_method_param={},
#     _data_type='trade',
#     _forward_window=60 * 5,
#     _signal_file='ma_signals',
#     _file_name='ret_ma'
# )

file = BrokerMgr.get_abstract_given_file_name('1732412132041939_grids').to_dict('records')
if file is not None:
    signal_ids = [v['result_id'] for v in file]
    coro = BrokerMgr.submit_target_tasks(
        _task_name='test',
        _signal_ids=signal_ids,
        _target_method_name='ConditionRet',
        _target_method_param={},
        _data_type='trade',
        _forward_window=60 * 5,
        _update_codes=True
    )
    loop.run_until_complete(coro)
