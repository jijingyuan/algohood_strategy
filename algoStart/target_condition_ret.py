# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 16:22
@file: target_condition_ret.py
@author: Jerry
"""
from algoBroker.brokerMgr import BrokerMgr

BrokerMgr.start_target_task(
    _target_method_name='ConditionRet',
    _target_method_param={},
    _data_type='trade',
    _forward_window=60 * 5,
    _signal_file='ma_signals',
    _file_name='ret_ma'
)
