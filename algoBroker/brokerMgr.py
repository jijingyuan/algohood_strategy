# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 14:06
@File: brokerMgr.py
@Author: Jingyuan
"""
import os
import pandas as pd
from algoSignal.algoEngine.signalMgr import SignalMgr
from algoSignal.algoEngine.targetMgr import TargetMgr
from algoUtils.loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class BrokerMgr:

    @classmethod
    def start_signal_task(
            cls, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag, _start_timestamp,
            _end_timestamp, _file_name
    ):
        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        signal_mgr = SignalMgr(_signal_method_name, _signal_method_param, _data_type)
        signals = signal_mgr.start_task(_lag, _symbols, _start_timestamp, _end_timestamp)
        if signals:
            pd.DataFrame(signals).to_csv('../algoFile/{}.csv'.format(_file_name))

    @classmethod
    def start_target_task(
            cls, _target_method_name, _target_method_param, _data_type, _forward_window, _signal_file, _file_name
    ):
        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        path = '../algoFile/{}.csv'.format(_signal_file)
        signals = pd.read_csv(path).to_dict('records')
        if not signals:
            logger.error('empty file: {}'.format(_file_name))
            return

        target_mgr = TargetMgr(_target_method_name, _target_method_param, _data_type)
        targets = target_mgr.handle_signals(signals, _forward_window, _file_name)
        pd.DataFrame(targets).to_csv('../algoFile/{}.csv'.format(_file_name))
