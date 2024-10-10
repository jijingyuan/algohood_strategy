# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 14:06
@File: brokerMgr.py
@Author: Jingyuan
"""
import os
from enum import Enum

import pandas as pd

from algoExecution.algoEngine.eventMgr import EventMgr
from algoSignal.algoEngine.dataMgr import DataMgr
from algoSignal.algoEngine.signalMgr import SignalMgr
from algoSignal.algoEngine.targetMgr import TargetMgr
from algoUtils.loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class SignalType(Enum):
    SIMPLE = 1
    ISOLATED = 2
    CONSECUTIVE = 3


class BrokerMgr:

    @classmethod
    def start_signal_task(
            cls, _signal_type: SignalType, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag,
            _start_timestamp, _end_timestamp, _file_name
    ):
        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        data_mgr = DataMgr(_data_type)
        data_mgr.init_data_mgr()
        signal_mgr = SignalMgr(_signal_method_name, _signal_method_param, data_mgr)
        signals = signal_mgr.start_task(_lag, _symbols, _start_timestamp, _end_timestamp)
        if signals:
            pd.DataFrame(signals).to_csv('../algoFile/{}_{}.csv'.format(_signal_type.name.lower(), _file_name))

    @classmethod
    def start_target_task(
            cls, _target_method_name, _target_method_param, _data_type, _forward_window, _signal_file, _file_name
    ):
        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        path = '../algoFile/{}.csv'.format(_signal_file)
        signals = pd.read_csv(path).to_dict('records')
        if not signals:
            logger.error('empty file: {}'.format(_signal_file))
            return

        data_mgr = DataMgr(_data_type)
        data_mgr.init_data_mgr()
        target_mgr = TargetMgr(_target_method_name, _target_method_param, data_mgr)
        targets = target_mgr.start_task(signals, _forward_window)
        pd.DataFrame(targets).to_csv('../algoFile/{}.csv'.format(_file_name))

    @classmethod
    def start_execute_task(
            cls, _start_execute_method, _start_execute_param, _data_type, _signal_type: SignalType, _signal_file,
            _end_execute_method=None, _end_execute_param=None
    ):
        if _signal_type == SignalType.SIMPLE:
            assert _end_execute_method is not None

        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        path = '../algoFile/{}_{}.csv'.format(_signal_type.name.lower(), _signal_file)
        signals = pd.read_csv(path).to_dict('records')
        if not signals:
            logger.error('empty file: {}'.format(_signal_file))
            return

        data_mgr = DataMgr(_data_type)
        data_mgr.init_data_mgr()
        if _signal_type in [SignalType.SIMPLE, SignalType.ISOLATED]:
            for signal in signals:
                event_mgr = EventMgr(
                    _start_execute_method, _start_execute_param, _end_execute_method, _end_execute_param, data_mgr
                )

                event_mgr.start_task(signal)

        elif _signal_type == SignalType.CONSECUTIVE:
            event_mgr = EventMgr(
                _start_execute_method, _start_execute_param, _end_execute_method, _end_execute_param, data_mgr
            )
            for signal in signals:
                event_mgr.start_task(signal)
