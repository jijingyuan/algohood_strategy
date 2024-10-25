# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 14:06
@File: brokerMgr.py
@Author: Jingyuan
"""
import os
from enum import Enum

import pandas as pd
import importlib
import inspect
import gzip
import uuid

from algoExecution.algoEngine.eventMgr import EventMgr
from algoUtils.zmqUtil import ReqZmq
from algoConfig.zmqConfig import host, port
from algoSignal.algoEngine.dataMgr import DataMgr as signalDataMgr
from algoExecution.algoEngine.dataMgr import DataMgr as ExecDataMgr
from algoSignal.algoEngine.signalMgr import SignalMgr
from algoSignal.algoEngine.targetMgr import TargetMgr
from algoUtils.loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class SignalType(Enum):
    ISOLATED = 1
    CONSECUTIVE = 2


class BrokerMgr:

    @classmethod
    def start_signal_task(
            cls, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag, _start_timestamp,
            _end_timestamp, _file_name
    ):
        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        data_mgr = signalDataMgr(_data_type)
        data_mgr.init_data_mgr()
        signal_mgr = SignalMgr(_signal_method_name, _signal_method_param, data_mgr)
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
            logger.error('empty file: {}'.format(_signal_file))
            return

        data_mgr = signalDataMgr(_data_type)
        data_mgr.init_data_mgr()
        target_mgr = TargetMgr(_target_method_name, _target_method_param, data_mgr)
        targets = target_mgr.start_task(signals, _forward_window)
        pd.DataFrame(targets).to_csv('../algoFile/{}.csv'.format(_file_name))

    @classmethod
    def start_execute_task(
            cls, _execute_method, _execute_param, _data_type, _signal_type: SignalType, _signal_file,
    ):

        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        path = '../algoFile/{}.csv'.format(_signal_file)
        signals = pd.read_csv(path).to_dict('records')
        if not signals:
            logger.error('empty file: {}'.format(_signal_file))
            return

        data_mgr = ExecDataMgr(_data_type)
        data_mgr.init_data_mgr()
        orders = []
        event_mgr = EventMgr(_execute_method, _execute_param, data_mgr, 'local')
        if _signal_type == SignalType.ISOLATED:
            for signal in signals:
                event_mgr.load_signals([signal])
                orders.extend(event_mgr.start_task())

        elif _signal_type == SignalType.CONSECUTIVE:
            event_mgr.load_signals(signals)
            orders.extend(event_mgr.start_task())

        if orders:
            pd.DataFrame(orders).to_csv('../algoFile/orders_{}.csv'.format(_signal_file))

    @classmethod
    def prepare_signal_task(
            cls, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag, _start_timestamp,
            _end_timestamp
    ):
        return {
            '_signal_method_name': _signal_method_name,
            '_signal_method_param': _signal_method_param,
            '_symbols': _symbols,
            '_data_type': _data_type,
            '_lag': _lag,
            '_start_timestamp': _start_timestamp,
            '_end_timestamp': _end_timestamp,
        }

    @classmethod
    def prepare_target_task(cls):
        pass

    @classmethod
    def prepare_execute_task(cls):
        pass

    @classmethod
    def submit_cluster_tasks(cls, _tasks, _update_codes=True):
        # update codes 2 remote server
        zmq_client = ReqZmq(port, host)
        module_names = set([v['_signal_method_name'] for v in _tasks])

        task_id = str(uuid.uuid4())
        for name in module_names:
            module_name = 'algoStrategy.signal{}'.format(name)
            module = importlib.import_module(module_name)
            script_content = inspect.getsource(module) if _update_codes else ''

            task_dict = {'task_type': 'signal', 'task': {'type': 'code', 'info': {
                'module_name': name, 'scripts': script_content
            }}}
            rsp = zmq_client.send_msg(task_dict)
            if rsp == 'finished':
                continue

            logger.error(rsp)
            return

        # submit tasks
        task_dict = {'task_type': 'signal', 'task': {'task_id': task_id, 'type': 'tasks', 'info': _tasks}}
        rsp = zmq_client.send_msg(task_dict)
        # waiting for results
