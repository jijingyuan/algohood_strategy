# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 14:06
@File: brokerMgr.py
@Author: Jingyuan
"""
import importlib
import inspect
import json
import os
import time
import uuid
from enum import Enum

import pandas as pd

from algoConfig.zmqConfig import host, port
from algoExecution.algoEngine.dataMgr import DataMgr as ExecDataMgr
from algoExecution.algoEngine.eventMgr import EventMgr
from algoSignal.algoEngine.dataMgr import DataMgr as signalDataMgr
from algoSignal.algoEngine.signalMgr import SignalMgr
from algoSignal.algoEngine.targetMgr import TargetMgr
from algoUtils.loggerUtil import generate_logger
from algoUtils.zmqUtil import ReqZmq

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

        data_mgr = signalDataMgr()
        data_mgr.init_data_mgr()
        data_mgr.set_data_type(_data_type)
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

        data_mgr = signalDataMgr()
        data_mgr.init_data_mgr()
        data_mgr.set_data_type(_data_type)
        target_mgr = TargetMgr(_target_method_name, _target_method_param, data_mgr)

        targets = []
        for signal in signals:
            target = target_mgr.start_task(signal, _forward_window)
            targets.append(target)

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

        data_mgr = ExecDataMgr()
        data_mgr.init_data_mgr()
        data_mgr.set_data_type(_data_type)
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
    def prepare_execute_task(cls, _exec_method_name, _exec_method_param, _data_type):
        return {
            '_execute_method': _exec_method_name,
            '_execute_param': _exec_method_param,
            '_data_type': _data_type,
        }

    @classmethod
    def submit_exec_tasks(cls, _task_name, _tasks, _signal_id, _signal_type: SignalType):
        zmq_client = ReqZmq(port, host)
        task_dict = {'task_type': 'exec', 'task': {
            'task_name': _task_name, 'signal_id': _signal_id, 'signal_type': _signal_type.name, 'info': _tasks
        }}
        task_id = zmq_client.send_msg(task_dict)
        logger.info('{} tasks submitted'.format(task_id))

        while True:
            try:
                task_left = zmq_client.send_msg({'task_type': 'check', 'task': task_id})
                if task_left is None:
                    continue

                elif not task_left:
                    logger.info('{} finished'.format(task_id))
                    break

                logger.info('{} left {}'.format(task_id, task_left))
                time.sleep(5)

            except Exception as e:
                logger.error(e)
                time.sleep(60)

    @classmethod
    def submit_target_tasks(
            cls, _task_name, _signal_id, _target_method_name, _target_method_param, _data_type, _forward_window,
            _update_codes=True
    ):
        zmq_client = ReqZmq(port, host)
        while True:
            try:
                task_left = zmq_client.send_msg({'task_type': 'check', 'task': _signal_id})
                if task_left is None:
                    continue

                elif not task_left:
                    logger.info('{} finished'.format(_signal_id))
                    break

                logger.info('{} left {}'.format(_signal_id, task_left))
                time.sleep(5)

            except Exception as e:
                logger.error(e)
                time.sleep(60)

        module_name = 'algoStrategy.target{}'.format(_target_method_name)
        module = importlib.import_module(module_name)
        script_content = inspect.getsource(module) if _update_codes else ''

        task_dict = {'task_type': 'target', 'task': {'type': 'code', 'info': {
            'module_name': _target_method_name, 'scripts': script_content
        }}}
        rsp = zmq_client.send_msg(task_dict)
        if rsp != 'finished':
            logger.error(rsp)
            return

        logger.info('strategy checked')
        # submit tasks
        task_dict = {'task_type': 'target', 'task': {
            'task_name': _task_name,
            'signal_id': _signal_id,
            'type': 'tasks',
            'info': {
                '_method_name': _target_method_name,
                '_method_param': _target_method_param,
                '_data_type': _data_type,
                '_forward_window': _forward_window
            }
        }}
        task_id = zmq_client.send_msg(task_dict)
        logger.info('{} tasks submitted'.format(task_id))
        cls.download_targets(task_id)

    @classmethod
    def download_targets(cls, _task_id):
        zmq_client = ReqZmq(port, host)
        while True:
            try:
                task_left = zmq_client.send_msg({'task_type': 'check', 'task': _task_id})
                if task_left is None:
                    continue

                elif not task_left:
                    logger.info('{} finished'.format(_task_id))
                    break

                logger.info('{} left {}'.format(_task_id, task_left))
                time.sleep(5)

            except Exception as e:
                logger.error(e)
                time.sleep(60)

        all_targets = []
        while True:
            try:
                rsp = zmq_client.send_msg({'task_type': 'download', 'task': _task_id})
                if isinstance(rsp, str):
                    logger.info(rsp)
                    break

                all_targets.extend(rsp)

            except Exception as e:
                logger.error(e)

        if all_targets:
            pd.DataFrame(all_targets).to_csv('../algoFile/cluster_targets_{}.csv'.format(_task_id))

    @classmethod
    def submit_signal_tasks(cls, _task_name, _tasks, _update_codes=True):
        # update codes 2 remote server
        zmq_client = ReqZmq(port, host)
        module_names = set([v['_signal_method_name'] for v in _tasks])

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

        logger.info('strategy checked')
        # submit tasks
        task_dict = {'task_type': 'signal', 'task': {'task_name': _task_name, 'type': 'tasks', 'info': _tasks}}
        task_id = zmq_client.send_msg(task_dict)
        logger.info('{} tasks submitted'.format(task_id))

        while True:
            try:
                task_left = zmq_client.send_msg({'task_type': 'check', 'task': task_id})
                if task_left is None:
                    continue

                elif not task_left:
                    logger.info('{} finished'.format(task_id))
                    break

                logger.info('{} left {}'.format(task_id, task_left))
                time.sleep(5)

            except Exception as e:
                logger.error(e)
                time.sleep(60)


if __name__ == '__main__':
    BrokerMgr.download_targets('1730094802752734_test')
