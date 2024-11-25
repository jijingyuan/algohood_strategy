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
import gzip
from enum import Enum

import pandas as pd

from algoConfig.zmqConfig import host, port
from algoExecution.algoEngine.dataMgr import DataMgr as ExecDataMgr
from algoExecution.algoEngine.eventMgr import EventMgr
from algoSignal.algoEngine.dataMgr import DataMgr as signalDataMgr
from algoSignal.algoEngine.signalMgr import SignalMgr
from algoSignal.algoEngine.targetMgr import TargetMgr
from algoUtils.asyncZmqUtil import AsyncReqZmq
from algoUtils.loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class SignalType(Enum):
    ISOLATED = 1
    CONSECUTIVE = 2


class BrokerMgr:

    @staticmethod
    def get_abstract_given_file_name(_file_name):
        try:
            file = pd.read_csv('../algoFile/abstract_{}.csv'.format(_file_name))
            return file
        except Exception as e:
            logger.error(e)

    @classmethod
    async def start_signal_task(
            cls, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag, _start_timestamp,
            _end_timestamp, _file_name
    ):
        if not os.path.exists('../algoFile'):
            os.mkdir('../algoFile')

        data_mgr = signalDataMgr()
        await data_mgr.init_data_mgr()
        data_mgr.set_data_type(_data_type)
        signal_mgr = SignalMgr(_signal_method_name, _signal_method_param, data_mgr)
        signals = await signal_mgr.start_task(_lag, _symbols, _start_timestamp, _end_timestamp)
        if signals:
            pd.DataFrame(signals).to_csv('../algoFile/{}.csv'.format(_file_name))

    @classmethod
    async def start_target_task(
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
        await data_mgr.init_data_mgr()
        data_mgr.set_data_type(_data_type)
        target_mgr = TargetMgr(_target_method_name, _target_method_param, data_mgr)

        targets = []
        for signal in signals:
            target = await target_mgr.start_task(signal, _forward_window)
            targets.append(target)

        pd.DataFrame(targets).to_csv('../algoFile/{}.csv'.format(_file_name))

    @classmethod
    async def start_execute_task(
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
        await data_mgr.init_data_mgr()
        data_mgr.set_data_type(_data_type)
        orders = []
        event_mgr = EventMgr(_execute_method, _execute_param, data_mgr, 'local')
        if _signal_type == SignalType.ISOLATED:
            for signal in signals:
                await event_mgr.load_signals([signal])
                orders.extend(await event_mgr.start_task())

        elif _signal_type == SignalType.CONSECUTIVE:
            await event_mgr.load_signals(signals)
            orders.extend(await event_mgr.start_task())

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
    async def submit_exec_tasks(cls, _task_name, _tasks, _signal_ids, _signal_type: SignalType):
        zmq_client = AsyncReqZmq(port, host)
        task_dict = {'task_type': 'exec', 'task': {
            'task_name': _task_name, 'signal_ids': _signal_ids, 'signal_type': _signal_type.name, 'info': _tasks
        }}

        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['code'] == 200:
            logger.info('{} tasks submitted'.format(rsp['msg']))
            await cls.download_abstract(rsp['msg'])
        else:
            logger.error(rsp['msg'])

    @classmethod
    async def download_orders(cls, _task_id, _separate=True):
        zmq_client = AsyncReqZmq(port, host)
        abstract = pd.read_csv('../algoFile/abstract_{}.csv'.format(_task_id)).to_dict('records')
        if not abstract:
            logger.error('abstract does not exist')
            return

        if not os.path.exists('../algoFile/cluster_orders_{}'.format(_task_id)):
            os.mkdir('../algoFile/cluster_orders_{}'.format(_task_id))

        orders = []
        signal_ids = list(set([v['_signal_id'] for v in abstract]))
        while signal_ids:
            try:
                signal_id = signal_ids.pop(0)
                task_dict = {'task_type': 'download_orders', 'task': {
                    'task_id': _task_id, 'signal_id': signal_id
                }}
                tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
                decompressed = gzip.decompress(tmp)
                rsp = json.loads(decompressed.decode())
                if rsp['code'] == 250:
                    logger.error(rsp['msg'])
                    return

                elif _separate:

                    pd.DataFrame(rsp['msg']).to_csv(
                        '../algoFile/cluster_orders_{}/{}.csv'.format(_task_id, signal_id)
                    )
                    logger.info('{} finished: {}'.format(signal_id, len(rsp['msg'])))

                else:
                    orders.extend(rsp['msg'])

            except Exception as e:
                logger.error(e)
                time.sleep(60)

        if orders:
            pd.DataFrame(orders).to_csv('../algoFile/cluster_all_orders_{}.csv'.format(_task_id))
            logger.info('{} finished: {}'.format(_task_id, len(orders)))

    @classmethod
    async def submit_target_tasks(
            cls, _task_name, _signal_ids, _target_method_name, _target_method_param, _data_type, _forward_window,
            _update_codes=True
    ):
        assert isinstance(_signal_ids, list)
        zmq_client = AsyncReqZmq(port, host)
        module_name = 'algoStrategy.target{}'.format(_target_method_name)
        module = importlib.import_module(module_name)
        script_content = inspect.getsource(module) if _update_codes else ''

        task_dict = {'task_type': 'target', 'task': {'type': 'code', 'info': {
            'module_name': _target_method_name, 'scripts': script_content
        }}}
        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['msg'] != 'finished':
            logger.error(rsp['msg'])
            return

        logger.info('strategy checked')
        # submit tasks
        task_dict = {'task_type': 'target', 'task': {
            'task_name': _task_name,
            'signal_ids': _signal_ids,
            'type': 'tasks',
            'info': {
                '_method_name': _target_method_name,
                '_method_param': _target_method_param,
                '_data_type': _data_type,
                '_forward_window': _forward_window
            }
        }}
        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['code'] == 200:
            logger.info('{} tasks submitted'.format(rsp['msg']))
            await cls.download_targets(rsp['msg'])
        else:
            logger.error(rsp['msg'])

    @classmethod
    async def download_targets(cls, _task_id):
        zmq_client = AsyncReqZmq(port, host)
        if not await cls.check_left(zmq_client, _task_id):
            return

        all_targets = []
        while True:
            try:
                task_dict = {'task_type': 'download_targets', 'task': _task_id}
                tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
                targets = json.loads(tmp.decode())
                if targets['code'] == 250:
                    logger.error(targets['error'])
                    return

                elif targets['msg'] == 'finished':
                    break

                else:
                    all_targets.extend(targets['msg'])

            except Exception as e:
                logger.error(e)

        if all_targets:
            pd.DataFrame(all_targets).to_csv('../algoFile/cluster_targets_{}.csv'.format(_task_id))

    @classmethod
    async def submit_signal_tasks(cls, _task_name, _tasks, _update_codes=True):
        # update codes 2 remote server
        zmq_client = AsyncReqZmq(port, host)
        module_names = set([v['_signal_method_name'] for v in _tasks])

        for name in module_names:
            module_name = 'algoStrategy.signal{}'.format(name)
            module = importlib.import_module(module_name)
            script_content = inspect.getsource(module) if _update_codes else ''

            task_dict = {'task_type': 'signal', 'task': {'type': 'code', 'info': {
                'module_name': name, 'scripts': script_content
            }}}
            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['msg'] == 'finished':
                continue
            else:
                logger.error(rsp['msg'])
                return

        logger.info('strategy checked')
        # submit tasks
        task_dict = {'task_type': 'signal', 'task': {'task_name': _task_name, 'type': 'tasks', 'info': _tasks}}
        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['code'] == 200:
            logger.info('{} tasks submitted'.format(rsp['msg']))
            await cls.download_abstract(rsp['msg'])
        else:
            logger.error(rsp['msg'])

    @classmethod
    async def download_abstract(cls, _task_id):
        zmq_client = AsyncReqZmq(port, host)
        if not await cls.check_left(zmq_client, _task_id):
            return

        # download abstract
        task_dict = {'task_type': 'download_abstract', 'task': _task_id}
        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['code'] == 200:
            pd.DataFrame(rsp['msg']).to_csv('../algoFile/abstract_{}.csv'.format(_task_id))
            logger.info('{} abstract saved'.format(_task_id))

        else:
            logger.info('{} not available: {}'.format(_task_id, rsp['msg']))

    @classmethod
    async def check_left(cls, _zmq_client, _task_id) -> bool:
        while True:
            try:
                msg = json.dumps({'task_type': 'check', 'task': _task_id}).encode()
                tmp = await _zmq_client.send_msg(msg)
                rsp = json.loads(tmp.decode())
                if rsp['code'] == 200:
                    if rsp['msg'] is None:
                        logger.info('{} finished'.format(_task_id))
                        return True

                    logger.info('{} left {}'.format(_task_id, rsp['msg']))
                    time.sleep(5)

                else:
                    logger.error(rsp['msg'])
                    return False

            except Exception as e:
                logger.error(e)
                time.sleep(60)


if __name__ == '__main__':
    import asyncio

    loop = asyncio.get_event_loop()
    coro = BrokerMgr.download_orders('1732540733065112_exec_test', False)
    loop.run_until_complete(coro)
