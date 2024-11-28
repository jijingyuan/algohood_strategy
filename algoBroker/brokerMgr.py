# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 14:06
@File: brokerMgr.py
@Author: Jingyuan
"""
import gzip
import importlib
import inspect
import json
import os
import time
from enum import Enum

import pandas as pd

from algoConfig.zmqConfig import host, port
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
    def prepare_signal_task(
            cls, _signal_name, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag, _start_timestamp,
            _end_timestamp
    ):
        return {
            '_signal_name': _signal_name,
            '_signal_method_name': _signal_method_name,
            '_signal_method_param': _signal_method_param,
            '_symbols': _symbols,
            '_data_type': _data_type,
            '_lag': _lag,
            '_start_timestamp': _start_timestamp,
            '_end_timestamp': _end_timestamp,
        }

    @classmethod
    def prepare_target_task(cls, _target_name, _target_method_name, _target_method_param, _data_type, _forward_window):
        return {
            '_target_name': _target_name,
            '_target_method_name': _target_method_name,
            '_target_method_param': _target_method_param,
            '_data_type': _data_type,
            '_forward_window': _forward_window
        }

    @classmethod
    def prepare_execute_task(cls, _execute_name, _exec_method_name, _exec_method_param, _data_type):
        return {
            '_execute_name': _execute_name,
            '_execute_method': _exec_method_name,
            '_execute_param': _exec_method_param,
            '_data_type': _data_type,
        }

    @classmethod
    async def submit_signal_tasks(cls, _task_name, _tasks, _update_codes=True, _use_cluster=False):
        if _use_cluster:
            signal_names = [v['_signal_name'] for v in _tasks]
            if len(signal_names) > len(set(signal_names)):
                logger.error('duplicated signal names')
                return

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
                if rsp['msg'] != 'finished':
                    logger.error(rsp['msg'])
                    return

            logger.info('strategy checked')
            task_dict = {'task_type': 'signal', 'task': {'task_name': _task_name, 'type': 'tasks', 'info': _tasks}}
            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['code'] == 200:
                logger.info('{} tasks submitted'.format(rsp['msg']))
                await cls.download_abstract(rsp['msg'])
            else:
                logger.error(rsp['msg'])

        else:
            if not os.path.exists('../algoFile'):
                os.mkdir('../algoFile')

            file_name = '{}_{}'.format(int(time.time() * 1000000), _task_name)
            folder_name = 'local_{}'.format(file_name)
            os.mkdir('../algoFile/{}'.format(folder_name))

            data_mgr = signalDataMgr()
            await data_mgr.init_data_mgr()
            abstract_list = []
            for task in _tasks:
                data_mgr.clear_cache()
                signal_name = task.pop('_signal_name')
                saving_name = '{}/{}'.format(folder_name, signal_name)
                param = task.copy()
                data_mgr.set_data_type(task.pop('_data_type'))
                signal_mgr = SignalMgr(
                    task.pop('_signal_method_name'),
                    task.pop('_signal_method_param'),
                    data_mgr
                )
                signals = await signal_mgr.start_task(**task)

                if signals:
                    abstract_list.append({'result_id': saving_name, 'result_counts': len(signals), **param})
                    pd.DataFrame(signals).to_csv('../algoFile/{}.csv'.format(saving_name))
                    logger.info('{} finished'.format(signal_name))

            if abstract_list:
                pd.DataFrame(abstract_list).to_csv('../algoFile/abstract_{}.csv'.format(file_name))

            logger.info('{} finished'.format(file_name))

    @classmethod
    async def submit_target_tasks(cls, _task_name, _tasks, _signal_ids, _update_codes=True, _use_cluster=False):
        if _use_cluster:
            zmq_client = AsyncReqZmq(port, host)
            module_names = set([v['_target_method_name'] for v in _tasks])

            for name in module_names:
                module_name = 'algoStrategy.target{}'.format(name)
                module = importlib.import_module(module_name)
                script_content = inspect.getsource(module) if _update_codes else ''

                task_dict = {'task_type': 'target', 'task': {'type': 'code', 'info': {
                    'module_name': name, 'scripts': script_content
                }}}
                tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
                rsp = json.loads(tmp.decode())
                if rsp['msg'] != 'finished':
                    logger.error(rsp['msg'])
                    return

            task_dict = {'task_type': 'target', 'task': {
                'type': 'tasks', 'task_name': _task_name, 'signal_ids': _signal_ids, 'info': _tasks
            }}

            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['code'] == 200:
                logger.info('{} tasks submitted'.format(rsp['msg']))
                await cls.download_abstract(rsp['msg'])
            else:
                logger.error(rsp['msg'])

        else:
            if not os.path.exists('../algoFile'):
                os.mkdir('../algoFile')

            file_name = '{}_{}'.format(int(time.time() * 1000000), _task_name)
            folder_name = 'local_{}'.format(file_name)
            os.mkdir('../algoFile/{}'.format(folder_name))

            data_mgr = signalDataMgr()
            await data_mgr.init_data_mgr()

            for task in _tasks:
                task_name = task['_target_name']
                data_mgr.set_data_type(task['_data_type'])
                target_mgr = TargetMgr(
                    task['_target_method_name'],
                    task['_target_method_param'],
                    data_mgr
                )

                task_path = '../algoFile/{}/{}'.format(folder_name, task_name)
                os.mkdir(task_path)
                for signal_id in _signal_ids:
                    _, signal_name = signal_id.split('/')
                    signal_path = '../algoFile/{}.csv'.format(signal_id)
                    signals = pd.read_csv(signal_path).to_dict('records')
                    targets = []
                    for signal in signals:
                        signal.pop('Unnamed: 0', None)
                        target = await target_mgr.start_task(signal, task['_forward_window'])
                        targets.append(target)

                    if targets:
                        pd.DataFrame(targets).to_csv('{}/{}.csv'.format(task_path, signal_name))
                        logger.info('{}/{} finished'.format(task_name, signal_name))

            logger.info('{} finished'.format(file_name))

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
    async def download_results(cls, _task_id):
        zmq_client = AsyncReqZmq(port, host)
        abstract_list = pd.read_csv('../algoFile/abstract_{}.csv'.format(_task_id)).to_dict('records')
        if not abstract_list:
            logger.error('abstract does not exist')
            return

        folder_path = '../algoFile/cluster_{}'.format(_task_id)
        os.mkdir(folder_path)

        while abstract_list:
            try:
                abstract = abstract_list.pop(0)
                task_dict = {'task_type': 'download_results', 'task': {
                    'task_id': _task_id, 'result_id': abstract['result_id']
                }}
                tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
                decompressed = gzip.decompress(tmp)
                rsp = json.loads(decompressed.decode())
                if rsp['code'] == 250:
                    logger.error(rsp['msg'])
                    return

                if rsp['msg']:
                    file_path = '{}/{}'.format(folder_path, abstract['task_name'])
                    if not os.path.exists(file_path):
                        os.mkdir(file_path)

                    pd.DataFrame(rsp['msg']).to_csv('{}/{}.csv'.format(file_path, abstract['signal_name']))
                    logger.info('{}/{} finished: {}'.format(
                        abstract['task_name'], abstract['signal_name'], len(rsp['msg'])
                    ))

            except Exception as e:
                logger.error(e)
                time.sleep(60)

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
    # coro = BrokerMgr.download_abstract('1732671908127699_grids')
    coro = BrokerMgr.download_results('1732779350854121_exec_test')
    loop.run_until_complete(coro)
