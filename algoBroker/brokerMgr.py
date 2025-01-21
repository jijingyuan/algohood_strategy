# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 14:06
@File: brokerMgr.py
@Author: Jingyuan
"""
import asyncio
import gzip
import importlib
import inspect
import json
import os
import time
from enum import Enum

import pandas as pd

from algoConfig.execConfig import delay_dict, fee_dict
from algoConfig.zmqConfig import host, port
from algoExecution.algoEngine.dataMgr import DataMgr as ExecuteDataMgr
from algoExecution.algoEngine.eventMgr import EventMgr
from algoPortfolio.algoEngine.dataMgr import DataMgr as PortfolioDataMgr
from algoPortfolio.algoEngine.eventMgr import EventMgr as PortfolioEventMgr
from algoSignal.algoEngine.dataMgr import DataMgr as SignalDataMgr
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

    @staticmethod
    async def get_active_symbols(_expire_duration, _data_type):
        zmq_client = AsyncReqZmq(port, host)
        task_dict = {
            'task_type': 'active_symbols', 'task': {'expire_duration': _expire_duration, 'data_type': _data_type}
        }
        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['code'] == 200:
            return rsp['msg']

        else:
            logger.error(rsp['msg'])

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
    def prepare_target_task(cls, _target_name, _target_method_name, _target_method_param, _data_type):
        return {
            '_target_name': _target_name,
            '_target_method_name': _target_method_name,
            '_target_method_param': _target_method_param,
            '_data_type': _data_type,
        }

    @classmethod
    def prepare_execute_task(cls, _execute_name, _execute_method_name, _execute_method_param, _data_type):
        return {
            '_execute_name': _execute_name,
            '_execute_method': _execute_method_name,
            '_execute_param': _execute_method_param,
            '_data_type': _data_type,
        }

    @classmethod
    def prepare_portfolio_task(
            cls,
            _portfolio_name,
            _data_type,
            _optimizer_method_name,
            _optimizer_method_param,
            _risk_method_name,
            _risk_method_param,
            _liquidity_method_name,
            _liquidity_method_param,
            _open_rebalance=False,
            _close_rebalance=False,
            _interval=None
    ):
        return {
            '_portfolio_name': _portfolio_name,
            '_data_type': _data_type,
            '_optimizer_method_name': _optimizer_method_name,
            '_optimizer_method_param': _optimizer_method_param,
            '_risk_method_name': _risk_method_name,
            '_risk_method_param': _risk_method_param,
            '_liquidity_method_name': _liquidity_method_name,
            '_liquidity_method_param': _liquidity_method_param,
            '_open_rebalance': _open_rebalance,
            '_close_rebalance': _close_rebalance,
            '_interval': _interval
        }

    @classmethod
    def prepare_order_task(cls, _order_name, _exec_id, _order_ids):
        abstract = pd.read_csv('../algoFile/abstract_{}.csv'.format(_exec_id)).to_dict('records')
        abstract_order_ids = {v['result_id']: (v['task_name'], v['signal_name']) for v in abstract}

        file_path = []
        for order_id in _order_ids:
            if order_id not in abstract_order_ids:
                logger.error('{} does not exist'.format(order_id))
                return

            file_path.append('../algoFile/cluster_{}/{}/{}.csv'.format(_exec_id, *abstract_order_ids[order_id]))

        return {
            '_order_name': _order_name,
            '_exec_id': _exec_id,
            '_order_ids': _order_ids,
            '_file_path': file_path,
        }

    @classmethod
    async def submit_signal_tasks(cls, _task_name, _tasks, _update_codes=True, _use_cluster=False):
        signal_names = [v['_signal_name'] for v in _tasks]
        if len(signal_names) > len(set(signal_names)):
            logger.error('duplicated signal names')
            return

        if _use_cluster:
            zmq_client = AsyncReqZmq(port, host)
            module_names = set([v['_signal_method_name'] for v in _tasks])

            for name in module_names:
                module_name = 'algoStrategy.algoSignals.{}'.format(name)
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
            file_name = '{}_{}'.format(int(time.time() * 1000000), _task_name)

            data_mgr = SignalDataMgr()
            await data_mgr.init_data_mgr()
            abstract_list = []
            for task in _tasks:
                data_mgr.clear_cache()
                signal_name = task.pop('_signal_name')
                result_path = '{}/{}'.format(file_name, signal_name)
                param = task.copy()
                data_mgr.set_data_type(task.pop('_data_type'))
                signal_mgr = SignalMgr(
                    task.pop('_signal_method_name'),
                    task.pop('_signal_method_param'),
                    data_mgr
                )
                signals = await signal_mgr.start_task(**task)

                if signals:
                    abstract = {'result_path': result_path, 'result_counts': len(signals), 'signal_name': signal_name}
                    os.makedirs('../algoFile/{}'.format(file_name), exist_ok=True)
                    abstract_list.append({**abstract, **param})
                    pd.DataFrame(signals).to_csv('../algoFile/{}.csv'.format(result_path))
                    logger.info('{} finished'.format(signal_name))

            if abstract_list:
                pd.DataFrame(abstract_list).to_csv('../algoFile/abstract_{}.csv'.format(file_name))

            logger.info('{} finished'.format(file_name))

    @classmethod
    async def submit_target_tasks(
            cls, _task_name, _tasks, _signal_paths, _keep_empty=False, _abstract_method=None, _abstract_param=None,
            _update_codes=True, _use_cluster=False
    ):
        target_names = [v['_target_name'] for v in _tasks]
        if len(target_names) > len(set(target_names)):
            logger.error('duplicated target names')
            return

        if _use_cluster:
            zmq_client = AsyncReqZmq(port, host)
            module_names = set([v['_target_method_name'] for v in _tasks])

            for name in module_names:
                module_name = 'algoStrategy.algoTargets.{}'.format(name)
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
                'type': 'tasks',
                'task_name': _task_name,
                'signal_paths': _signal_paths,
                'info': _tasks,
                'keep_empty': _keep_empty
            }}

            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['code'] == 200:
                logger.info('{} tasks submitted'.format(rsp['msg']))
                await cls.download_abstract(rsp['msg'], _abstract_method, _abstract_param)
            else:
                logger.error(rsp['msg'])

        else:
            file_name = '{}_{}'.format(int(time.time() * 1000000), _task_name)

            data_mgr = SignalDataMgr()
            await data_mgr.init_data_mgr()

            abstracts = []
            for task in _tasks:
                task_name = task['_target_name']
                data_mgr.set_data_type(task['_data_type'])
                target_mgr = TargetMgr(
                    task['_target_method_name'],
                    task['_target_method_param'],
                    data_mgr
                )

                for signal_path in _signal_paths:
                    _, signal_name = signal_path.split('/')
                    file_path = '../algoFile/{}.csv'.format(signal_path)
                    signals = pd.read_csv(file_path).to_dict('records')
                    targets = []
                    for signal in signals:
                        signal.pop('Unnamed: 0', None)
                        target = await target_mgr.start_task(signal, _keep_empty)
                        if target:
                            targets.append(target)

                    task_path = '../algoFile/{}/{}'.format(file_name, task_name)
                    abstract = await target_mgr.generate_abstract(targets, _abstract_method, _abstract_param) or {}
                    abstracts.append({
                        'result_path': '{}/{}/{}'.format(file_name, task_name, signal_name),
                        'result_counts': len(signals),
                        **task,
                        **abstract
                    })

                    if targets:
                        os.makedirs(task_path, exist_ok=True)
                        pd.DataFrame(targets).to_csv('{}/{}.csv'.format(task_path, signal_name))
                        logger.info('{}/{} finished'.format(task_name, signal_name))

            if abstracts:
                pd.DataFrame(abstracts).to_csv('../algoFile/abstract_{}.csv'.format(file_name))

            logger.info('{} finished'.format(file_name))

    @classmethod
    async def submit_execute_tasks(cls, _task_name, _tasks, _signal_paths, _user_cluster=False, _update_codes=False):
        exec_config = {}
        for exchange_name, delay in delay_dict.items():
            fee = fee_dict[exchange_name]
            exec_config[exchange_name] = {'_delay_dict': delay, '_fee_dict': fee}

        if _user_cluster:
            execute_names = [v['_execute_name'] for v in _tasks]
            if len(execute_names) > len(set(execute_names)):
                logger.error('duplicated execute names')
                return

            zmq_client = AsyncReqZmq(port, host)
            module_names = set([v['_execute_method'] for v in _tasks])

            for name in module_names:
                module_name = 'algoStrategy.algoExecutors.{}'.format(name)
                module = importlib.import_module(module_name)
                script_content = inspect.getsource(module) if _update_codes else ''

                task_dict = {'task_type': 'exec', 'task': {'type': 'code', 'info': {
                    'module_name': name, 'scripts': script_content
                }}}
                tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
                rsp = json.loads(tmp.decode())
                if rsp['msg'] != 'finished':
                    logger.error(rsp['msg'])
                    return

            logger.info('strategy checked')
            task_dict = {'task_type': 'exec', 'task': {'type': 'tasks', 'info': {
                'task_name': _task_name,
                'signal_paths': _signal_paths,
                'exec_config': exec_config,
                'info': _tasks
            }}}

            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['code'] == 200:
                logger.info('{} tasks submitted'.format(rsp['msg']))
                await cls.download_abstract(rsp['msg'])
            else:
                logger.error(rsp['msg'])

        else:
            file_name = '{}_{}'.format(int(time.time() * 1000000), _task_name)

            data_mgr = ExecuteDataMgr()
            await data_mgr.init_data_mgr()

            for task in _tasks:
                execute_name = task.pop('_execute_name')
                data_mgr.set_data_type(task.pop('_data_type'))
                event_mgr = EventMgr(_data_mgr=data_mgr, **task)
                await event_mgr.init_mgrs(_logger_type='local', _exec_config=exec_config)

                orders = []
                task_path = '../algoFile/{}'.format(file_name)
                for signal_path in _signal_paths:
                    file_path = '../algoFile/{}.csv'.format(signal_path)
                    signals = pd.read_csv(file_path).to_dict('records')
                    signals.sort(key=lambda x: x['signal_timestamp'])
                    orders.append(await event_mgr.start_event_loop(signals))

                if orders:
                    os.makedirs(task_path, exist_ok=True)
                    pd.DataFrame(orders).to_csv('{}/{}.csv'.format(task_path, execute_name))
                    logger.info('{}/{} finished'.format(file_name, execute_name))

            logger.info('{} finished'.format(file_name))

    @classmethod
    async def submit_portfolio_tasks(
            cls, _task_name, _portfolio_tasks, _order_tasks, _update_codes=True, _use_cluster=True
    ):
        if _use_cluster:
            zmq_client = AsyncReqZmq(port, host)

            name_dict = {'Optimizers': set(), 'Risks': set(), 'Liquiditys': set()}
            for portfolio_task in _portfolio_tasks:
                name_dict['Optimizers'].add(portfolio_task['_optimizer_method_name'])
                name_dict['Risks'].add(portfolio_task['_risk_method_name'])
                name_dict['Liquiditys'].add(portfolio_task['_liquidity_method_name'])

            info = {}
            for module_type, module_names in name_dict.items():
                for p_module in module_names:
                    module_name = 'algoStrategy.algo{}.{}'.format(module_type, p_module)
                    module = importlib.import_module(module_name)
                    info.setdefault(module_type, {})[p_module] = inspect.getsource(module) if _update_codes else ''

            task_dict = {'task_type': 'portfolio', 'task': {'type': 'code', 'info': info}}
            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['msg'] != 'finished':
                logger.error(rsp['msg'])
                return

            logger.info('strategy checked')
            task_dict = {'task_type': 'portfolio', 'task': {
                'type': 'tasks',
                'task_name': _task_name,
                'portfolio_tasks': _portfolio_tasks,
                'order_tasks': _order_tasks
            }}

            tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
            rsp = json.loads(tmp.decode())
            if rsp['code'] == 200:
                logger.info('{} tasks submitted'.format(rsp['msg']))
                await cls.download_abstract(rsp['msg'])
            else:
                logger.error(rsp['msg'])

        else:
            file_name = '{}_{}'.format(int(time.time() * 1000000), _task_name)
            folder_name = 'local_{}'.format(file_name)

            data_mgr = PortfolioDataMgr()
            await data_mgr.init_data_mgr()
            abstract_list = []
            for order_task in _order_tasks:
                order_name = order_task['_order_name']
                portfolio_orders = []
                for index, path in enumerate(order_task['_file_path']):
                    orders = pd.read_csv(path).to_dict('records')
                    for order in orders:
                        order.pop('Unnamed: 0')
                        portfolio_orders.append({'strategy_id': order_task['_order_ids'][index], **order})

                portfolio_orders.sort(key=lambda x: x['local_timestamp'])
                for portfolio_task in _portfolio_tasks:
                    portfolio_name = portfolio_task.pop('_portfolio_name')
                    interval = portfolio_task.pop('_interval')
                    data_mgr.clear_cache()
                    await data_mgr.load_orders(portfolio_orders)
                    data_mgr.set_data_type(portfolio_task.pop('_data_type'))
                    event_mgr = PortfolioEventMgr(portfolio_task, data_mgr, interval)
                    earnings = await event_mgr.start_sim_portfolio()
                    if earnings:
                        res_path = '../algoFile/{}/{}'.format(folder_name, portfolio_name)
                        os.makedirs(res_path, exist_ok=True)
                        pd.DataFrame(earnings).to_csv('{}/{}.csv'.format(res_path, order_name))

            if abstract_list:
                pd.DataFrame(abstract_list).to_csv('../algoFile/abstract_{}.csv'.format(file_name))

            logger.info('{} finished'.format(file_name))

    @classmethod
    async def download_results(cls, _task_id, _task_type, _split=True):
        check_list = ['signal', 'target', 'exec', 'portfolio']
        if _task_type not in check_list:
            logger.error('unknown task type: {}|{}'.format(_task_type, check_list))
            return

        zmq_client = AsyncReqZmq(port, host)
        abstract_list = pd.read_csv('../algoFile/abstract_{}.csv'.format(_task_id)).to_dict('records')
        if not abstract_list:
            logger.error('abstract does not exist')
            return

        for abstract in abstract_list:
            try:
                task_dict = {'task_type': 'download_results', 'task': {
                    'result_path': abstract['result_path'], 'result_type': _task_type
                }}
                tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
                decompressed = gzip.decompress(tmp)
                rsp = json.loads(decompressed.decode())
                if rsp['code'] == 250:
                    logger.error(rsp['msg'])
                    continue

                if rsp['msg']:
                    if _task_type == 'exec':
                        file_path = '../algoFile/{}/{}'.format(_task_id, abstract['_execute_name'])
                        os.makedirs(file_path, exist_ok=True)
                        pd.DataFrame(rsp['msg']).to_csv('{}/{}.csv'.format(file_path, abstract['signal_name']))
                        logger.info('{} finished: {}'.format(
                            abstract['result_path'], len(rsp['msg'])
                        ))

                    elif _task_type == 'signal':
                        os.makedirs('../algoFile/{}'.format(_task_id), exist_ok=True)
                        pd.DataFrame(rsp['msg']).to_csv('../algoFile/{}.csv'.format(abstract['result_path']))
                        logger.info('{} finished: {}'.format(
                            abstract['result_path'], len(rsp['msg'])
                        ))

                    elif _task_type == 'target':
                        os.makedirs('../algoFile/{}/{}'.format(_task_id, abstract['_target_name']), exist_ok=True)
                        pd.DataFrame(rsp['msg']).to_csv('../algoFile/{}.csv'.format(abstract['result_path']))
                        logger.info('{} finished: {}'.format(
                            abstract['result_path'], len(rsp['msg'])
                        ))

                    elif _task_type == 'portfolio':
                        file_path = '../algoFile/cluster_{}/{}'.format(_task_id, abstract['portfolio_name'])
                        os.makedirs(file_path, exist_ok=True)
                        pd.DataFrame(rsp['msg']).to_csv('{}/{}.csv'.format(file_path, abstract['order_name']))
                        logger.info('{}/{} finished: {}'.format(
                            abstract['portfolio_name'], abstract['order_name'], len(rsp['msg'])
                        ))

            except Exception as e:
                logger.error(e)
                time.sleep(60)

    @classmethod
    async def download_abstract(cls, _task_id, _abstract_method=None, _abstract_param=None):
        zmq_client = AsyncReqZmq(port, host)
        if not await cls.check_left(zmq_client, _task_id):
            return

        # download abstract
        task_dict = {'task_type': 'download_abstract', 'task': {'task_id': _task_id}}
        if _abstract_method is not None:
            module_name = 'algoStrategy.algoAbstracts.{}'.format(_abstract_method)
            module = importlib.import_module(module_name)
            task_dict['task'].update(
                {
                    'abstract_method': _abstract_method,
                    'abstract_param': _abstract_param,
                    'scripts': inspect.getsource(module)
                }
            )

        tmp = await zmq_client.send_msg(json.dumps(task_dict).encode())
        rsp = json.loads(tmp.decode())
        if rsp['code'] == 200:
            pd.DataFrame(rsp['msg']).to_csv('../algoFile/abstract_{}.csv'.format(_task_id))
            logger.info('{} abstract saved'.format(_task_id))

        else:
            logger.info('{} not available: {}'.format(_task_id, rsp['msg']))

    @classmethod
    async def generate_abstract(cls, _task_id, _abstract_method, _local_test=True):
        if _local_test:
            pass

        else:
            zmq_client = AsyncReqZmq(port, host)
            if not await cls.check_left(zmq_client, _task_id):
                return

            # download abstract
            task_dict = {'task_type': 'generate_abstract', 'task': _task_id}
            module_name = 'algoStrategy.algoAbstracts.{}'.format(_abstract_method)
            module = importlib.import_module(module_name)
            task_dict['scripts'] = inspect.getsource(module)

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
                    await asyncio.sleep(5)

                else:
                    logger.error(rsp['msg'])
                    return False

            except Exception as e:
                logger.error(e)
                time.sleep(60)
