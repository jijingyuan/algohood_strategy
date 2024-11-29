# -*- coding: utf-8 -*-
"""
@Create: 2024/11/28 16:13
@File: portfolio_needle_reverse.py
@Author: Jingyuan
"""
from algoBroker.brokerMgr import BrokerMgr


async def start_task(_file):
    if not _file:
        return

    portfolio_tasks = [
        BrokerMgr.prepare_portfolio_task(
            _portfolio_name='needle_reverse',
            _data_type='trade',
            _optimizer_method_name='Test',
            _optimizer_method_param={},
            _risk_method_name='Test',
            _risk_method_param={},
            _liquidity_method_name='Test',
            _liquidity_method_param={},
            _trigger_type='earning',
            _interval=None,
            _omit_open=True,
            _omit_close=True
        )
    ]

    order_tasks = [
        BrokerMgr.prepare_order_task(
            _order_name='long_orders',
            _order_task_id='1732860022705314_exec_test',
            _order_ids=[v['result_id'] for v in file if 'long' in v['task_name']]
        )
    ]

    await BrokerMgr.submit_portfolio_tasks(
        _task_name='portfolio_test',
        _portfolio_tasks=portfolio_tasks,
        _order_tasks=order_tasks,
        _use_cluster=False
    )


if __name__ == '__main__':
    import asyncio

    file = BrokerMgr.get_abstract_given_file_name('1732860022705314_exec_test').to_dict('records')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_task(file))
