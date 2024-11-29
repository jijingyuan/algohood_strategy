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

    tasks = [
        BrokerMgr.prepare_portfolio_task(
            _portfolio_name='needle_reverse',
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

    await BrokerMgr.submit_portfolio_tasks(
        _task_name='portfolio_test',
        _tasks=tasks,
        _order_ids=[v['result_id'] for v in file],
        _use_cluster=False
    )


if __name__ == '__main__':
    import asyncio

    file = BrokerMgr.get_abstract_given_file_name('1732850400179628_exec_test').to_dict('records')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_task(file))
