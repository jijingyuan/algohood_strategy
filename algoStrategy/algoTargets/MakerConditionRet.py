# -*- coding: utf-8 -*-
"""
@Create on  2024/9/27 15:12
@file: MakerConditionRet.py
@author: Jerry
"""
import sys
import numpy as np
from typing import Optional, Dict

from algoUtils.DefUtil import TargetBase


class Algo(TargetBase):
    def __init__(self, _maker_hold, _profit_grid, _stop_grid, _stop_delay):
        self.maker_hold = _maker_hold
        self.profit_grid = _profit_grid
        self.stop_grid = _stop_grid
        self.stop_delay = _stop_delay
        self.precision = 0

    def update_precision(self, _price):
        decimal_places = len(str(_price).split(".")[-1])
        self.precision = max(self.precision, decimal_places)

    async def generate_targets(self, _signal, _data_mgr) -> Optional[Dict]:
        a_m = 1 if _signal['action'] == 'open' else -1
        p_m = 1 if _signal['position'] == 'long' else -1
        multiplier = 1 if a_m * p_m > 0 else -1
        price_dict = _signal['signal_price']
        current_price = price_dict[_signal['symbol']]
        self.update_precision(current_price)
        min_move = 0.1 ** self.precision / current_price
        if min_move > 0.0001:
            return

        profit_price = round(current_price * (1 + multiplier * self.profit_grid), self.precision)
        stop_price = round(current_price * (1 - multiplier * self.stop_grid), self.precision)
        abstract: np.array = await _data_mgr.get_abstract(_signal['symbol'])
        condition = abstract[:, -1] > _signal['signal_timestamp']
        filtered = abstract[condition]
        if filtered.any():
            start_ts = float(filtered[0, 0])
            end_ts = float(filtered[0, -1])
            arr = await _data_mgr.get_all_data_by_symbol(_signal['symbol'], start_ts, end_ts)
            condition_1 = arr[:, 0] > _signal['signal_timestamp']
            condition_2 = arr[:, 0] < _signal['signal_timestamp'] + self.maker_hold
            condition_3 = arr[:, 4] < 1 if multiplier > 0 else arr[:, 4] > 0
            condition_4 = arr[:, 2] < current_price if multiplier > 0 else arr[:, 2] > current_price
            filter_condition = condition_1 & condition_2 & condition_3 & condition_4
            if filter_condition.any():
                index = np.argmax(filter_condition)
                open_ts = arr[index, 0]

            else:
                return

        else:
            return

        cut_ts = open_ts
        while True:
            condition_1 = abstract[:, -1] > cut_ts
            condition_2 = abstract[:, 1] > profit_price if multiplier > 0 else abstract[:, 1] > stop_price
            condition_3 = abstract[:, 2] < stop_price if multiplier > 0 else abstract[:, 2] < profit_price

            condition = condition_1 & (condition_2 | condition_3)
            filtered = abstract[condition]
            if filtered.any():
                start_ts = float(filtered[0, 0])
                end_ts = float(filtered[0, -1])
                arr = await _data_mgr.get_all_data_by_symbol(_signal['symbol'], start_ts, end_ts)
                condition_1 = arr[:, 0] > _signal['signal_timestamp']
                condition_2 = arr[:, 0] > _signal['signal_timestamp'] + self.stop_delay
                condition_3 = arr[:, 2] > profit_price if multiplier > 0 else arr[:, 2] < profit_price
                condition_4 = arr[:, 2] < stop_price if multiplier > 0 else arr[:, 2] > stop_price

                filter_condition = (condition_1 & condition_3) | (condition_2 & condition_4)
                if filter_condition.any():
                    index = np.argmax(filter_condition)
                    close = arr[index, 2]
                    return {
                        'open_timestamp': float(open_ts),
                        'win': int(close > profit_price) if multiplier > 0 else int(close < profit_price),
                        'ret': float((close - current_price) / (close + current_price) * 2 * multiplier),
                        'close_timestamp': float(arr[index, 0])
                    }

                else:
                    cut_ts = end_ts

            else:
                return
