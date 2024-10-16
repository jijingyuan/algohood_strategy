# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 14:59
@file: signalMA.py
@author: Jerry
"""
import uuid
from typing import Optional, List, Dict

import numpy as np
from collections import deque
from algoUtils.DefUtil import SignalBase


class MA(SignalBase):
    def __init__(self, _short_term, _long_term):
        self.short_term = _short_term
        self.long_term = _long_term
        self.cache = deque(maxlen=_long_term)
        self.symbol = None
        self.prev_direction = None

    def generate_signals(self, _data: list) -> Optional[List[Dict]]:
        """
        generate signal to execution module
        :param _data: data source
        :return: [{
            'bind_id': str(uuid),
            'symbol': 'btc_usdt|binance_future',
            'action': 'open' or 'close'
            'position': 'long' or 'short'
            **kwargs: other info that you need for analyze
        }, ...]
        """
        if self.symbol is None:
            self.symbol = _data[-1]['symbol']

        close_list = [v['close'] for v in _data]
        self.cache.append(np.mean(close_list))

        if len(self.cache) < self.long_term:
            return

        short_cache = list(self.cache)[-self.short_term:]
        condition = np.mean(short_cache) > np.mean(self.cache)
        curr_direction = 1 if condition else -1
        if self.prev_direction is None:
            self.prev_direction = curr_direction
            return

        if self.prev_direction * curr_direction >= 0:
            return

        self.prev_direction = curr_direction
        return [{
            'bind_id': str(uuid.uuid4()),
            'symbol': self.symbol,
            'action': 'open',
            'position': 'long' if curr_direction > 0 else 'short',
        }]
