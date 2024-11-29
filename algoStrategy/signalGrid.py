# -*- coding: utf-8 -*-
"""
@Create: 2024/10/17 8:51
@File: signalGrid.py
@Author: Jingyuan
"""
import uuid
from typing import Optional, List, Dict

from algoUtils.DefUtil import SignalBase


class Grid(SignalBase):
    def __init__(self, _grid):
        self.grid = _grid
        self.last_price = None

    def generate_signals(self, _data: list) -> Optional[List[Dict]]:
        current_price = _data[-1]['close']
        symbol = _data[-1]['symbol']
        if self.last_price is None:
            self.last_price = current_price
            return [{
                'bind_id': str(uuid.uuid4()),
                'symbol': symbol,
                'action': 'open',
                'position': 'long',
            }]

        if abs(current_price - self.last_price) / (current_price + self.last_price) * 2 <= self.grid:
            return

        position = 'long' if current_price > self.last_price else 'short'
        self.last_price = current_price
        return [{
            'bind_id': str(uuid.uuid4()),
            'symbol': symbol,
            'action': 'open',
            'position': position,
        }]
