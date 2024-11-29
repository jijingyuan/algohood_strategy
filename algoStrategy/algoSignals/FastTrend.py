# -*- coding: utf-8 -*-
"""
@Create: 2024/11/29 10:26
@File: FastTrend.py
@Author: Jingyuan
"""
from typing import Optional, List, Dict

from algoUtils.DefUtil import SignalBase


class FastTrend(SignalBase):
    def __init__(self, _time_th):
        self.time_th = _time_th
        self.cache = []
        self.start = False

    def generate_signals(self, _data: list) -> Optional[List[Dict]]:
        self.cache.extend(_data)
        if not self.start:
            return
