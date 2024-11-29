# -*- coding: utf-8 -*-
"""
@Create on  2024/9/27 15:12
@file: ConditionRet.py
@author: Jerry
"""
from algoUtils.DefUtil import TargetBase


class ConditionRet(TargetBase):
    def __init__(self):
        pass

    def generate_targets(self, _data: list) -> dict or None:
        """
        generate target for signals
        :param _data: data source
        :return: {**kwargs: stats that you need for analyzing}
        """
        return {
            'interval_ret': _data[-1]['close'] / _data[0]['close'] - 1
        }
