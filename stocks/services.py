#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/3 12:42
# @Author：Malcolm
# @File : services.py
# @Software: PyCharm

# stocks/services.py
#
# import tushare as ts
# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta
# from django.conf import settings
# from loguru import logger
#
#
# class CandidateService:
#     def __init__(self, lookback_days: int = 300):
#         ts.set_token(settings.TUSHARE_TOKEN)
#         self.pro = ts.pro_api()
#         self.lookback_days = lookback_days
#
#     @staticmethod
#     def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
#         delta = series.diff()
#         up = delta.clip(lower=0)
#         down = -delta.clip(upper=0)
#         ma_up = up.rolling(period).mean()
#         ma_down = down.rolling(period).mean()
#         rs = ma_up / ma_down
#         return 100 - 100 / (1 + rs)
#
#     def get_candidates(self) -> list:
#         """
#         返回满足“波段底部且波段中曾出现涨停”的股票列表，
#         每项包含 ts_code、name，以及最近 200 日的 ['date','open','high','low','close'] kline 数据。
#         """
#         end_date = datetime.now().strftime('%Y%m%d')
#         start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')
#
#         stocks = self.pro.stock_basic(
#             exchange='',
#             list_status='L',
#             fields='ts_code,name'
#         )
#         #获取websocket总数
#         logger.info("length of stocks: {}", len(stocks))
#         result = []
#
#         for code, name in zip(stocks['ts_code'], stocks['name']):
#             #在此处发送websocket条目加1
#             # 排除科创板(688) & 北交所(83)
#
#             logger.info("拉取 {},{} 日线数据...", code, name)
#             if code.startswith(('688', '83')):
#                 continue
#
#             df = self.pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
#             if df is None or len(df) < 50:
#                 continue
#
#             df = df.sort_values('trade_date').reset_index(drop=True)
#
#             # 布林带
#             df['ma20'] = df['close'].rolling(20).mean()
#             df['std20'] = df['close'].rolling(20).std()
#             df['lower'] = df['ma20'] - 2 * df['std20']
#             # RSI
#             df['rsi14'] = self.calculate_rsi(df['close'], 14)
#             # MACD 柱状线
#             ema12 = df['close'].ewm(span=12, adjust=False).mean()
#             ema26 = df['close'].ewm(span=26, adjust=False).mean()
#             df['hist'] = (ema12 - ema26) - ((ema12 - ema26).ewm(span=9, adjust=False).mean())
#
#             today = df.iloc[-1]
#             prev = df.iloc[-2]
#             # 波段底部条件
#             if not ((today['close'] <= today['lower'] or today['rsi14'] < 30)
#                     and (today['hist'] > prev['hist'])):
#                 continue
#
#             # 波段中最近一次涨停
#             limit_pct = 19.8 if code.startswith('300') else 9.8
#             df_before = df[df['trade_date'] < today['trade_date']]
#             if df_before[df_before['pct_chg'] >= limit_pct].empty:
#                 continue
#
#             # 最近 200 日 OHLC
#             recent_200 = df.tail(200)[['trade_date', 'open', 'high', 'low', 'close']].copy()
#             recent_200.rename(columns={'trade_date': 'date'}, inplace=True)
#
#             result.append({
#                 'ts_code': code,
#                 'name': name,
#                 'kline': recent_200.to_dict(orient='records')
#             })
#
#         return result

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from django.conf import settings
from loguru import logger


class CandidateService:
    def __init__(self, lookback_days: int = 300):
        ts.set_token(settings.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        self.lookback_days = lookback_days

    @staticmethod
    def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ma_up = up.rolling(period).mean()
        ma_down = down.rolling(period).mean()
        rs = ma_up / ma_down
        return 100 - 100 / (1 + rs)

    def get_candidates(self) -> list:
        """
        批量拉取日线，使用向量化计算并按 ts_code 分组筛选：
        1) 当日处于波段底部（下轨或RSI超卖，且MACD柱线回升）
        2) 在此底部日之前的整个波段中至少出现一次涨停，
           或者底部当日的最高价达到了当日的涨停价
        返回 ts_code、name 列表。
        """
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')

        # 1. 代码-名称映射
        stocks = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,exchange')
        # 2. 过滤掉科创板（ts_code 以 688 开头）和北交所（exchange == 'BSE'）
        stocks = stocks[
            (~stocks['ts_code'].str.startswith('688')) &
            (stocks['exchange'] != 'BSE')
            ].reset_index(drop=True)
        name_map = dict(zip(stocks['ts_code'], stocks['name']))
        valid_codes = set(stocks['ts_code'].tolist())

        # 2. 批量拉取交易日数据（同之前）
        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        trade_dates = cal['cal_date'].tolist()
        dfs = []
        for td in trade_dates:
            df0 = self.pro.daily(trade_date=td,
                                 fields='ts_code,trade_date,open,high,low,close,pct_chg')
            logger.info("拉取 {} 日线数据...", td)
            if df0 is not None and not df0.empty:
                dfs.append(df0)
        if not dfs:
            return []

        df = pd.concat(dfs, ignore_index=True)
        df = df.sort_values(['ts_code', 'trade_date'])
        df = df[df['ts_code'].isin(valid_codes)]

        # 3. 向量化计算指标（同之前）
        df['ma20'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(20).mean())
        df['std20'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(20).std())
        df['lower'] = df['ma20'] - 2 * df['std20']
        df['rsi14'] = df.groupby('ts_code')['close'].transform(lambda x: self.calculate_rsi(x, 14))
        ema12 = df.groupby('ts_code')['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
        ema26 = df.groupby('ts_code')['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())
        signal = (ema12 - ema26).groupby(df['ts_code']).transform(lambda x: x.ewm(span=9, adjust=False).mean())
        df['hist'] = (ema12 - ema26) - signal

        results = []
        # 4. 分组筛选
        for code, grp in df.groupby('ts_code', sort=False):
            last = grp.iloc[-1]
            prev = grp.iloc[-2]

            # 4.1 波段底部条件
            if not ((last['close'] <= last['lower'] or last['rsi14'] < 30)
                    and (last['hist'] > prev['hist'])):
                continue

            # 4.2 涨停条件：波段上涨过程有过一次涨停，或底部当日最高价触及涨停
            limit_pct = 19.8 if code.startswith('300') else 9.8
            # 理论涨停价 = 前一日收盘 * (1 + limit_pct/100)
            theoretical_limit = prev['close'] * (1 + limit_pct / 100)

            # 整个波段：从开头到底部当日（不含当天）
            wave = grp.iloc[:-1]
            has_limit_up = (wave['pct_chg'] >= limit_pct).any()
            touched_limit = last['high'] >= theoretical_limit

            if not (has_limit_up or touched_limit):
                continue

            recent_200 = df.tail(200)[['trade_date', 'open', 'high', 'low', 'close']].copy()
            recent_200.rename(columns={'trade_date': 'date'}, inplace=True)
            # 通过所有条件，加入结果
            results.append({
                'ts_code': code,
                'name': name_map.get(code, ''),
                'kline': recent_200.to_dict(orient='records')
            })

        return results

    def get_ma5_cross_ma10(self) -> list:
        """
        策略：筛选当日 5 日均线上穿 10 日均线的股票
        返回包含 ts_code 和 name 的列表
        """
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')

        stocks = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        name_map = dict(zip(stocks['ts_code'], stocks['name']))

        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        dates = cal['cal_date'].tolist()
        dfs = []
        for dt in dates:
            df0 = self.pro.daily(trade_date=dt, fields='ts_code,trade_date,close')
            if df0 is not None and not df0.empty:
                dfs.append(df0)
        df = pd.concat(dfs, ignore_index=True)
        df = df.sort_values(['ts_code', 'trade_date'])
        df = df[~df['ts_code'].str.startswith(('688', '83'))]

        # 计算 MA5 和 MA10
        df['ma5'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(5).mean())
        df['ma10'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(10).mean())

        crosses = []
        for code, grp in df.groupby('ts_code', sort=False):
            if len(grp) < 11:
                continue
            prev5, prev10 = grp.iloc[-2][['ma5', 'ma10']]
            last5, last10 = grp.iloc[-1][['ma5', 'ma10']]
            # 判断上穿
            if prev5 <= prev10 and last5 > last10:
                crosses.append({'ts_code': code, 'name': name_map.get(code, '')})
        return crosses
