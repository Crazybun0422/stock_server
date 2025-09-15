#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""Celery tasks for stock data processing."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List, Dict, Any

from celery import shared_task
from django.conf import settings


def _safe_import(module: str):
    try:
        return __import__(module)
    except Exception:  # pragma: no cover - missing optional deps
        return None


@dataclass
class ExplosionPattern:
    start_date: str
    kline: List[Dict[str, Any]]


def _find_explosion_patterns(df) -> List[ExplosionPattern]:
    """Identify explosion patterns in a DataFrame of daily kline data.

    The logic follows the user definition:
    - A 15 day window gaining more than 50% marks the start of an effective rise.
    - The effective window continues until price retraces more than 20% from the
      highest close reached during that window.
    - For every start date we record the 50 trading days prior to the move.
    """
    patterns: List[ExplosionPattern] = []
    if df is None or df.empty:
        return patterns
    df = df.sort_values('trade_date').reset_index(drop=True)
    closes = df['close']
    i = 0
    n = len(df)
    while i + 15 < n:
        start_price = closes.iloc[i]
        window_max = closes.iloc[i:i+15].max()
        if (window_max - start_price) / start_price >= 0.5:
            peak_index = closes.iloc[i:i+15].idxmax()
            peak_price = closes.iloc[peak_index]
            j = peak_index + 1
            while j < n and (peak_price - closes.iloc[j]) / peak_price <= 0.2:
                peak_price = max(peak_price, closes.iloc[j])
                j += 1
            start_date = df.at[i, 'trade_date']
            pre_start = df.iloc[max(i-50, 0):i]
            pattern = ExplosionPattern(
                start_date=start_date,
                kline=pre_start[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
                .rename(columns={'trade_date': 'date', 'vol': 'volume'})
                .to_dict('records')
            )
            patterns.append(pattern)
            i = j
        else:
            i += 1
    return patterns


def _train_tensorflow():
    tf = _safe_import('tensorflow')
    if tf is None:  # pragma: no cover - optional dependency
        return
    # Minimal stub model for demonstration
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=(50, 6)),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')
    # No actual training data provided; this is a placeholder.
    return model


@shared_task(bind=True)
def build_explosion_patterns(self):
    """Fetch all stock data, store explosion patterns and train TF model.

    Progress is reported via ``update_state`` with the keys ``current`` and
    ``total``.
    """
    ts = _safe_import('tushare')
    pd = _safe_import('pandas')
    pymongo = _safe_import('pymongo')
    if ts is None or pd is None or pymongo is None:  # pragma: no cover - deps missing
        return {'current': 0, 'total': 0}

    ts.set_token(getattr(settings, 'TUSHARE_TOKEN', ''))
    pro = ts.pro_api()
    stocks = pro.stock_basic(exchange='', list_status='L',
                             fields='ts_code,name,industry,exchange')
    stocks = stocks[(~stocks['ts_code'].str.startswith('688')) &
                    (stocks['exchange'] != 'BSE') &
                    (~stocks['name'].str.contains('ST'))]
    total = len(stocks)

    mongo_client = pymongo.MongoClient(getattr(settings, 'MONGO_URI', 'mongodb://localhost:27017/'))
    db = mongo_client.get_default_database() or mongo_client['productdb']
    collection = db['explosion_patterns']

    for idx, row in stocks.iterrows():
        code = row.ts_code
        self.update_state(state='PROGRESS', meta={'current': idx, 'total': total})
        daily = pro.daily(ts_code=code)
        patterns = _find_explosion_patterns(daily)
        if patterns:
            doc = {
                'ts_code': code,
                'industry': row.industry,
                'patterns': [p.__dict__ for p in patterns]
            }
            collection.update_one({'ts_code': code}, {'$set': doc}, upsert=True)
    _train_tensorflow()
    return {'current': total, 'total': total}


async def wait_for_task(async_result):
    """Utility coroutine to poll a Celery task for progress."""
    while not async_result.ready():
        meta = async_result.info or {}
        yield meta.get('current', 0), meta.get('total', 1)
        await asyncio.sleep(1)
    yield async_result.info.get('current', 1), async_result.info.get('total', 1)
