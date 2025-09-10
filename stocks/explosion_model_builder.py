#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/24 16:36
# @Author：Malcolm
# @File : explosion_model_builder.py
# @Software: PyCharm

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts
from pymongo import MongoClient


class ExplosionModelBuilder:
    def __init__(self,
                 tushare_token: str,
                 mongo_uri: str = "mongodb://localhost:27017",
                 db_name: str = "quant_models",
                 lookback_days: int = 300):
        # Tushare 初始化
        ts.set_token(tushare_token)
        self.pro = ts.pro_api()
        self.lookback_days = lookback_days

        # MongoDB 初始化
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.models_col = self.db["explosion_models"]
        self.progress_col = self.db["explosion_progress"]

    def _load_stock_list(self) -> pd.DataFrame:
        """获取沪深所有A股（排除 ST、科创板688、北交所BSE）"""
        df = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,name,exchange"
        )
        # 过滤 ST、科创板、北交所
        df = df[~df["name"].str.match(r'^[\*\s]*ST')]
        df = df[~df["ts_code"].str.startswith("688")]
        df = df[df["exchange"] != "BSE"]    
        df = df.reset_index(drop=True)
        return df

    def _get_progress(self) -> int:
        """读取上次跑到的股票索引"""
        rec = self.progress_col.find_one({"_id": "explosion"})
        return rec["last_idx"] if rec else 0

    def _set_progress(self, idx: int):
        """更新跑到的股票索引"""
        self.progress_col.update_one(
            {"_id": "explosion"},
            {"$set": {"last_idx": idx, "ts": datetime.utcnow()}},
            upsert=True
        )

    def _fetch_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """拉某只股票日线"""
        return self.pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,open,high,low,close,vol"
        )

    def build(self):
        """主流程：遍历股票，发现爆点并存 K 线模型到 Mongo"""
        stocks = self._load_stock_list()
        start_idx = self._get_progress()

        end_date = datetime.now().strftime("%Y%m%d")
        start_all = "20160101"  # 从 2016 年开始
        # 拉一次全量交易日历，用于计算“1 个月”≈20个交易日
        cal = self.pro.trade_cal(exchange="", start_date=start_all, end_date=end_date, is_open="1")
        trade_days = cal["cal_date"].tolist()

        for idx in range(start_idx, len(stocks)):
            ts_code = stocks.at[idx, "ts_code"]
            print(f"[{idx}/{len(stocks)}] Processing {ts_code} ...")
            try:
                df = self._fetch_daily(ts_code, start_all, end_date)
                if df is None or len(df) < 220:
                    self._set_progress(idx + 1)
                    continue

                df = df.sort_values("trade_date").reset_index(drop=True)
                # 计算未来 20 个交易日后的涨幅
                df["pct_forward20"] = (
                                              df["close"].shift(-20) / df["close"] - 1
                                      ) * 100

                # 定位所有“20 日后涨幅 ≥100%”的位置
                hits = df.index[df["pct_forward20"] >= 100].tolist()

                for hit in hits:
                    if hit < 200:
                        continue  # 不足200日历史，跳过

                    model_df = df.loc[hit - 200: hit - 1,
                               ["trade_date", "open", "high", "low", "close", "vol"]].copy()
                    model_df.rename(columns={"trade_date": "date", "vol": "volume"}, inplace=True)

                    # 存到 Mongo
                    doc = {
                        "ts_code": ts_code,
                        "event_date": df.at[hit, "trade_date"],
                        "created_at": datetime.utcnow(),
                        "kline": model_df.to_dict("records")
                    }
                    self.models_col.insert_one(doc)

                # 更新进度到下一个股票
                self._set_progress(idx + 1)
                # 为了防止接口限流，可适当 sleep
                time.sleep(0.2)

            except Exception as e:
                print(f"Error on {ts_code}: {e}")
                # 先保存进度再 raise，避免进度丢失
                self._set_progress(idx)
                raise

        print("All stocks processed.")


if __name__ == "__main__":
    BUILDER = ExplosionModelBuilder(
        tushare_token=os.getenv("TUSHARE_TOKEN"),
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        db_name="quant_models"
    )
    BUILDER.build()
