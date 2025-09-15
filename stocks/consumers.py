#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/5 14:26
# @Author：Malcolm
# @File : consumers.py
# @Software: PyCharm

import asyncio
from celery.result import AsyncResult
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from stocks.services import CandidateService
from stocks.tasks import build_explosion_patterns


class CandidateConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        # 接受连接
        await self.accept()
        await self.send_json({'message': 'Connected to Candidate WebSocket'})

    async def disconnect(self, close_code):
        # 断开清理
        pass

    async def receive_json(self, content):
        # 客户端发送：{'action':'start'}
        if content.get('action') == 'start':
            # 在此调用 CandidateService 并逐步发送进度

            service = CandidateService()
            await self.send_json({'message': 'Fetching candidates...'})
            # 同步调用（耗时），可考虑移到线程
            results = service.get_candidates()
            await self.send_json({'candidates': results})


class TrainingProgressConsumer(AsyncJsonWebsocketConsumer):
    """WebSocket consumer to report Celery training task progress."""

    async def connect(self):
        await self.accept()
        await self.send_json({'message': 'Connected to Training WebSocket'})
        self._result: AsyncResult | None = None

    async def receive_json(self, content, **kwargs):
        action = content.get('action')
        if action == 'start':
            self._result = build_explosion_patterns.delay()
            await self.send_json({'task_id': self._result.id})
            while not self._result.ready():
                meta = self._result.info or {}
                await self.send_json({
                    'progress': meta.get('current', 0),
                    'total': meta.get('total', 1),
                })
                await asyncio.sleep(1)
            meta = self._result.info or {}
            await self.send_json({'status': 'completed', **meta})
        elif action == 'status' and self._result is not None:
            meta = self._result.info or {}
            await self.send_json({
                'progress': meta.get('current', 0),
                'total': meta.get('total', 1),
            })
