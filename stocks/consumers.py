#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/5 14:26
# @Author：Malcolm
# @File : consumers.py
# @Software: PyCharm
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from stocks.services import CandidateService


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
