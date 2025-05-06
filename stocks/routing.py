#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/5 14:26
# @Author：Malcolm
# @File : routing.py
# @Software: PyCharm
from django.urls import re_path
from stocks.consumers import CandidateConsumer

websocket_urlpatterns = [
    re_path(r'ws/stocks/$', CandidateConsumer.as_asgi()),
]