#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/3 12:36
# @Author：Malcolm
# @File : urls.py
# @Software: PyCharm


from django.urls import path
from .views import CandidateView, start_explosion, pause_explosion, list_explosions, board_stocks

urlpatterns = [
    path('api/<str:action>/', CandidateView.as_view(), name='candidates'),
    path('board/', board_stocks, name='board-stocks'),
    path('explosion/start/', start_explosion, name='explosion-start'),
    path('explosion/pause/', pause_explosion, name='explosion-pause'),
    path('explosion/', list_explosions, name='explosion-list'),
]

