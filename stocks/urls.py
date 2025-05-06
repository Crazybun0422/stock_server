#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time : 2025/5/3 12:36
# @Author：Malcolm
# @File : urls.py
# @Software: PyCharm


from django.urls import path
from .views import CandidateView

urlpatterns = [
    path('api/<str:action>/', CandidateView.as_view(), name='candidates'),
]

