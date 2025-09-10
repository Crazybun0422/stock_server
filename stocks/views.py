# stocks/views.py

import json
import os

from django.http import JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseNotFound
from django.views import View

from stock_server import settings
from .services import CandidateService


class CandidateView(View):
    """
    支持 GET 和 POST 两种方式：
      - GET: 通过 query string 传 days，如 /api/candidates/?days=200
      - POST: 以 JSON 体传 {"days":200}
    返回波段底部候选股票的 JSON 列表。
    """

    def get(self, request: HttpRequest,action) -> HttpResponseBadRequest | JsonResponse:
        service = CandidateService()
        if action == 'candidates':
            data = service.get_candidates()
            # # 假设 test.json 放在 stocks 应用根目录
            # json_path = os.path.join(settings.BASE_DIR, 'stocks', 'test.json')
            # if not os.path.exists(json_path):
            #     return HttpResponseNotFound('test.json not found')
            #
            # with open(json_path, encoding='utf-8') as f:
            #     data = json.load(f)
            #
            # return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})
        elif action == 'ma5cross':
            data = service.get_ma5_cross_ma10()
        elif action == 'lianban-or-ma5cross':
            data = service.get_lianban_or_up_ma5()
        elif action == 'today-limit-kline':
            data = service.get_today_limit_with_kline()
        else:
            return HttpResponseBadRequest('未知的 action')

        return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})

    def post(self, request: HttpRequest) -> HttpResponseBadRequest | JsonResponse:
        try:
            body = json.loads(request.body.decode('utf-8'))
            days = body.get('days')
            lookback = int(days) if days is not None else None
        except (ValueError, json.JSONDecodeError):
            return HttpResponseBadRequest('请求体必须是合法 JSON，且 "days" 为整数')
        service = CandidateService(lookback_days=lookback) if lookback else CandidateService()
        data = service.get_candidates()
        return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})
