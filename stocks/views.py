# stocks/views.py

import json

from django.http import JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseNotFound
from django.views import View
from django.views.decorators.http import require_POST, require_GET
from django.core.paginator import Paginator

from stock_server import settings
from celery.result import AsyncResult
import pymongo

from .services import (
    CandidateService,
    get_board_stocks,
    BOARD_MAIN,
    BOARD_GEM,
    BOARD_STAR,
    BOARD_ST,
    BOARD_BSE,
)
from .tasks import build_explosion_patterns

_task_id: str | None = None


class CandidateView(View):
    """
    支持 GET 和 POST 两种方式：
      - GET: 通过 query string 传 days，如 /api/candidates/?days=200
      - POST: 以 JSON 体传 {"days":200}
    返回波段底部候选股票的 JSON 列表。
    """

    def get(self, request: HttpRequest, action) -> HttpResponseBadRequest | JsonResponse:
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


@require_GET
def board_stocks(request: HttpRequest) -> JsonResponse | HttpResponseBadRequest:
    """Return paginated stock info for a given board."""
    board = request.GET.get('board')
    page = int(request.GET.get('page', 1))
    size = int(request.GET.get('page_size', 20))
    if board not in {BOARD_MAIN, BOARD_GEM, BOARD_STAR, BOARD_ST, BOARD_BSE}:
        return HttpResponseBadRequest('invalid board')
    data = get_board_stocks(board)
    paginator = Paginator(data, size)
    page_obj = paginator.get_page(page)
    return JsonResponse(
        {
            'total': paginator.count,
            'page': page,
            'page_size': size,
            'items': list(page_obj),
        },
        json_dumps_params={'ensure_ascii': False},
    )


@require_POST
def start_explosion(request: HttpRequest) -> JsonResponse:
    """启动生成起爆图的后台任务."""
    global _task_id
    result = build_explosion_patterns.delay()
    _task_id = result.id
    return JsonResponse({'task_id': _task_id})


@require_POST
def pause_explosion(request: HttpRequest) -> JsonResponse | HttpResponseBadRequest:
    """暂停当前运行的任务."""
    global _task_id
    try:
        body = json.loads(request.body.decode('utf-8')) if request.body else {}
    except json.JSONDecodeError:
        return HttpResponseBadRequest('请求体必须是合法 JSON')
    task_id = body.get('task_id') or _task_id
    if not task_id:
        return HttpResponseBadRequest('缺少 task_id')
    AsyncResult(task_id).revoke(terminate=True)
    return JsonResponse({'status': 'revoked', 'task_id': task_id})


@require_GET
def list_explosions(request: HttpRequest) -> JsonResponse:
    """分页返回已生成的起爆图."""
    page = int(request.GET.get('page', 1))
    size = int(request.GET.get('page_size', 10))
    mongo_client = pymongo.MongoClient(getattr(settings, 'MONGO_URI', 'mongodb://localhost:27017/'))
    db = mongo_client.get_default_database() or mongo_client['productdb']
    collection = db['explosion_patterns']
    total = collection.count_documents({})
    items = list(collection.find({}, {'_id': 0}).skip((page - 1) * size).limit(size))
    return JsonResponse({'total': total, 'page': page, 'page_size': size, 'items': items},
                        json_dumps_params={'ensure_ascii': False})
