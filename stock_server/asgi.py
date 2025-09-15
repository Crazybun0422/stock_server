"""ASGI config for stock_server project."""

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
import stocks.routing

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stock_server.settings')

application = ProtocolTypeRouter({
    'http': get_asgi_application(),
    'websocket': URLRouter(
        stocks.routing.websocket_urlpatterns
    ),
})
