"""业务路由汇总：批量事件固定路由先注册，其余模块共用资源路由。"""
from __future__ import annotations

from app.routers import events, resource

ROUTERS = [events, resource]
