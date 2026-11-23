"""业务路由汇总：孤儿巡检等专用路由先注册，通用资源路由兜底动态模块路径。"""
from __future__ import annotations

from app.routers import orphan_review, resource

# 专用路由必须排在通用 /api/{module} 动态路由之前，否则固定路径会被抢先匹配。
ROUTERS = [orphan_review, resource]
