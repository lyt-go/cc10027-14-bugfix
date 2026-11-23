"""批量事件上报接口：逐项受理、凭据重试和受理明细查询。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import EventBatchPayload
from app.services.event_tracker import BatchSubmissionInterrupted, EventTrackerService

router = APIRouter(prefix="/api", tags=["事件批量上报"])

# 这些固定路径必须先于通用资源路由注册，避免被 /api/{module} 抢先匹配。
service = EventTrackerService()


@router.post("/event-batches")
def submit_event_batch(payload: EventBatchPayload) -> dict[str, Any]:
    """逐项受理一批事件上报，返回每个位置的受理明细。"""
    try:
        return service.submit_batch(
            payload.event_codes,
            payload.payloads,
            payload.submission_id,
            interrupt_after=payload.interrupt_after,
        )
    except BatchSubmissionInterrupted as exc:
        raise HTTPException(
            status_code=504,
            detail={
                "message": str(exc),
                "submission_id": exc.submission_id,
                "receipt": exc.receipt,
            },
        )


@router.get("/event-batches")
def list_event_batches() -> dict[str, Any]:
    """列出全部批量提交的受理明细。"""
    receipts = service.list_submissions()
    return {"items": receipts, "total": len(receipts)}


@router.get("/event-batches/{submission_id}")
def get_event_batch(submission_id: str) -> dict[str, Any]:
    """按原提交凭据读取逐项受理明细。"""
    receipt = service.get_submission_receipt(submission_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"提交凭据 {submission_id} 不存在")
    return receipt


@router.get("/events/{event_code}/stats")
def get_event_stats(event_code: str) -> dict[str, Any]:
    """事件入库统计：实际入库次数与可追查的缺项。"""
    return service.get_event_stats(event_code)
