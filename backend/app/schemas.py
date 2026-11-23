"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class EventBatchPayload(BaseModel):
    """批量事件上报：编号与载荷按位置配对，允许不等长。"""

    submission_id: str | None = None
    event_codes: list[str | None] = Field(default_factory=list)
    payloads: list[dict[str, Any] | None] | None = None
    # 仅用于演练“写入若干项后响应中断”，便于验证原凭据重送的行为。
    interrupt_after: int | None = None
