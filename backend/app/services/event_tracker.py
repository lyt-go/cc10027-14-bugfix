"""事件埋点：管理埋点事件定义、触发上报和事件数据持久化。

批量上报以“逐项受理明细”（submission receipt）为唯一事实来源：
每个输入位置都会得到一条受理记录，要么已经落盘，要么留下可追查的待补项。
调用方重送时沿用同一个提交凭据（submission_id），已受理的项不会再次入库。
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 同一进程内的提交可能来自资源路由和跨模块管道两个服务实例，锁放在模块级别。
_BATCH_LOCK = threading.RLock()

# 尚未产生“已落盘”结果的受理状态。
PENDING_STATES = {
    "pending",
    "missing_payload",
    "missing_event_code",
    "writing",
    "error",
    "rejected",
}


class BatchSubmissionInterrupted(RuntimeError):
    """模拟调用方超时/响应中断；此时已处理项已经落盘，可用原凭据重送。"""

    def __init__(self, submission_id: str, receipt: dict[str, Any]) -> None:
        super().__init__(f"批量事件提交 {submission_id} 响应中断")
        self.submission_id = submission_id
        self.receipt = receipt


class EventTrackerService(BaseModuleService):
    MODULE = "event_tracker"
    ENTITY = "埋点事件"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["事件编号", "事件名称", "埋点页面", "触发条件", "上报字段", "采样率", "事件版本", "生效状态"]
    REQUIRED_FIELDS = ["事件编号", "事件名称", "埋点页面"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用事件": "已停用"}
    NEGATIVE_ACTIONS = ["停用事件"]

    # -- 事件定义与单条上报 --

    def activate_event(self, entry_id: int) -> dict[str, Any]:
        """生效事件并自动创建配套校验规则。"""
        entry, msg = self.run_action(entry_id, "标记生效")
        if entry is None:
            return {"error": msg, "entry_id": entry_id}
        event_code = entry["事件编号"]

        triggers = {}
        # 自动创建配套event_validate校验规则
        try:
            validate_entry = self._auto_create_seed("event_validate", {
                "规则编号": f"VALIDATE-{event_code}",
                "关联事件": event_code,
                "字段检查": event_code,
                "格式约束": "auto_format",
                "取值边界": "auto_bound",
                "采样校验": "全量",
                "阻断动作": "warn",
                "规则版本": "v1.0",
            })
            if validate_entry:
                triggers["event_validate"] = {"created": validate_entry["规则编号"], "status": "已生效"}
        except Exception as e:
            triggers["event_validate"] = {"error": str(e)}

        # 自动触发事件校验
        validate_r = self._trigger_validation_for_event(event_code)
        if "error" not in validate_r:
            triggers["validation"] = validate_r

        return {
            "event_entry": entry,
            "event_code": event_code,
            "activated_at": time.time(),
            "auto_triggers": triggers,
        }

    def track_event(self, event_code: str, payload: dict[str, Any]) -> dict[str, Any]:
        """触发一次事件埋点上报（单条入口）。"""
        event = self._find_active_event(event_code)
        if event is None:
            return {"error": f"事件 {event_code} 未生效或不存在"}

        record = self._build_record(
            event=event,
            event_code=event_code,
            payload=payload,
            record_id=f"SINGLE-{uuid.uuid4().hex}",
            submission_id=None,
            item_index=None,
            source="single",
        )
        self._append_event_record(event_code, record)

        cp = self._read_checkpoint(f"event_{event_code}") or {"count": 0}
        cp["count"] = cp.get("count", 0) + 1
        cp["last_fired"] = time.time()
        self._write_checkpoint(f"event_{event_code}", cp)

        result = {"status": "tracked", "event_code": event_code, "count": cp["count"]}

        # 事后自动触发校验
        validate_r = self._trigger_validation_for_event(event_code)
        if "error" not in validate_r:
            result["auto_validate"] = validate_r

        return result

    # -- 批量逐项受理 --

    def submit_batch(
        self,
        event_codes: list[str],
        payloads: list[dict[str, Any] | None] | None,
        submission_id: str | None = None,
        *,
        auto_validate: bool = True,
        interrupt_after: int | None = None,
    ) -> dict[str, Any]:
        """逐项受理一批事件上报并按位置生成受理明细。

        ``event_codes`` 与 ``payloads`` 按位置配对，不要求等长：
        - 某个位置缺少载荷时保留为 ``missing_payload`` 待补项，不会从明细里消失；
        - 相同事件编号出现在多个位置时，每个位置都是独立的一次浏览/上报；
        - 同一 ``submission_id`` 重送时，已受理的位置直接跳过，只受理未完成项。

        ``interrupt_after`` 仅用于模拟首批写入后响应中断：写完前 N 个位置后抛出
        :class:`BatchSubmissionInterrupted`，已写入项及待补项均已落盘。
        """
        codes = [str(code) if code is not None else None for code in (event_codes or [])]
        submitted_payloads: list[Any] = list(payloads) if payloads is not None else []
        position_count = max(len(codes), len(submitted_payloads))

        with _BATCH_LOCK:
            submission_id = submission_id or self._new_submission_id()
            receipt = self._load_submission(submission_id)
            attempt = 1
            if receipt is None:
                receipt = {
                    "submission_id": submission_id,
                    "created_at": time.time(),
                    "attempts": 0,
                    "items": [],
                }
            else:
                attempt = int(receipt.get("attempts", 0)) + 1

            receipt["attempts"] = attempt
            receipt["updated_at"] = time.time()
            receipt["last_submitted_at"] = time.time()

            # 先把本次提交涉及的所有位置登记下来，再逐项受理。
            # 这样即使进程在逐项处理途中退出，每个位置也都有落盘或待补结果。
            self._ensure_positions(receipt, codes, submitted_payloads, position_count)
            receipt["last_attempt"] = {
                "attempt": attempt,
                "stored": 0,
                "already_stored": 0,
                "pending": 0,
                "rejected": 0,
                "conflicts": 0,
                "interrupted": False,
            }
            self._save_submission(receipt)

            affected_codes: set[str] = set()
            for index in range(position_count):
                item = receipt["items"][index]
                item["last_seen_attempt"] = attempt

                code = codes[index] if index < len(codes) else None
                payload_present = (
                    index < len(submitted_payloads)
                    and submitted_payloads[index] is not None
                )
                payload = submitted_payloads[index] if payload_present else None

                action = self._accept_item(
                    receipt, item, code, payload, payload_present, attempt)
                item["last_action"] = action

                if action == "stored":
                    receipt["last_attempt"]["stored"] += 1
                    if item.get("event_code"):
                        affected_codes.add(item["event_code"])
                    if interrupt_after is not None and \
                            receipt["last_attempt"]["stored"] >= interrupt_after:
                        receipt["last_attempt"]["interrupted"] = True
                        self._refresh_summary(receipt)
                        self._save_submission(receipt)
                        raise BatchSubmissionInterrupted(submission_id, receipt)
                elif action == "already_stored":
                    receipt["last_attempt"]["already_stored"] += 1
                elif action == "pending":
                    receipt["last_attempt"]["pending"] += 1
                elif action == "rejected":
                    receipt["last_attempt"]["rejected"] += 1
                elif action == "conflict":
                    receipt["last_attempt"]["conflicts"] += 1

            self._refresh_summary(receipt)
            self._save_submission(receipt)
            response = self._public_receipt(receipt)

        if auto_validate and affected_codes:
            validations = {}
            for code in sorted(affected_codes):
                validate_r = self._trigger_validation_for_event(code)
                if "error" not in validate_r:
                    validations[code] = validate_r
            if validations:
                response["auto_validate"] = validations
        return response

    # 兼容不同调用方对“批量上报”的命名习惯。
    def accept_batch_events(
        self,
        event_codes: list[str],
        payloads: list[dict[str, Any] | None] | None = None,
        submission_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return self.submit_batch(event_codes, payloads, submission_id, **kwargs)

    def get_submission_receipt(self, submission_id: str) -> dict[str, Any] | None:
        """按提交凭据读取逐项受理明细。"""
        receipt = self._load_submission(submission_id)
        if receipt is None:
            return None
        self._refresh_summary(receipt)
        return self._public_receipt(receipt)

    def list_submissions(self) -> list[dict[str, Any]]:
        """列出所有批量提交的受理明细，便于追查缺项。"""
        receipts = []
        for path in sorted(self._submission_dir().glob("sub_*.json")):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    receipt = json.load(f)
                self._refresh_summary(receipt)
                receipts.append(self._public_receipt(receipt))
            except (OSError, json.JSONDecodeError):
                continue
        return receipts

    # -- 统计 --

    def get_event_stats(self, event_code: str) -> dict[str, Any]:
        """获取事件埋点统计数据。

        入库次数直接来自事件文件里的实际记录条数，不受“提交了几个编号”影响；
        缺载荷/未完成的位置从各自的提交受理明细中汇总，可沿 submission_id 追查。
        """
        events_file = self._data_dir("events") / f"{event_code}.jsonl"
        records = self._read_jsonl(events_file) if events_file.exists() else []

        missing_items: list[dict[str, Any]] = []
        submission_ids: set[str] = set()
        batch_stored = 0
        for receipt in self.list_submissions():
            for item in receipt.get("items", []):
                if item.get("event_code") != event_code:
                    continue
                submission_ids.add(receipt["submission_id"])
                if item.get("stored"):
                    batch_stored += 1
                elif item.get("state") in PENDING_STATES:
                    missing_items.append({
                        "submission_id": receipt["submission_id"],
                        "index": item.get("index"),
                        "event_code": item.get("event_code"),
                        "state": item.get("state"),
                        "reason": item.get("last_reason") or item.get("state"),
                        "payload_present": bool(item.get("payload_present")),
                        "attempts": item.get("attempts", 0),
                    })

        fired_at = [rec.get("tracked_at") for rec in records if rec.get("tracked_at") is not None]
        return {
            "event_code": event_code,
            "total_fired": len(records),
            "stored_count": len(records),
            "batch_stored_count": batch_stored,
            "submission_count": len(submission_ids),
            "pending_count": len(missing_items),
            "missing_items": missing_items,
            "first_fired": min(fired_at) if fired_at else None,
            "last_fired": max(fired_at) if fired_at else None,
        }

    # -- 受理明细内部实现 --

    def _accept_item(
        self,
        receipt: dict[str, Any],
        item: dict[str, Any],
        code: str | None,
        payload: dict[str, Any] | None,
        payload_present: bool,
        attempt: int,
    ) -> str:
        # 已受理项：重试只确认，不再写第二遍。
        if item.get("state") == "accepted":
            if code is not None and item.get("event_code") != code:
                item["conflict"] = True
                item["last_reason"] = (
                    f"该位置已受理，重试事件编号与原提交不一致，保留原记录："
                    f"{item.get('event_code')} -> {code}")
                return "conflict"
            if payload_present and self._payload_fingerprint(payload) != item.get("payload_fingerprint"):
                item["conflict"] = True
                item["last_reason"] = "该位置已受理，重试载荷与原提交不一致，保留原记录"
                return "conflict"
            item["last_reason"] = "重试命中已受理项，未重复入库"
            return "already_stored"

        # 上次在“写盘前/写盘后”中断：以 record_id 回查事件文件，避免重复写入。
        if item.get("state") == "writing":
            record_id = item.get("record_id")
            existing_code = item.get("event_code")
            if record_id and existing_code and self._record_exists(existing_code, record_id):
                item["state"] = "accepted"
                item["stored"] = True
                item["accepted_at"] = item.get("accepted_at") or time.time()
                item["last_reason"] = "上次写入实际成功，重试仅确认"
                return "already_stored"

        # 事件编号在首次出现后固定到位置上，重送时不允许悄悄换号。
        if code is not None:
            if item.get("event_code") and item["event_code"] != code:
                item["conflict"] = True
                item["last_reason"] = (
                    f"同一位置的事件编号与原提交不一致：{item['event_code']} -> {code}")
                item["state"] = item.get("state") or "pending"
                return "conflict"
            item["event_code"] = code
        else:
            code = item.get("event_code")

        if not payload_present:
            item["state"] = "missing_payload"
            item["stored"] = False
            item["last_reason"] = "本次提交缺少载荷，已登记为待补项"
            return "pending"

        item["payload"] = payload
        item["payload_present"] = True
        item["payload_fingerprint"] = self._payload_fingerprint(payload)

        if not code:
            item["state"] = "missing_event_code"
            item["stored"] = False
            item["last_reason"] = "该位置有载荷但缺少事件编号，已登记为待补项"
            return "pending"

        event = self._find_active_event(code)
        if event is None:
            item["state"] = "rejected"
            item["stored"] = False
            item["last_reason"] = f"事件 {code} 未生效或不存在"
            return "rejected"

        record_id = self._record_id(receipt["submission_id"], item["index"])

        # 先落“写入中”状态，再写事件数据；无论响应是否中断，重试都能据此判断。
        item["state"] = "writing"
        item["record_id"] = record_id
        item["attempts"] = int(item.get("attempts", 0)) + 1
        item["last_reason"] = "写入中"
        self._save_submission(receipt)

        record = self._build_record(
            event=event,
            event_code=code,
            payload=payload,
            record_id=record_id,
            submission_id=receipt["submission_id"],
            item_index=item["index"],
            source="batch",
        )
        try:
            self._append_event_record(code, record)
        except OSError as exc:
            item["state"] = "error"
            item["stored"] = False
            item["last_reason"] = f"写入失败：{exc}"
            return "pending"

        item["state"] = "accepted"
        item["stored"] = True
        item["accepted_at"] = time.time()
        item["accepted_attempt"] = attempt
        item["last_reason"] = "已落盘"
        self._save_submission(receipt)
        return "stored"

    def _ensure_positions(
        self,
        receipt: dict[str, Any],
        codes: list[str | None],
        payloads: list[Any],
        position_count: int,
    ) -> None:
        items: list[dict[str, Any]] = receipt.setdefault("items", [])
        while len(items) < position_count:
            index = len(items)
            code = codes[index] if index < len(codes) else None
            payload_present = index < len(payloads) and payloads[index] is not None
            items.append({
                "index": index,
                "event_code": code,
                "payload": payloads[index] if payload_present else None,
                "payload_present": payload_present,
                "payload_fingerprint": (
                    self._payload_fingerprint(payloads[index]) if payload_present else None
                ),
                "state": "pending",
                "stored": False,
                "record_id": None,
                "attempts": 0,
                "last_action": "pending",
                "last_reason": "等待受理",
                "conflict": False,
                "accepted_at": None,
                "accepted_attempt": None,
            })

    def _refresh_summary(self, receipt: dict[str, Any]) -> None:
        items = receipt.get("items", [])
        by_state: dict[str, int] = {}
        for item in items:
            state = str(item.get("state") or "pending")
            by_state[state] = by_state.get(state, 0) + 1
        stored = sum(1 for item in items if item.get("stored"))
        receipt["summary"] = {
            "total": len(items),
            "stored": stored,
            "accepted": stored,
            "pending": len(items) - stored,
            "missing_payload": by_state.get("missing_payload", 0),
            "missing_event_code": by_state.get("missing_event_code", 0),
            "rejected": by_state.get("rejected", 0),
            "error": by_state.get("error", 0),
            "writing": by_state.get("writing", 0),
            "conflicts": sum(1 for item in items if item.get("conflict")),
        }

    def _public_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        items = receipt.get("items", [])
        return {
            **receipt,
            "summary": receipt.get("summary", {}),
            "missing_items": [
                {
                    "index": item.get("index"),
                    "event_code": item.get("event_code"),
                    "state": item.get("state"),
                    "reason": item.get("last_reason"),
                }
                for item in items
                if not item.get("stored")
            ],
        }

    # -- 文件与记录工具 --

    def _submission_dir(self) -> Path:
        return self._data_dir("events") / "submissions"

    def _submission_path(self, submission_id: str) -> Path:
        digest = hashlib.sha256(submission_id.encode("utf-8")).hexdigest()[:16]
        return self._submission_dir() / f"sub_{digest}.json"

    def _load_submission(self, submission_id: str) -> dict[str, Any] | None:
        path = self._submission_path(submission_id)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                receipt = json.load(f)
        except (OSError, json.JSONDecodeError):
            return None
        # 文件名是摘要，真正凭据以文件内容为准；二者不一致说明不是同一提交。
        if receipt.get("submission_id") != submission_id:
            return None
        return receipt

    def _save_submission(self, receipt: dict[str, Any]) -> Path:
        receipt["updated_at"] = time.time()
        path = self._submission_path(receipt["submission_id"])
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(receipt, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, path)
        return path

    def _find_active_event(self, event_code: str) -> dict[str, Any] | None:
        for event in store.rows(self.MODULE):
            if event.get("事件编号") == event_code and event.get("status") == "已生效":
                return event
        return None

    def _build_record(
        self,
        *,
        event: dict[str, Any],
        event_code: str,
        payload: dict[str, Any] | None,
        record_id: str,
        submission_id: str | None,
        item_index: int | None,
        source: str,
    ) -> dict[str, Any]:
        return {
            "record_id": record_id,
            "submission_id": submission_id,
            "item_index": item_index,
            "source": source,
            "event_code": event_code,
            "event_name": event["事件名称"],
            "page": event["埋点页面"],
            "payload": payload,
            "tracked_at": time.time(),
            "event_version": event.get("事件版本", "v1"),
        }

    def _append_event_record(self, event_code: str, record: dict[str, Any]) -> None:
        output_file = self._data_dir("events") / f"{event_code}.jsonl"
        self._write_jsonl(output_file, [record])

    def _record_exists(self, event_code: str, record_id: str) -> bool:
        events_file = self._data_dir("events") / f"{event_code}.jsonl"
        return any(
            rec.get("record_id") == record_id
            for rec in self._read_jsonl(events_file)
        )

    @staticmethod
    def _record_id(submission_id: str, index: int) -> str:
        return f"{submission_id}#{index}"

    @staticmethod
    def _payload_fingerprint(payload: Any) -> str:
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _new_submission_id() -> str:
        stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
        return f"SUB-{stamp}-{uuid.uuid4().hex[:8]}"
