"""批量事件逐项受理明细的回归测试。

覆盖：编号与载荷不等长、重复事件编号、响应中断后按原凭据重送、
已受理项不重复入库、缺载荷项可补交，以及统计里的实际入库次数与缺项。
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.services.base import DATA_ROOT
from app.services.event_tracker import (
    BatchSubmissionInterrupted,
    EventTrackerService,
)
from app.services.pipeline import CrossModulePipeline
from app.store import store


def _reset(event_code: str, submission_ids: list[str] | None = None) -> None:
    """只清理测试事件自己的产物，避免影响其他模块的测试数据。"""
    event = next(
        (row for row in store.rows("event_tracker") if row.get("事件编号") == event_code),
        None,
    )
    if event is None:
        rows = store.rows("event_tracker")
        event = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "事件编号": event_code,
            "事件名称": f"批量受理测试事件 {event_code}",
            "埋点页面": "/test/batch",
            "触发条件": "test",
            "上报字段": "browse",
            "采样率": "100",
            "事件版本": "v1",
            "生效状态": "已生效",
            "pending": False,
            "abnormal": False,
        }
        rows.append(event)
    event["status"] = "已生效"

    events_dir = DATA_ROOT / "events"
    for name in (
        f"{event_code}.jsonl",
        f"{event_code}_validated.jsonl",
        f"{event_code}_stream.jsonl",
    ):
        path = events_dir / name
        if path.exists():
            path.unlink()

    for name in (f"event_{event_code}", f"stream_{event_code}", f"validate_{event_code}"):
        path = DATA_ROOT / "checkpoints" / f"{name}.json"
        if path.exists():
            path.unlink()

    submissions = events_dir / "submissions"
    if submission_ids is None:
        shutil.rmtree(submissions, ignore_errors=True)
    else:
        import hashlib
        for submission_id in submission_ids:
            digest = hashlib.sha256(submission_id.encode("utf-8")).hexdigest()[:16]
            path = submissions / f"sub_{digest}.json"
            if path.exists():
                path.unlink()

    # 缓冲服务是跨测试共享的实例，清掉这个事件的残留。
    from app.services.stream_buffer import StreamBufferService

    buffer = StreamBufferService()
    buffer._buffers.pop(event_code, None)


def _records(event_code: str) -> list[dict]:
    path = DATA_ROOT / "events" / f"{event_code}.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_batch_keeps_every_position_when_codes_and_payloads_differ():
    """三个编号两份载荷、其中两个编号相同：逐项受理，不丢位置也不合并浏览。"""
    event_code = "EVEN-BATCH-A"
    _reset(event_code)
    svc = EventTrackerService()

    codes = [event_code, event_code, "EVEN-MISSING"]
    payloads = [{"browse": 1}, {"browse": 2}]
    receipt = svc.submit_batch(codes, payloads, "SUB-BATCH-A", auto_validate=False)

    assert len(receipt["items"]) == 3
    assert [item["event_code"] for item in receipt["items"]] == codes
    assert [item["state"] for item in receipt["items"]] == [
        "accepted", "accepted", "missing_payload"]
    assert receipt["summary"]["stored"] == 2
    assert receipt["summary"]["pending"] == 1
    assert receipt["summary"]["missing_payload"] == 1

    records = _records(event_code)
    assert len(records) == 2, records
    assert {rec["payload"]["browse"] for rec in records} == {1, 2}
    assert len({rec["record_id"] for rec in records}) == 2


def test_retry_after_interruption_only_accepts_unfinished_items():
    """首批写入后响应中断，原凭据重送只补未完成项，已受理的两份载荷各记一次。"""
    event_code = "EVEN-BATCH-B"
    _reset(event_code)
    svc = EventTrackerService()

    codes = [event_code, event_code, "EVEN-MISSING"]
    payloads = [{"browse": 1}, {"browse": 2}]

    try:
        svc.submit_batch(codes, payloads, "SUB-BATCH-B", auto_validate=False, interrupt_after=1)
    except BatchSubmissionInterrupted as exc:
        assert exc.receipt["summary"]["stored"] == 1
        assert exc.receipt["summary"]["pending"] == 2
    else:
        raise AssertionError("首批应该在写入一项后中断")

    receipt = svc.submit_batch(codes, payloads, "SUB-BATCH-B", auto_validate=False)
    assert receipt["attempts"] == 2
    assert [item["last_action"] for item in receipt["items"]] == [
        "already_stored", "stored", "pending"]
    assert receipt["summary"]["stored"] == 2
    assert receipt["summary"]["missing_payload"] == 1

    records = _records(event_code)
    assert len(records) == 2, records
    assert sorted(rec["payload"]["browse"] for rec in records) == [1, 2]
    # 中断前已落盘的位置没有被重写。
    first = [item for item in receipt["items"] if item["index"] == 0][0]
    assert first["record_id"] == "SUB-BATCH-B#0"
    assert [rec["record_id"] for rec in records].count("SUB-BATCH-B#0") == 1


def test_replaying_completed_submission_does_not_duplicate_records():
    """已全部受理后再次重送同一凭据，只确认，不新增记录。"""
    event_code = "EVEN-BATCH-C"
    _reset(event_code)
    svc = EventTrackerService()

    codes = [event_code, event_code]
    payloads = [{"browse": 1}, {"browse": 2}]
    svc.submit_batch(codes, payloads, "SUB-BATCH-C", auto_validate=False)
    replay = svc.submit_batch(codes, payloads, "SUB-BATCH-C", auto_validate=False)

    assert replay["summary"]["stored"] == 2
    assert all(item["last_action"] == "already_stored" for item in replay["items"])
    assert len(_records(event_code)) == 2


def test_missing_payload_item_can_be_supplemented_under_same_submission():
    """缺载荷项保留在原位置，补交后同一凭据下只入库一次。"""
    event_code = "EVEN-BATCH-D"
    _reset(event_code)
    svc = EventTrackerService()

    first = svc.submit_batch(
        [event_code, event_code], [{"browse": 1}, None],
        "SUB-BATCH-D", auto_validate=False)
    assert first["items"][1]["state"] == "missing_payload"
    assert len(_records(event_code)) == 1

    second = svc.submit_batch(
        [event_code, event_code], [{"browse": 1}, {"browse": 2}],
        "SUB-BATCH-D", auto_validate=False)
    assert [item["last_action"] for item in second["items"]] == [
        "already_stored", "stored"]
    records = _records(event_code)
    assert len(records) == 2
    assert sorted(rec["payload"]["browse"] for rec in records) == [1, 2]

    replay = svc.submit_batch(
        [event_code, event_code], [{"browse": 1}, {"browse": 2}],
        "SUB-BATCH-D", auto_validate=False)
    assert len(_records(event_code)) == 2
    assert all(item["last_action"] == "already_stored" for item in replay["items"])


def test_stats_report_actual_stored_count_and_traceable_missing_items():
    """统计给出实际入库次数，缺载荷项能沿 submission_id 追查。"""
    event_code = "EVEN-BATCH-E"
    _reset(event_code)
    svc = EventTrackerService()

    svc.submit_batch(
        [event_code, event_code, event_code], [{"browse": 1}, {"browse": 2}],
        "SUB-BATCH-E", auto_validate=False)

    stats = svc.get_event_stats(event_code)
    assert stats["stored_count"] == 2
    assert stats["total_fired"] == 2
    assert stats["pending_count"] == 1
    assert stats["missing_items"] == [{
        "submission_id": "SUB-BATCH-E",
        "index": 2,
        "event_code": event_code,
        "state": "missing_payload",
        "reason": "本次提交缺少载荷，已登记为待补项",
        "payload_present": False,
        "attempts": 0,
    }]

    receipt = svc.get_submission_receipt("SUB-BATCH-E")
    assert receipt["submission_id"] == "SUB-BATCH-E"
    assert receipt["summary"]["stored"] == 2


def test_pipeline_reports_each_position_instead_of_collapsing_duplicates():
    """跨模块管道按位置记录阶段结果，同编号的两次浏览各自保留。"""
    event_code = "EVEN-BATCH-F"
    _reset(event_code)
    pipeline = CrossModulePipeline()

    result = pipeline.event_tracking_pipeline(
        [event_code, event_code, "EVEN-MISSING"],
        [{"browse": 1}, {"browse": 2}],
        "SUB-BATCH-F",
    )

    assert result["submission_id"] == "SUB-BATCH-F"
    assert result["events_submitted"] == 3
    assert result["events_stored"] == 2
    assert result["events_pending"] == 1
    # stages 以位置为键，原先按事件编号做键会把两次浏览合成一条。
    assert set(result["stages"]) == {"item-0", "item-1", "item-2"}
    assert result["stages"]["item-0"]["state"] == "accepted"
    assert result["stages"]["item-1"]["state"] == "accepted"
    assert result["stages"]["item-2"]["state"] == "missing_payload"
    assert len(_records(event_code)) == 2


def test_replayed_pipeline_does_not_rebuffer_or_rewrite():
    """管道重送同一凭据：事件与缓冲都不再重复写入。"""
    event_code = "EVEN-BATCH-G"
    _reset(event_code)
    pipeline = CrossModulePipeline()
    submission_id = "SUB-BATCH-G"
    codes = [event_code]
    payloads = [{"browse": 1}]

    first = pipeline.event_tracking_pipeline(codes, payloads, submission_id)
    second = pipeline.event_tracking_pipeline(codes, payloads, submission_id)

    assert first["events_stored"] == 1
    assert second["events_stored"] == 1  # 累计受理数，不是本次新增
    assert second["stages"]["item-0"]["state"] == "accepted"
    assert second["receipt"]["items"][0]["last_action"] == "already_stored"
    assert len(_records(event_code)) == 1

    stream_file = DATA_ROOT / "events" / f"{event_code}_stream.jsonl"
    stream_records = [
        json.loads(line) for line in stream_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ] if stream_file.exists() else []
    assert len(stream_records) == 1, stream_records


if __name__ == "__main__":
    failures = 0
    for name, func in sorted(globals().items()):
        if name.startswith("test_") and callable(func):
            try:
                func()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {exc!r}")
    sys.exit(1 if failures else 0)
