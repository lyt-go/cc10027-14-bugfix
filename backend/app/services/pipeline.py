"""跨模块数据流管道：协调多个服务执行完整的数据处理工作流。"""
from __future__ import annotations

import time
from typing import Any

from app.services.log_source import LogSourceService
from app.services.log_parser import LogParserService
from app.services.field_mapping import FieldMappingService
from app.services.format_output import FormatOutputService
from app.services.event_tracker import EventTrackerService
from app.services.event_validate import EventValidateService
from app.services.stream_buffer import StreamBufferService
from app.services.agent_heartbeat import AgentHeartbeatService
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.backfill_range import BackfillRangeService
from app.services.log_rotate import LogRotateService
from app.services.log_partition import LogPartitionService
from app.services.enrich_join import EnrichJoinService
from app.services.aggregate_olap import AggregateOlapService
from app.services.alert_rule import AlertRuleService
from app.services.dedup_window import DedupWindowService
from app.services.sampling_rule import SamplingRuleService
from app.services.log_quality import LogQualityService
from app.store import store


class CrossModulePipeline:
    """协调所有数据工程服务的跨模块工作流管道。"""

    def __init__(self):
        self.log_source = LogSourceService()
        self.log_parser = LogParserService()
        self.field_mapping = FieldMappingService()
        self.format_output = FormatOutputService()
        self.event_tracker = EventTrackerService()
        self.event_validate = EventValidateService()
        self.stream_buffer = StreamBufferService()
        self.agent_heartbeat = AgentHeartbeatService()
        self.collect_checkpoint = CollectCheckpointService()
        self.backfill_range = BackfillRangeService()
        self.log_rotate = LogRotateService()
        self.log_partition = LogPartitionService()
        self.enrich_join = EnrichJoinService()
        self.aggregate_olap = AggregateOlapService()
        self.alert_rule = AlertRuleService()
        self.dedup_window = DedupWindowService()
        self.sampling_rule = SamplingRuleService()
        self.log_quality = LogQualityService()

    def full_collect_pipeline(self, source_id: int = 0) -> dict[str, Any]:
        """完整采集管道：采集 -> 解析 -> 映射 -> 输出。"""
        started_at = time.time()

        active_sources = self.log_source.get_active_sources()
        if not active_sources:
            return {"error": "无活跃日志源", "status": "no_sources"}

        if source_id > 0:
            active_sources = [s for s in active_sources if s.get("id") == source_id]
            if not active_sources:
                return {"error": f"日志源 {source_id} 未接入", "status": "not_found"}

        results = {"pipeline": "full_collect", "stages": {}}

        for source in active_sources:
            key = source["源编号"]
            source_results = {}

            # 阶段1: 采集日志
            collect_r = self.log_source.collect_from_source(source["id"])
            source_results["collect"] = collect_r

            # 阶段2: 解析日志
            parse_r = self.log_parser.parse_log_file(key)
            source_results["parse"] = parse_r

            # 阶段3: 字段映射
            map_r = self.field_mapping.apply_mappings(key)
            source_results["mapping"] = map_r

            # 阶段4: 格式输出
            export_r = self.format_output.export_source(key, "jsonl")
            source_results["output"] = export_r

            results["stages"][key] = source_results

        results["duration"] = round(time.time() - started_at, 2)
        results["sources_processed"] = len(active_sources)
        return results

    def event_tracking_pipeline(
        self,
        event_codes: list[str],
        payloads: list[dict[str, Any] | None] | None = None,
        submission_id: str | None = None,
        *,
        interrupt_after: int | None = None,
    ) -> dict[str, Any]:
        """事件追踪管道：逐项受理 -> 缓冲刷新 -> 校验。

        受理交给 ``EventTrackerService.submit_batch``，按位置生成受理明细；
        编号和载荷数量不一致时不会整段丢掉尾部，重复编号也按各自位置分别入库。
        重送同一个 ``submission_id`` 时只处理未完成位置，不会重复写事件数据或缓冲。
        """
        started_at = time.time()
        results: dict[str, Any] = {"pipeline": "event_tracking", "stages": {}}

        submitted_payloads: list[dict[str, Any] | None] | None
        if payloads is None:
            submitted_payloads = [
                {"page_view": "home", "user_action": "click"} for _ in event_codes
            ]
        else:
            submitted_payloads = list(payloads)

        receipt = self.event_tracker.submit_batch(
            event_codes,
            submitted_payloads,
            submission_id,
            interrupt_after=interrupt_after,
        )
        results["submission_id"] = receipt["submission_id"]
        results["receipt"] = receipt

        # 只缓冲本次真正新落盘的位置：重送时已受理项不会再次入缓冲。
        newly_stored = [
            item for item in receipt.get("items", [])
            if item.get("last_action") == "stored"
        ]
        buffered = {}
        for item in newly_stored:
            buffer_r = self.stream_buffer.buffer_event(
                item["event_code"],
                {
                    "event_data": item.get("payload"),
                    "record_id": item.get("record_id"),
                    "submission_id": receipt["submission_id"],
                    "item_index": item.get("index"),
                },
            )
            buffered[item["index"]] = buffer_r

        flush_r = self.stream_buffer.flush_all()

        # 按输入位置记录阶段结果；同一事件编号的多次浏览不会互相覆盖。
        for item in receipt.get("items", []):
            index = item.get("index")
            results["stages"][f"item-{index}"] = {
                "index": index,
                "event_code": item.get("event_code"),
                "state": item.get("state"),
                "stored": bool(item.get("stored")),
                "reason": item.get("last_reason"),
                "buffer": buffered.get(index),
            }

        results["buffer_flush"] = flush_r
        results["duration"] = round(time.time() - started_at, 2)
        results["events_submitted"] = max(len(event_codes), len(submitted_payloads))
        results["events_processed"] = results["events_submitted"]
        results["events_stored"] = receipt.get("summary", {}).get("stored", 0)
        results["events_pending"] = receipt.get("summary", {}).get("pending", 0)
        return results

    def backfill_pipeline(self, source_key: str, time_range: str) -> dict[str, Any]:
        """回溯采集管道：创建回溯任务 -> 执行采集 -> 位点管理。"""
        started_at = time.time()

        task = self.backfill_range.create_backfill(source_key, time_range)
        if "error" in task:
            return task

        task_id = task.get("任务编号", "")

        # 分批执行回溯
        total_written = 0
        batches = 3
        for _ in range(batches):
            result = self.backfill_range.execute_backfill(task_id, batch_size=20)
            total_written += result.get("written", 0)

        # 完成回溯
        complete_r = self.backfill_range.complete_backfill(task_id)

        # 更新位点
        cp_state = self.collect_checkpoint.get_checkpoint_state(source_key)

        return {
            "pipeline": "backfill",
            "task_id": task_id,
            "source_key": source_key,
            "time_range": time_range,
            "total_written": total_written,
            "checkpoint": cp_state,
            "duration": round(time.time() - started_at, 2),
        }

    def quality_alert_pipeline(self, source_key: str) -> dict[str, Any]:
        """质量诊断+告警管道：诊断 -> 评估 -> 触发告警 -> 位点回滚。"""
        started_at = time.time()
        results = {"pipeline": "quality_alert", "stages": {}}

        # 阶段1: 质量诊断
        quality_r = self.log_quality.diagnose_source(source_key)
        results["stages"]["diagnose"] = quality_r

        # 阶段2: 告警评估
        alert_r = self.alert_rule.evaluate_rules(source_key, {
            "parse_success_rate": quality_r.get("parse_success_rate", 100),
            "field_completeness": quality_r.get("field_completeness", {}).get("overall", 100),
            "source_key": source_key,
        })
        results["stages"]["alert"] = alert_r

        # 阶段3: 如果告警触发，回滚采集位点
        if alert_r.get("alerts_fired", 0) > 0:
            cp_state = self.collect_checkpoint.get_checkpoint_state(source_key)
            if cp_state and cp_state.get("offset", 0) > 0:
                rollback_r = self.collect_checkpoint.rollback_to(source_key, 0)
                results["stages"]["rollback"] = rollback_r

        results["duration"] = round(time.time() - started_at, 2)
        return results

    def enrichment_aggregation_pipeline(self, source_key: str) -> dict[str, Any]:
        """富化+聚合管道：构建维表 -> 富化 -> 聚合 -> 告警评估。"""
        started_at = time.time()
        results = {"pipeline": "enrichment_aggregation", "stages": {}}

        # 阶段1: 确保维表缓存已构建
        enrich_rules = [r for r in store.rows("enrich_join") if r.get("status") == "已生效"]
        for rule in enrich_rules:
            self.enrich_join.build_dimension_cache(rule["规则编号"])

        # 阶段2: 维表富化
        enrich_r = self.enrich_join.enrich_source(source_key)
        results["stages"]["enrich"] = enrich_r

        # 阶段3: 聚合计算
        aggregate_r = self.aggregate_olap.aggregate_source(source_key)
        results["stages"]["aggregate"] = aggregate_r

        # 阶段4: OLAP立方体
        olap_r = self.aggregate_olap.compute_olap_cube(source_key)
        results["stages"]["olap"] = olap_r

        # 阶段5: 告警评估
        alert_r = self.alert_rule.evaluate_rules(source_key)
        results["stages"]["alert"] = alert_r

        results["duration"] = round(time.time() - started_at, 2)
        return results

    def dedup_sample_pipeline(self, source_key: str) -> dict[str, Any]:
        """去重+采样管道：窗口去重 -> 采样 -> 格式化输出。"""
        started_at = time.time()
        results = {"pipeline": "dedup_sample", "stages": {}}

        # 阶段1: 窗口去重
        dedup_r = self.dedup_window.deduplicate_source(source_key)
        results["stages"]["dedup"] = dedup_r

        # 阶段2: 采样
        sample_r = self.sampling_rule.sample_source(source_key)
        results["stages"]["sample"] = sample_r

        # 阶段3: 导出采样结果
        export_r = self.format_output.export_source(source_key, "jsonl")
        results["stages"]["output"] = export_r

        results["duration"] = round(time.time() - started_at, 2)
        return results

    def rotate_partition_pipeline(self, source_key: str) -> dict[str, Any]:
        """轮转+分区管道：轮转日志 -> 分区路由 -> 归并。"""
        started_at = time.time()
        results = {"pipeline": "rotate_partition", "stages": {}}

        # 需要先有采集数据
        log_file = self.log_source._data_dir("logs") / f"{source_key}.jsonl"
        if not log_file.exists():
            return {"error": f"日志源 {source_key} 无采集数据，请先执行采集管道"}

        # 阶段1: 先解析产生分区所需数据
        parse_r = self.log_parser.parse_log_file(source_key)
        results["stages"]["parse"] = parse_r

        # 阶段2: 日志轮转
        rotate_r = self.log_rotate.rotate_source(source_key)
        results["stages"]["rotate"] = rotate_r

        # 阶段3: 分区路由
        partition_r = self.log_partition.partition_source(source_key)
        results["stages"]["partition"] = partition_r

        # 阶段4: 分区归并
        merge_r = self.log_partition.merge_partitions(source_key)
        results["stages"]["merge"] = merge_r

        results["duration"] = round(time.time() - started_at, 2)
        return results

    def agent_checkpoint_pipeline(self, agent_code: str) -> dict[str, Any]:
        """代理心跳+位点管道：心跳上报 -> 位点同步 -> 离线检测。"""
        started_at = time.time()
        results = {"pipeline": "agent_checkpoint", "stages": {}}

        # 阶段1: 代理心跳上报
        heartbeat_r = self.agent_heartbeat.send_heartbeat(agent_code, {
            "collected": 1500,
            "error_rate": 0.02,
            "memory_mb": 256,
        })
        results["stages"]["heartbeat"] = heartbeat_r

        # 阶段2: 保存位点
        cp_r = self.collect_checkpoint.save_checkpoint(agent_code, 4096, 200)
        results["stages"]["checkpoint"] = cp_r

        # 阶段3: 离线检测
        offline = self.agent_heartbeat.detect_offline_agents()
        results["stages"]["offline_detection"] = {"offline_count": len(offline), "agents": offline}

        results["duration"] = round(time.time() - started_at, 2)
        return results

    def run_all_pipelines(self) -> dict[str, Any]:
        """运行所有管道组合。"""
        started_at = time.time()
        results = {"full_run": True, "pipelines": {}}

        active_sources = self.log_source.get_active_sources()
        if not active_sources:
            return {"error": "无活跃日志源，请先接入日志源"}

        first_key = active_sources[0]["源编号"]

        results["pipelines"]["collect"] = self.full_collect_pipeline()
        results["pipelines"]["enrich_aggregate"] = self.enrichment_aggregation_pipeline(first_key)
        results["pipelines"]["dedup_sample"] = self.dedup_sample_pipeline(first_key)
        results["pipelines"]["quality_alert"] = self.quality_alert_pipeline(first_key)
        results["pipelines"]["rotate_partition"] = self.rotate_partition_pipeline(first_key)

        results["duration"] = round(time.time() - started_at, 2)
        results["sources"] = [s["源编号"] for s in active_sources]
        return results


pipeline = CrossModulePipeline()