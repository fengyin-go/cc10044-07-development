"""湖仓审计：Parquet 数据路径孤儿文件巡检闭环领域服务。

数据链路位置：数据审计层。覆盖「扫描核查单导出 -> 分批逐行回执导入 ->
当前快照引用约束 -> 清理计划生成 -> 审计留痕」的完整闭环。

领域对象（均存放在内存 store 的独立命名表，与 18 个业务模块互不干扰）：

* ``orphan_scan_batch``     扫描批次与扫描凭据（manifest、校验和、基线快照）
* ``orphan_candidate``      核查单条目：批次 + 文件全路径 + 判作孤儿的依据
* ``orphan_receipt``        逐行回执：按原批次与路径归档
* ``snapshot_reference``    当前快照引用登记（回执回传期间可能新增）
* ``cleanup_plan``          清理计划：从孤儿导出回执提取拟清理路径
* ``orphan_audit_event``    审计记录：每个处置动作都留痕

核心约束：

1. 核查单可以分几次回传，没收到回执的条目继续留在待核清单；
2. 可执行范围同时受「当前快照引用」和「回执反馈」约束——即使回执确认是
   孤儿，只要被当前快照引用，就不得进入本次可执行清理范围；
3. 清理计划保留本次计划引用的扫描凭据，保证拟清理路径可回溯到扫描批次；
4. 新快照引用如何改变原孤儿意见、本轮如何处置，必须写入审计记录。
"""
from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any

from app.store import store

# 回执判定（反馈约束）
VERDICT_CONFIRM_ORPHAN = "确认孤儿"
VERDICT_NOT_ORPHAN = "非孤儿"
VERDICT_DOUBT = "存疑"
VALID_VERDICTS = {VERDICT_CONFIRM_ORPHAN, VERDICT_NOT_ORPHAN, VERDICT_DOUBT}

# 核查单条目状态（待核清单 / 已闭环状态）
ITEM_PENDING_RECEIPT = "待回执"
ITEM_CONFIRMED = "已确认孤儿"
ITEM_CLEARED = "已排除"
ITEM_DOUBTFUL = "存疑待复核"
ITEM_BLOCKED = "快照引用拦截"
ITEM_PLANNED = "已纳入清理计划"
# 下一轮巡检待核项的状态集合
NEXT_ROUND_PENDING_STATUSES = {
    ITEM_PENDING_RECEIPT,
    ITEM_DOUBTFUL,
    ITEM_BLOCKED,
}

# 清理计划状态
PLAN_DRAFT = "待执行"
PLAN_EXECUTED = "已执行"

# 核查单 CSV 表头：扫描批次、文件全路径、判作孤儿的依据为核心三列
CHECKLIST_HEADER = [
    "scan_batch_id", "扫描批次", "file_path", "判作孤儿的依据",
    "size_bytes", "last_modified", "回执判定", "核查人", "核查备注",
]
RECEIPT_INPUT_FIELDS = {"scan_batch_id", "file_path", "回执判定", "核查人", "核查备注"}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _table(name: str) -> list[dict[str, Any]]:
    """惰性领取内存表，避免改动 seed 的初始模块集合。"""
    return store.rows(name)


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


class OrphanReviewService:
    """孤儿文件巡检闭环服务（无状态外观，数据落在全局内存 store）。"""

    # ------------------------------------------------------------------ #
    # 扫描批次与核查单
    # ------------------------------------------------------------------ #
    def register_scan_batch(
        self,
        batch_code: str,
        *,
        storage_root: str,
        baseline_snapshot_ids: list[str],
        scanner_version: str,
        scanned_at: str | None = None,
        credentials: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """登记一次扫描批次及其扫描凭据。

        扫描凭据（manifest 摘要、校验和、基线快照清单）会被后续清理计划
        原样引用，保证拟清理路径能回溯到这次扫描。
        """
        if not batch_code.strip():
            raise ValueError("扫描批次编号不能为空")
        batches = _table("orphan_scan_batch")
        if any(b["batch_code"] == batch_code for b in batches):
            raise ValueError(f"扫描批次 {batch_code} 已存在")
        batch = {
            "id": _next_id(batches),
            "batch_code": batch_code.strip(),
            "storage_root": storage_root,
            "scanned_at": scanned_at or _now(),
            "scanner_version": scanner_version,
            "baseline_snapshot_ids": list(baseline_snapshot_ids),
            "manifest_uri": (credentials or {}).get(
                "manifest_uri", f"manifests/{batch_code.strip()}.manifest.json"),
            "manifest_sha256": (credentials or {}).get("manifest_sha256", ""),
            "scan_job_id": (credentials or {}).get("scan_job_id", ""),
        }
        batches.append(batch)
        self._audit("SCAN_BATCH_REGISTERED", batch_code, None,
                    f"登记扫描批次 {batch_code}，基线快照 {','.join(baseline_snapshot_ids) or '无'}")
        return batch

    def add_candidate(
        self,
        batch_code: str,
        file_path: str,
        reason: str,
        *,
        size_bytes: int = 0,
        last_modified: str = "",
    ) -> dict[str, Any]:
        """向核查单追加一个待核路径（必须属于已登记的扫描批次）。"""
        batch = self._require_batch(batch_code)
        candidates = _table("orphan_candidate")
        for row in candidates:
            if row["batch_code"] == batch_code and row["file_path"] == file_path:
                raise ValueError(f"{file_path} 已在批次 {batch_code} 的核查单中")
        item = {
            "id": _next_id(candidates),
            "batch_code": batch_code,
            "file_path": file_path,
            "orphan_reason": reason,
            "size_bytes": int(size_bytes),
            "last_modified": last_modified,
            "status": ITEM_PENDING_RECEIPT,
            "receipt_id": None,
            "blocked_by_snapshot": None,
            "plan_id": None,
            "created_at": _now(),
        }
        candidates.append(item)
        self._audit("CANDIDATE_LISTED", batch_code, file_path,
                    f"核查单列入待核路径，判定依据：{reason}")
        return item

    # ------------------------------------------------------------------ #
    # 当前快照引用（可执行范围的外部约束）
    # ------------------------------------------------------------------ #
    def register_snapshot_reference(
        self,
        snapshot_id: str,
        file_paths: list[str],
        *,
        dataset: str = "",
        created_at: str | None = None,
    ) -> dict[str, Any]:
        """登记当前快照对文件路径的引用。

        典型场景：核查单导出后、回执回传前，新快照引用了其中一个文件。
        登记后该路径即使被回执确认孤儿，也会被拦截在清理计划之外。
        """
        refs = _table("snapshot_reference")
        record = {
            "id": _next_id(refs),
            "snapshot_id": snapshot_id,
            "dataset": dataset,
            "file_paths": list(file_paths),
            "created_at": created_at or _now(),
        }
        refs.append(record)
        candidates = _table("orphan_candidate")
        affected: list[dict[str, Any]] = []
        ref_set = set(file_paths)
        for item in candidates:
            if item["file_path"] in ref_set and item["status"] != ITEM_CLEARED:
                before = item["status"]
                item["blocked_by_snapshot"] = snapshot_id
                # 已确认孤儿 / 存疑 / 待回执：只要被当前快照引用，统一转为拦截状态
                item["status"] = ITEM_BLOCKED
                affected.append(item)
                self._audit(
                    "SNAPSHOT_REFERENCE_BLOCK", item["batch_code"], item["file_path"],
                    f"新快照 {snapshot_id} 引用该路径，原状态「{before}」转为「{ITEM_BLOCKED}」，"
                    "原孤儿意见在当前快照保留期内中止执行",
                )
        return {"reference": record, "affected_items": affected}

    def is_referenced_by_current_snapshot(self, file_path: str) -> str | None:
        """返回当前引用该路径的快照 id；没有引用返回 None。"""
        for ref in _table("snapshot_reference"):
            if file_path in ref.get("file_paths", []):
                return str(ref["snapshot_id"])
        return None

    # ------------------------------------------------------------------ #
    # 核查单导出 / 回执导入（支持分几次回传）
    # ------------------------------------------------------------------ #
    def export_checklist_csv(self, batch_code: str) -> str:
        """导出核查单 CSV：扫描批次、文件全路径、判作孤儿的依据。

        已闭环的条目回传时带最近一次回执判定，未回执的项目留空，
        方便审计方分几次填写后再回传。
        """
        self._require_batch(batch_code)
        output = io.StringIO()
        output.write("﻿")
        writer = csv.DictWriter(output, fieldnames=CHECKLIST_HEADER, lineterminator="\n",
                                extrasaction="ignore")
        writer.writeheader()
        for item in self._candidates_of(batch_code):
            receipt = self._receipt_of(batch_code, item["file_path"])
            writer.writerow({
                "scan_batch_id": batch_code,
                "扫描批次": batch_code,
                "file_path": item["file_path"],
                "判作孤儿的依据": item["orphan_reason"],
                "size_bytes": item["size_bytes"],
                "last_modified": item["last_modified"],
                "回执判定": receipt["verdict"] if receipt else "",
                "核查人": receipt["reviewer"] if receipt else "",
                "核查备注": receipt["remark"] if receipt else "",
            })
        return output.getvalue()

    def import_receipts_csv(self, content: str) -> dict[str, Any]:
        """逐行导入回执 CSV，按原批次与路径归档，支持分批次回传。

        每行回执独立校验：找不到批次 / 路径不在核查单 / 判定值非法的行
        计入 rejected，不影响其它行落库；同一 (批次, 路径) 重复回传按
        最新回执覆盖并保留覆盖审计。
        """
        text = content.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            raise ValueError("回执文件缺少表头")
        missing = {"scan_batch_id", "file_path", "回执判定"} - set(reader.fieldnames)
        if missing:
            raise ValueError(f"回执缺少必填列：{'、'.join(sorted(missing))}")
        rows = list(reader)
        return self.import_receipts([
            {key: (row.get(key) or "").strip() for key in RECEIPT_INPUT_FIELDS}
            for row in rows
        ])

    def import_receipts(self, rows: list[dict[str, str]]) -> dict[str, Any]:
        """导入结构化回执（JSON 数组），逐行归档并更新核查单状态。"""
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, str]] = []
        for row in rows:
            batch_code = row.get("scan_batch_id", "").strip()
            file_path = row.get("file_path", "").strip()
            verdict = row.get("回执判定", "").strip()
            reviewer = row.get("核查人", "").strip()
            remark = row.get("核查备注", "").strip()

            error = self._validate_receipt_row(batch_code, file_path, verdict)
            if error:
                rejected.append({"scan_batch_id": batch_code, "file_path": file_path,
                                 "回执判定": verdict, "error": error})
                continue

            item = next(c for c in self._candidates_of(batch_code) if c["file_path"] == file_path)
            receipt = self._upsert_receipt(batch_code, file_path, verdict, reviewer, remark)
            self._apply_verdict(item, receipt)
            accepted.append(receipt)
        return {
            "received_lines": len(rows),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "accepted_items": accepted,
            "rejected_items": rejected,
            "imported_at": _now(),
        }

    def _validate_receipt_row(self, batch_code: str, file_path: str, verdict: str) -> str:
        if not batch_code or not file_path or not verdict:
            return "scan_batch_id、file_path、回执判定均不能为空"
        if not any(b["batch_code"] == batch_code for b in _table("orphan_scan_batch")):
            return f"扫描批次 {batch_code} 未登记"
        if not any(c["file_path"] == file_path for c in self._candidates_of(batch_code)):
            return f"路径不在批次 {batch_code} 的核查单中"
        if verdict not in VALID_VERDICTS:
            return f"回执判定「{verdict}」非法，允许值：{'、'.join(sorted(VALID_VERDICTS))}"
        return ""

    def _upsert_receipt(
        self, batch_code: str, file_path: str,
        verdict: str, reviewer: str, remark: str,
    ) -> dict[str, Any]:
        receipts = _table("orphan_receipt")
        existing = self._receipt_of(batch_code, file_path)
        if existing is not None:
            before = existing["verdict"]
            existing.update({
                "verdict": verdict, "reviewer": reviewer, "remark": remark,
                "received_at": _now(), "revision": int(existing.get("revision", 1)) + 1,
            })
            self._audit("RECEIPT_SUPERSEDED", batch_code, file_path,
                        f"重复回传覆盖回执：{before} -> {verdict}")
            return existing
        receipt = {
            "id": _next_id(receipts),
            "batch_code": batch_code,
            "file_path": file_path,
            "verdict": verdict,
            "reviewer": reviewer,
            "remark": remark,
            "received_at": _now(),
            "revision": 1,
        }
        receipts.append(receipt)
        return receipt

    def _apply_verdict(self, item: dict[str, Any], receipt: dict[str, Any]) -> None:
        """把回执反馈落到核查单条目上，再叠加当前快照引用约束。"""
        verdict = receipt["verdict"]
        item["receipt_id"] = receipt["id"]
        snapshot_id = self.is_referenced_by_current_snapshot(item["file_path"])
        if snapshot_id:
            # 反馈约束与快照约束冲突时：快照约束优先，拦截清理但保留回执意见
            item["blocked_by_snapshot"] = snapshot_id
            item["status"] = ITEM_BLOCKED
            self._audit(
                "RECEIPT_BLOCKED_BY_SNAPSHOT", item["batch_code"], item["file_path"],
                f"回执判定「{verdict}」已归档；该路径被当前快照 {snapshot_id} 引用，"
                "不纳入本次清理，转下一轮巡检待核",
            )
            return
        item["blocked_by_snapshot"] = None
        if verdict == VERDICT_CONFIRM_ORPHAN:
            item["status"] = ITEM_CONFIRMED
        elif verdict == VERDICT_NOT_ORPHAN:
            item["status"] = ITEM_CLEARED
        else:
            item["status"] = ITEM_DOUBTFUL
        self._audit("RECEIPT_ACCEPTED", item["batch_code"], item["file_path"],
                    f"回执签收，判定「{verdict}」，核查单状态转为「{item['status']}」")

    # ------------------------------------------------------------------ #
    # 待核清单 / 清理计划
    # ------------------------------------------------------------------ #
    def pending_checklist(self, batch_code: str | None = None) -> dict[str, Any]:
        """待核清单：没收到回执的项目继续列出，另含存疑与快照拦截项。"""
        candidates = _table("orphan_candidate")
        if batch_code:
            candidates = [c for c in candidates if c["batch_code"] == batch_code]
        items = [self._enrich(c) for c in candidates
                 if c["status"] in NEXT_ROUND_PENDING_STATUSES]
        return {
            "batch_code": batch_code or "ALL",
            "total_candidates": len(candidates),
            "pending_count": len(items),
            "items": items,
        }

    def build_cleanup_plan(
        self,
        plan_name: str,
        batch_code: str,
        *,
        cleanup_job_id: int | None = None,
    ) -> dict[str, Any]:
        """从孤儿文件导出对应的回执提取拟清理路径，生成清理计划。

        可执行范围双重约束：
        * 反馈约束：必须有「确认孤儿」回执（从孤儿导出的回执中提取）；
        * 快照约束：当前快照未引用该路径。
        计划同时保留扫描批次的全部扫描凭据，路径可回溯。
        """
        batch = self._require_batch(batch_code)
        candidates = self._candidates_of(batch_code)
        executable: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for item in candidates:
            receipt = self._receipt_of(batch_code, item["file_path"])
            if receipt is None or receipt["verdict"] != VERDICT_CONFIRM_ORPHAN:
                continue  # 非确认孤儿的回执、存疑、未回执都不进入计划
            snapshot_id = self.is_referenced_by_current_snapshot(item["file_path"])
            if snapshot_id:
                blocked.append(self._enrich(item))
                continue
            executable.append(self._enrich(item))

        plans = _table("cleanup_plan")
        plan = {
            "id": _next_id(plans),
            "plan_name": plan_name,
            "batch_code": batch_code,
            "cleanup_job_id": cleanup_job_id,
            "status": PLAN_DRAFT,
            "created_at": _now(),
            # 保留本次计划引用的扫描凭据
            "scan_credentials": {
                "scan_batch_id": batch["batch_code"],
                "scanned_at": batch["scanned_at"],
                "scanner_version": batch["scanner_version"],
                "manifest_uri": batch["manifest_uri"],
                "manifest_sha256": batch["manifest_sha256"],
                "scan_job_id": batch["scan_job_id"],
                "baseline_snapshot_ids": batch["baseline_snapshot_ids"],
            },
            "executable_paths": [
                {
                    "file_path": item["file_path"],
                    "orphan_reason": item["orphan_reason"],
                    "size_bytes": item["size_bytes"],
                    "receipt_id": item["receipt_id"],
                    "reviewer": (self._receipt_of(batch_code, item["file_path"]) or {}).get("reviewer"),
                }
                for item in executable
            ],
            "blocked_paths": [
                {
                    "file_path": item["file_path"],
                    "blocked_by_snapshot": item["blocked_by_snapshot"],
                    "receipt_verdict": (self._receipt_of(batch_code, item["file_path"]) or {}).get("verdict"),
                }
                for item in blocked
            ],
        }
        plan["total_size_bytes"] = sum(p["size_bytes"] for p in plan["executable_paths"])
        plans.append(plan)
        for item in executable:
            raw = next(c for c in candidates if c["file_path"] == item["file_path"])
            raw["status"] = ITEM_PLANNED
            raw["plan_id"] = plan["id"]
        self._audit(
            "CLEANUP_PLAN_BUILT", batch_code, None,
            f"清理计划「{plan_name}」生成：可执行 {len(executable)} 个路径，"
            f"快照引用拦截 {len(blocked)} 个；扫描凭据 {batch['manifest_uri']} 已随计划留存",
        )
        return plan

    def mark_plan_executed(self, plan_id: int) -> dict[str, Any]:
        """清理作业执行后回填计划状态（演示用动作，不真正删除文件）。"""
        plan = self._require_plan(plan_id)
        plan["status"] = PLAN_EXECUTED
        plan["executed_at"] = _now()
        self._audit("CLEANUP_PLAN_EXECUTED", plan["batch_code"], None,
                    f"清理计划 {plan_id}「{plan['plan_name']}」执行完成，"
                    f"实际清理 {len(plan['executable_paths'])} 个路径")
        return plan

    # ------------------------------------------------------------------ #
    # 查询与审计留痕
    # ------------------------------------------------------------------ #
    def list_batches(self) -> list[dict[str, Any]]:
        result = []
        for batch in _table("orphan_scan_batch"):
            candidates = self._candidates_of(batch["batch_code"])
            result.append({
                **batch,
                "candidate_count": len(candidates),
                "receipt_count": len(self._receipts_of(batch["batch_code"])),
                "pending_count": sum(
                    1 for c in candidates if c["status"] in NEXT_ROUND_PENDING_STATUSES),
            })
        return result

    def list_candidates(self, batch_code: str | None = None) -> list[dict[str, Any]]:
        candidates = _table("orphan_candidate")
        if batch_code:
            candidates = [c for c in candidates if c["batch_code"] == batch_code]
        return [self._enrich(c) for c in candidates]

    def list_receipts(self, batch_code: str | None = None) -> list[dict[str, Any]]:
        receipts = _table("orphan_receipt")
        if batch_code:
            receipts = [r for r in receipts if r["batch_code"] == batch_code]
        return receipts

    def list_plans(self) -> list[dict[str, Any]]:
        return _table("cleanup_plan")

    def audit_trail(self, batch_code: str | None = None) -> list[dict[str, Any]]:
        events = _table("orphan_audit_event")
        if batch_code:
            events = [e for e in events if e["batch_code"] == batch_code]
        return events

    def audit_narrative(self, batch_code: str) -> dict[str, Any]:
        """生成审计说明：新快照引用怎样影响原意见、本轮如何处置。"""
        self._require_batch(batch_code)
        candidates = self._candidates_of(batch_code)
        blocked = [self._enrich(c) for c in candidates if c["status"] == ITEM_BLOCKED]
        confirmed = [self._enrich(c) for c in candidates if c["status"] == ITEM_CONFIRMED]
        planned = [self._enrich(c) for c in candidates if c["status"] == ITEM_PLANNED]
        pending_receipt = [self._enrich(c) for c in candidates
                           if c["status"] == ITEM_PENDING_RECEIPT]
        cleared = [self._enrich(c) for c in candidates if c["status"] == ITEM_CLEARED]
        doubtful = [self._enrich(c) for c in candidates if c["status"] == ITEM_DOUBTFUL]

        statements: list[str] = []
        for item in blocked:
            receipt = self._receipt_of(batch_code, item["file_path"])
            verdict = receipt["verdict"] if receipt else "（尚无回执）"
            statements.append(
                f"路径 {item['file_path']} 扫描时依据「{item['orphan_reason']}」判作孤儿；"
                f"回执回传前被新快照 {item['blocked_by_snapshot']} 引用，"
                f"原孤儿意见（回执判定：{verdict}）在该快照保留期内不再成立，"
                "本轮不予清理，转入下一轮巡检待核清单并持续监控快照引用。"
            )
        for item in confirmed + planned:
            statements.append(
                f"路径 {item['file_path']} 经回执确认孤儿且无当前快照引用，"
                f"原孤儿意见维持有效，本轮{'已纳入清理计划' if item['status'] == ITEM_PLANNED else '可进入清理计划'}。"
            )
        for item in cleared:
            statements.append(
                f"路径 {item['file_path']} 回执判定非孤儿，原扫描意见被人工核查推翻，闭环排除。"
            )
        for item in doubtful:
            statements.append(
                f"路径 {item['file_path']} 回执存疑，保留在下一轮巡检待核清单复核。"
            )
        for item in pending_receipt:
            statements.append(
                f"路径 {item['file_path']} 尚未收到回执，继续列在待核清单，不进入清理范围。"
            )
        return {
            "batch_code": batch_code,
            "generated_at": _now(),
            "summary": {
                "total": len(candidates),
                "blocked_by_snapshot": len(blocked),
                "confirmed_orphan": len(confirmed),
                "in_cleanup_plan": len(planned),
                "cleared": len(cleared),
                "doubtful": len(doubtful),
                "awaiting_receipt": len(pending_receipt),
            },
            "statements": statements,
        }

    # ------------------------------------------------------------------ #
    # 演示场景：同一张核查单两个文件，回传前一个被新快照引用
    # ------------------------------------------------------------------ #
    def seed_demo_scenario(self, *, reset: bool = True) -> dict[str, Any]:
        """构造题目要求的标准场景并返回关键标识。"""
        if reset:
            for name in ("orphan_scan_batch", "orphan_candidate", "orphan_receipt",
                         "snapshot_reference", "cleanup_plan", "orphan_audit_event"):
                _table(name).clear()

        batch = self.register_scan_batch(
            "SCAN-20261123-01",
            storage_root="/data/lakehouse",
            baseline_snapshot_ids=["snap-20261122"],
            scanner_version="orphan-scanner/3.2.1",
            scanned_at="2026-11-23T02:00:00Z",
            credentials={
                "manifest_uri": "manifests/SCAN-20261123-01.manifest.json",
                "manifest_sha256": "9f2c7a1e4b6d8f0a3c5e7b9d1f2a4c6e8b0d2f4a6c8e0b2d4f6a8c0e2b4d6f8a",
                "scan_job_id": "job-scan-8842",
            },
        )
        f1 = self.add_candidate(
            "SCAN-20261123-01",
            "/data/lakehouse/archived/dwd_order_v1/part-00007-a1.parquet",
            "旧Schema版本数据，基线快照 snap-20261122 无引用",
            size_bytes=1_073_741_824, last_modified="2026-10-01",
        )
        f2 = self.add_candidate(
            "SCAN-20261123-01",
            "/data/lakehouse/staging/_temporary/merge_3872/part-00002.parquet",
            "合并任务 merge_3872 残留文件，任务目录已不在运行清单",
            size_bytes=262_144_000, last_modified="2026-11-20",
        )
        # 回传前：新快照引用了 F1
        ref = self.register_snapshot_reference(
            "snap-20261123",
            ["/data/lakehouse/archived/dwd_order_v1/part-00007-a1.parquet"],
            dataset="dwd_order",
            created_at="2026-11-23T05:30:00Z",
        )
        self._audit("DEMO_SCENARIO_READY", "SCAN-20261123-01", None,
                    "演示场景就绪：核查单含2个Parquet路径，其中1个在回执回传前被新快照 snap-20261123 引用")
        return {
            "batch": batch,
            "candidate_f1": f1,
            "candidate_f2": f2,
            "snapshot_reference": ref["reference"],
            "checklist_csv": self.export_checklist_csv("SCAN-20261123-01"),
        }

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _require_batch(self, batch_code: str) -> dict[str, Any]:
        for batch in _table("orphan_scan_batch"):
            if batch["batch_code"] == batch_code:
                return batch
        raise KeyError(f"扫描批次 {batch_code} 未登记")

    def _require_plan(self, plan_id: int) -> dict[str, Any]:
        for plan in _table("cleanup_plan"):
            if int(plan["id"]) == plan_id:
                return plan
        raise KeyError(f"清理计划 {plan_id} 不存在")

    def _candidates_of(self, batch_code: str) -> list[dict[str, Any]]:
        return [c for c in _table("orphan_candidate") if c["batch_code"] == batch_code]

    def _receipts_of(self, batch_code: str) -> list[dict[str, Any]]:
        return [r for r in _table("orphan_receipt") if r["batch_code"] == batch_code]

    def _receipt_of(self, batch_code: str, file_path: str) -> dict[str, Any] | None:
        for receipt in self._receipts_of(batch_code):
            if receipt["file_path"] == file_path:
                return receipt
        return None

    def _enrich(self, item: dict[str, Any]) -> dict[str, Any]:
        receipt = self._receipt_of(item["batch_code"], item["file_path"])
        return {
            **item,
            "receipt_verdict": receipt["verdict"] if receipt else None,
            "reviewer": receipt["reviewer"] if receipt else None,
            "currently_referenced": self.is_referenced_by_current_snapshot(item["file_path"]),
        }

    def _audit(self, event_type: str, batch_code: str | None,
               file_path: str | None, detail: str) -> dict[str, Any]:
        events = _table("orphan_audit_event")
        event = {
            "id": _next_id(events),
            "event_type": event_type,
            "batch_code": batch_code,
            "file_path": file_path,
            "detail": detail,
            "created_at": _now(),
        }
        events.append(event)
        return event


orphan_review_service = OrphanReviewService()
