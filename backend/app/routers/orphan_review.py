"""湖仓审计-孤儿文件巡检闭环接口。

路由前缀 ``/api/orphan-review``，与通用资源路由并列：
核查单导出、逐行回执导入、快照引用登记、清理计划和审计留痕。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.orphan_review import (
    CHECKLIST_HEADER,
    VALID_VERDICTS,
    orphan_review_service,
)

router = APIRouter(prefix="/api/orphan-review", tags=["湖仓审计-孤儿巡检"])


# --------------------------------------------------------------------- #
# 请求模型
# --------------------------------------------------------------------- #
class ScanBatchPayload(BaseModel):
    batch_code: str
    storage_root: str = "/data/lakehouse"
    baseline_snapshot_ids: list[str] = Field(default_factory=list)
    scanner_version: str = "orphan-scanner/3.2.1"
    scanned_at: str | None = None
    credentials: dict[str, str] = Field(default_factory=dict)


class CandidatePayload(BaseModel):
    batch_code: str
    file_path: str
    reason: str
    size_bytes: int = 0
    last_modified: str = ""


class SnapshotReferencePayload(BaseModel):
    snapshot_id: str
    file_paths: list[str]
    dataset: str = ""
    created_at: str | None = None


class ReceiptRow(BaseModel):
    scan_batch_id: str
    file_path: str
    回执判定: str
    核查人: str = ""
    核查备注: str = ""


class ReceiptImportPayload(BaseModel):
    receipts: list[ReceiptRow]


class CleanupPlanPayload(BaseModel):
    plan_name: str
    batch_code: str
    cleanup_job_id: int | None = None


class DemoPayload(BaseModel):
    reset: bool = True


def _guard(call):
    """把领域层的 KeyError/ValueError 转成 HTTP 错误，正常结果原样返回。"""
    try:
        return call()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# --------------------------------------------------------------------- #
# 扫描批次与核查单
# --------------------------------------------------------------------- #
@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"items": orphan_review_service.list_batches()}


@router.post("/batches")
def register_batch(payload: ScanBatchPayload) -> dict[str, Any]:
    batch = _guard(lambda: orphan_review_service.register_scan_batch(
        payload.batch_code,
        storage_root=payload.storage_root,
        baseline_snapshot_ids=payload.baseline_snapshot_ids,
        scanner_version=payload.scanner_version,
        scanned_at=payload.scanned_at,
        credentials=payload.credentials,
    ))
    return {"ok": True, "message": "扫描批次已登记", "entry": batch}


@router.post("/candidates")
def add_candidate(payload: CandidatePayload) -> dict[str, Any]:
    item = _guard(lambda: orphan_review_service.add_candidate(
        payload.batch_code, payload.file_path, payload.reason,
        size_bytes=payload.size_bytes, last_modified=payload.last_modified,
    ))
    return {"ok": True, "message": "待核路径已列入核查单", "entry": item}


@router.get("/candidates")
def list_candidates(batch_code: str | None = None) -> dict[str, Any]:
    return {"items": orphan_review_service.list_candidates(batch_code)}


@router.get("/checklist/export")
def export_checklist(batch_code: str) -> StreamingResponse:
    def render() -> str:
        return orphan_review_service.export_checklist_csv(batch_code)
    content = _guard(render)
    filename = f"orphan_checklist_{batch_code}.csv"
    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/checklist/template")
def receipt_template() -> StreamingResponse:
    """回执模板：仅保留需要审计方逐行填写的列。"""
    import csv
    import io

    output = io.StringIO()
    output.write("﻿")
    input_fields = [f for f in CHECKLIST_HEADER if f in
                    {"scan_batch_id", "file_path", "回执判定", "核查人", "核查备注"}]
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(input_fields)
    writer.writerow(["<扫描批次>", "<文件全路径>", " / ".join(sorted(VALID_VERDICTS)),
                     "<核查人>", "<核查备注>"])
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="orphan_receipt_template.csv"'},
    )


# --------------------------------------------------------------------- #
# 快照引用约束
# --------------------------------------------------------------------- #
@router.post("/snapshot-references")
def register_snapshot_reference(payload: SnapshotReferencePayload) -> dict[str, Any]:
    result = _guard(lambda: orphan_review_service.register_snapshot_reference(
        payload.snapshot_id, payload.file_paths,
        dataset=payload.dataset, created_at=payload.created_at,
    ))
    return {"ok": True, "message": "快照引用已登记并重新评估核查单", **result}


@router.get("/snapshot-references")
def list_snapshot_references() -> dict[str, Any]:
    from app.store import store
    return {"items": store.rows("snapshot_reference")}


# --------------------------------------------------------------------- #
# 回执导入（逐行、可分批回传）
# --------------------------------------------------------------------- #
@router.post("/receipts/import")
def import_receipts(payload: ReceiptImportPayload) -> dict[str, Any]:
    rows = [row.model_dump() for row in payload.receipts]
    result = _guard(lambda: orphan_review_service.import_receipts(rows))
    result["ok"] = result["rejected"] == 0
    result["message"] = (
        f"回执签收完成：接收 {result['received_lines']} 行，"
        f"归档 {result['accepted']} 行，拒收 {result['rejected']} 行"
    )
    return result


@router.post("/receipts/import-csv")
async def import_receipts_csv(request: Request) -> dict[str, Any]:
    """支持直接上传回执 CSV（Content-Type: text/csv 或 multipart 文件）。"""
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        upload = form.get("file")
        if upload is None:
            raise HTTPException(status_code=400, detail="multipart 表单缺少 file 字段")
        raw = (await upload.read()).decode("utf-8-sig")
    else:
        raw = (await request.body()).decode("utf-8-sig")
    result = _guard(lambda: orphan_review_service.import_receipts_csv(raw))
    result["ok"] = result["rejected"] == 0
    result["message"] = (
        f"CSV 回执签收完成：接收 {result['received_lines']} 行，"
        f"归档 {result['accepted']} 行，拒收 {result['rejected']} 行"
    )
    return result


@router.get("/receipts")
def list_receipts(batch_code: str | None = None) -> dict[str, Any]:
    return {"items": orphan_review_service.list_receipts(batch_code)}


# --------------------------------------------------------------------- #
# 待核清单、清理计划与审计
# --------------------------------------------------------------------- #
@router.get("/pending")
def pending_checklist(batch_code: str | None = None) -> dict[str, Any]:
    return orphan_review_service.pending_checklist(batch_code)


@router.post("/cleanup-plans")
def build_cleanup_plan(payload: CleanupPlanPayload) -> dict[str, Any]:
    plan = _guard(lambda: orphan_review_service.build_cleanup_plan(
        payload.plan_name, payload.batch_code,
        cleanup_job_id=payload.cleanup_job_id,
    ))
    return {"ok": True,
            "message": f"清理计划已生成：可执行 {len(plan['executable_paths'])} 个路径，"
                       f"拦截 {len(plan['blocked_paths'])} 个路径",
            "entry": plan}


@router.get("/cleanup-plans")
def list_plans() -> dict[str, Any]:
    return {"items": orphan_review_service.list_plans()}


@router.post("/cleanup-plans/{plan_id}/execute")
def execute_plan(plan_id: int) -> dict[str, Any]:
    plan = _guard(lambda: orphan_review_service.mark_plan_executed(plan_id))
    return {"ok": True, "message": "清理计划已标记执行完成", "entry": plan}


@router.get("/audit-trail")
def audit_trail(batch_code: str | None = None) -> dict[str, Any]:
    return {"items": orphan_review_service.audit_trail(batch_code)}


@router.get("/audit-narrative")
def audit_narrative(batch_code: str) -> dict[str, Any]:
    return _guard(lambda: orphan_review_service.audit_narrative(batch_code))


# --------------------------------------------------------------------- #
# 演示场景
# --------------------------------------------------------------------- #
@router.post("/demo/init")
def init_demo(payload: DemoPayload) -> dict[str, Any]:
    return {"ok": True, "message": "演示场景已就绪",
            "entry": orphan_review_service.seed_demo_scenario(reset=payload.reset)}
