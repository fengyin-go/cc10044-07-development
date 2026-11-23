"""孤儿文件巡检闭环领域逻辑测试。

覆盖题目主场景：同一张核查单两个 Parquet 文件，回执回传前一个被新快照引用；
签收回执后核对清理计划可执行路径、下一轮巡检待核项与审计记录。
"""
from __future__ import annotations

import sys

from app.services.orphan_review import (
    ITEM_BLOCKED,
    ITEM_CONFIRMED,
    ITEM_PENDING_RECEIPT,
    ITEM_PLANNED,
    VERDICT_CONFIRM_ORPHAN,
    VERDICT_DOUBT,
    VERDICT_NOT_ORPHAN,
    OrphanReviewService,
)

BATCH = "SCAN-20261123-01"
F1 = "/data/lakehouse/archived/dwd_order_v1/part-00007-a1.parquet"
F2 = "/data/lakehouse/staging/_temporary/merge_3872/part-00002.parquet"
FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" :: {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def fresh_service() -> OrphanReviewService:
    svc = OrphanReviewService()
    svc.seed_demo_scenario(reset=True)
    return svc


def main() -> int:
    svc = fresh_service()

    # 1. 演示场景：核查单两个文件，F1 在回传前被新快照引用
    candidates = svc.list_candidates(BATCH)
    check("核查单包含2个待核路径", len(candidates) == 2)
    f1_item = next(c for c in candidates if c["file_path"] == F1)
    f2_item = next(c for c in candidates if c["file_path"] == F2)
    check("F1 因新快照引用进入拦截状态", f1_item["status"] == ITEM_BLOCKED,
          f1_item["status"])
    check("F1 记录拦截快照", f1_item["blocked_by_snapshot"] == "snap-20261123")
    check("F1 当前仍被快照引用", f1_item["currently_referenced"] == "snap-20261123")
    check("F2 仍在待回执状态", f2_item["status"] == ITEM_PENDING_RECEIPT)

    # 2. 核查单导出：扫描批次、文件全路径、判作孤儿的依据三列齐备
    csv_text = svc.export_checklist_csv(BATCH)
    lines = [line for line in csv_text.strip().splitlines() if line.strip()]
    check("导出含表头+2行数据", len(lines) == 3, str(len(lines)))
    header = lines[0].lstrip("﻿").split(",")
    for column in ("scan_batch_id", "扫描批次", "file_path", "判作孤儿的依据"):
        check(f"导出表头含 {column}", column in header)
    check("导出行携带批次号", all(BATCH in line for line in lines[1:]))
    check("导出含 F1/F2 全路径", F1 in csv_text and F2 in csv_text)
    check("未回执项目回执判定留空", lines[1].split(",")[6] == "" and lines[2].split(",")[6] == "")

    # 3. 分批回传：第一次只回 F2（确认孤儿）
    first_batch = svc.import_receipts([
        {"scan_batch_id": BATCH, "file_path": F2, "回执判定": VERDICT_CONFIRM_ORPHAN,
         "核查人": "auditor_li", "核查备注": "现场核验无元数据引用"},
    ])
    check("首批回执接收1行归档1行",
          first_batch["received_lines"] == 1 and first_batch["accepted"] == 1)
    f2_after = next(c for c in svc.list_candidates(BATCH) if c["file_path"] == F2)
    check("F2 回执签收后确认孤儿", f2_after["status"] == ITEM_CONFIRMED, f2_after["status"])

    # 没收到回执的项目继续列在待核清单（F1 为快照拦截，仍属下一轮待核）
    pending = svc.pending_checklist(BATCH)
    pending_paths = {item["file_path"] for item in pending["items"]}
    check("待核清单仍含未闭环的 F1", F1 in pending_paths)
    check("已确认孤儿的 F2 不在待核清单", F2 not in pending_paths)

    # 4. 第二次回传：F1 同样确认孤儿，但快照约束优先
    second_batch = svc.import_receipts([
        {"scan_batch_id": BATCH, "file_path": F1, "回执判定": VERDICT_CONFIRM_ORPHAN,
         "核查人": "auditor_li", "核查备注": "扫描意见属实但注意新快照"},
    ])
    check("次批回执归档1行", second_batch["accepted"] == 1 and second_batch["rejected"] == 0)
    f1_after = next(c for c in svc.list_candidates(BATCH) if c["file_path"] == F1)
    check("F1 回执已归档但仍被快照拦截", f1_after["status"] == ITEM_BLOCKED)
    check("F1 回执判定保留为确认孤儿", f1_after["receipt_verdict"] == VERDICT_CONFIRM_ORPHAN)

    # 5. 逐行校验：坏行拒收且不影响好行
    bad = svc.import_receipts([
        {"scan_batch_id": "NO-SUCH-BATCH", "file_path": F1, "回执判定": VERDICT_CONFIRM_ORPHAN},
        {"scan_batch_id": BATCH, "file_path": "/not/in/checklist.parquet",
         "回执判定": VERDICT_CONFIRM_ORPHAN},
        {"scan_batch_id": BATCH, "file_path": F2, "回执判定": "随便写写"},
        {"scan_batch_id": BATCH, "file_path": F2, "回执判定": VERDICT_NOT_ORPHAN,
         "核查人": "auditor_wang"},
    ])
    check("3行坏数据被拒收", bad["rejected"] == 3, str(bad["rejected_items"]))
    check("1行好数据被归档", bad["accepted"] == 1)
    check("坏行各自带错误原因", all(item.get("error") for item in bad["rejected_items"]))

    # F2 被最新回执改判为非孤儿 → 关闭原孤儿意见
    f2_revised = next(c for c in svc.list_candidates(BATCH) if c["file_path"] == F2)
    check("F2 改判非孤儿后排除", f2_revised["status"] == "已排除", f2_revised["status"])

    # 再改回确认孤儿，验证重复回传覆盖留痕
    svc.import_receipts([
        {"scan_batch_id": BATCH, "file_path": F2, "回执判定": VERDICT_CONFIRM_ORPHAN,
         "核查人": "auditor_li", "核查备注": "复核后维持孤儿判定"},
    ])
    receipt_f2 = svc._receipt_of(BATCH, F2)
    # F2 共回传 3 次：确认孤儿 → 非孤儿 → 确认孤儿，修订版本应为 3
    check("重复回传产生修订版本", receipt_f2["revision"] == 3)
    check("覆盖事件写入审计", any(e["event_type"] == "RECEIPT_SUPERSEDED"
                                  and e["file_path"] == F2 for e in svc.audit_trail(BATCH)))

    # 6. CSV 通道导入（带 BOM、含存疑判定的新批次路径）
    svc.add_candidate(BATCH, "/data/lakehouse/tmp/etl_temp_x.parquet",
                      "临时文件未清理", size_bytes=1024, last_modified="2026-11-15")
    csv_upload = (
        "﻿scan_batch_id,file_path,回执判定,核查人,核查备注\n"
        f"{BATCH},/data/lakehouse/tmp/etl_temp_x.parquet,{VERDICT_DOUBT},auditor_zhao,需二次确认\n"
    )
    csv_result = svc.import_receipts_csv(csv_upload)
    check("CSV 回执导入1行", csv_result["accepted"] == 1 and csv_result["rejected"] == 0)
    doubtful = next(c for c in svc.list_candidates(BATCH)
                    if c["file_path"] == "/data/lakehouse/tmp/etl_temp_x.parquet")
    check("存疑项进入存疑待复核", doubtful["status"] == "存疑待复核", doubtful["status"])

    # 7. 清理计划：双重约束（回执确认孤儿 + 当前快照未引用）
    plan = svc.build_cleanup_plan("plan-orphan-20261123", BATCH, cleanup_job_id=3)
    executable_paths = [p["file_path"] for p in plan["executable_paths"]]
    blocked_paths = [p["file_path"] for p in plan["blocked_paths"]]
    check("可执行路径仅 F2", executable_paths == [F2], str(executable_paths))
    check("F1 进入拦截清单", blocked_paths == [F1], str(blocked_paths))
    check("存疑/未回执路径不进计划",
          "/data/lakehouse/tmp/etl_temp_x.parquet" not in executable_paths)

    # 保留本次计划引用的扫描凭据
    creds = plan["scan_credentials"]
    check("计划留存 manifest 凭据", creds["manifest_uri"].endswith(".manifest.json"))
    check("计划留存校验和", len(creds["manifest_sha256"]) == 64)
    check("计划留存基线快照", creds["baseline_snapshot_ids"] == ["snap-20261122"])
    check("计划关联清理作业", plan["cleanup_job_id"] == 3)
    check("拟清理路径可回溯到回执", plan["executable_paths"][0]["receipt_id"] is not None)

    # 8. 计划生成后核查单状态与下一轮待核项
    f2_planned = next(c for c in svc.list_candidates(BATCH) if c["file_path"] == F2)
    check("F2 已纳入清理计划", f2_planned["status"] == ITEM_PLANNED)
    next_round = svc.pending_checklist(BATCH)
    next_paths = {item["file_path"] for item in next_round["items"]}
    check("下一轮待核含快照拦截的 F1", F1 in next_paths)
    check("下一轮待核含存疑项", "/data/lakehouse/tmp/etl_temp_x.parquet" in next_paths)
    check("下一轮待核不含已计划的 F2", F2 not in next_paths)

    # 9. 审计记录交代新快照引用如何影响原意见及本轮处置
    narrative = svc.audit_narrative(BATCH)
    f1_statement = next(s for s in narrative["statements"] if F1 in s)
    check("审计说明覆盖 F1 原意见变化", "原孤儿意见" in f1_statement and "不再成立" in f1_statement)
    check("审计说明交代本轮处置", "不予清理" in f1_statement and "下一轮巡检待核" in f1_statement)
    check("审计汇总拦截数为1", narrative["summary"]["blocked_by_snapshot"] == 1)
    trail_types = {e["event_type"] for e in svc.audit_trail(BATCH)}
    for event_type in ("SCAN_BATCH_REGISTERED", "CANDIDATE_LISTED",
                       "SNAPSHOT_REFERENCE_BLOCK", "RECEIPT_ACCEPTED",
                       "RECEIPT_BLOCKED_BY_SNAPSHOT", "CLEANUP_PLAN_BUILT"):
        check(f"审计轨迹含 {event_type}", event_type in trail_types)

    # 10. 计划执行回填 + 重复导出带最新回执
    executed = svc.mark_plan_executed(plan["id"])
    check("计划可标记执行完成", executed["status"] == "已执行")
    re_export = svc.export_checklist_csv(BATCH)
    check("再次导出回填回执判定", "确认孤儿" in re_export and "存疑" in re_export)

    # 11. 快照解除后 F1 可在下一轮进入计划（约束随当前快照动态变化）
    from app.store import store
    store.rows("snapshot_reference").clear()
    check("快照解除后无引用", svc.is_referenced_by_current_snapshot(F1) is None)
    plan2 = svc.build_cleanup_plan("plan-orphan-20261124", BATCH)
    exec2 = {p["file_path"] for p in plan2["executable_paths"]}
    check("快照解除后 F1 进入新一轮可执行范围", F1 in exec2, str(exec2))

    print("\n审计叙述示例：")
    for statement in narrative["statements"]:
        print(" -", statement)

    print(f"\n共 {len(FAILURES)} 项失败" if FAILURES else "\n全部用例通过")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
