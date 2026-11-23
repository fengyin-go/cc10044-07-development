"""HTTP 端到端验证：用标准库 urllib 打真实 uvicorn 进程。

覆盖：演示场景初始化、核查单 CSV 导出、分批回执（JSON + CSV 两种通道）、
待核清单、清理计划、审计叙述，以及 400/404 错误路径。
"""
from __future__ import annotations

import csv
import io
import json
import sys
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8018/api/orphan-review"
BATCH = "SCAN-20261123-01"
F1 = "/data/lakehouse/archived/dwd_order_v1/part-00007-a1.parquet"
F2 = "/data/lakehouse/staging/_temporary/merge_3872/part-00002.parquet"
FAILURES: list[str] = []


def call(method: str, path: str, body: object = None, *, raw: bytes | None = None,
         content_type: str = "application/json"):
    data = None
    headers = {}
    if raw is not None:
        data = raw
        headers["Content-Type"] = content_type
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            payload = response.read()
            ctype = response.headers.get("content-type", "")
            return response.status, payload, ctype
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), exc.headers.get("content-type", "")


def check(name: str, condition: bool, detail: str = "") -> None:
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" :: {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def main() -> int:
    # 1. 初始化演示场景
    status, payload, _ = call("POST", "/demo/init", {"reset": True})
    data = json.loads(payload)
    check("演示场景初始化 200", status == 200 and data["ok"] is True)
    check("初始化返回核查单 CSV", "scan_batch_id" in data["entry"]["checklist_csv"])

    # 2. 核查单导出（真实文件下载语义）
    status, payload, ctype = call("GET", f"/checklist/export?batch_code={BATCH}")
    check("核查单导出 200 且为 CSV", status == 200 and "text/csv" in ctype)
    text = payload.decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text)))
    check("导出 CSV 含 2 行核查项", len(rows) == 2)
    check("导出行含批次/全路径/判定依据",
          all(r["scan_batch_id"] == BATCH and r["file_path"] and r["判作孤儿的依据"] for r in rows))

    # 3. 回执模板可下载
    status, payload, _ = call("GET", "/checklist/template")
    check("回执模板可下载", status == 200 and "回执判定" in payload.decode("utf-8-sig"))

    # 4. 第一批回执：仅回 F2
    status, payload, _ = call("POST", "/receipts/import", {"receipts": [
        {"scan_batch_id": BATCH, "file_path": F2, "回执判定": "确认孤儿",
         "核查人": "auditor_li", "核查备注": "现场核验"},
    ]})
    data = json.loads(payload)
    check("首批 JSON 回执签收", status == 200 and data["accepted"] == 1 and data["ok"] is True)

    # 5. 第二批走 CSV 通道回 F1（同样判孤儿，但被快照拦截）
    csv_body = (
        "scan_batch_id,file_path,回执判定,核查人,核查备注\n"
        f"{BATCH},{F1},确认孤儿,auditor_li,扫描意见属实\n"
    ).encode("utf-8")
    status, payload, _ = call("POST", "/receipts/import-csv",
                              raw=csv_body, content_type="text/csv; charset=utf-8")
    data = json.loads(payload)
    check("CSV 通道回执签收", status == 200 and data["accepted"] == 1)

    # 6. 坏数据逐行拒收
    status, payload, _ = call("POST", "/receipts/import", {"receipts": [
        {"scan_batch_id": "GHOST", "file_path": F1, "回执判定": "确认孤儿"},
        {"scan_batch_id": BATCH, "file_path": F2, "回执判定": "非法值"},
    ]})
    data = json.loads(payload)
    check("坏回执整体不回滚且逐行拒收",
          status == 200 and data["rejected"] == 2 and data["ok"] is False)
    check("拒收原因可读", all(x["error"] for x in data["rejected_items"]))

    # 7. 待核清单：F1（快照拦截）仍在，F2（已确认）不在
    status, payload, _ = call("GET", f"/pending?batch_code={BATCH}")
    pending = json.loads(payload)
    pending_paths = {item["file_path"] for item in pending["items"]}
    check("待核清单含快照拦截项 F1", F1 in pending_paths)
    check("待核清单不含已确认项 F2", F2 not in pending_paths)

    # 8. 生成清理计划：仅 F2 可执行，F1 被拦截
    status, payload, _ = call("POST", "/cleanup-plans",
                              {"plan_name": "plan-orphan-20261123", "batch_code": BATCH,
                               "cleanup_job_id": 3})
    plan_resp = json.loads(payload)
    plan = plan_resp["entry"]
    check("清理计划生成 200", status == 200 and plan_resp["ok"] is True)
    check("可执行路径仅 F2", [p["file_path"] for p in plan["executable_paths"]] == [F2])
    check("F1 在拦截清单且带快照号",
          [p["file_path"] for p in plan["blocked_paths"]] == [F1]
          and plan["blocked_paths"][0]["blocked_by_snapshot"] == "snap-20261123")
    check("计划保留扫描凭据",
          plan["scan_credentials"]["manifest_uri"].endswith(".manifest.json")
          and len(plan["scan_credentials"]["manifest_sha256"]) == 64)

    # 9. 计划列表与执行回填
    status, payload, _ = call("GET", "/cleanup-plans")
    plans = json.loads(payload)["items"]
    check("计划列表可查询", len(plans) == 1 and plans[0]["status"] == "待执行")
    status, payload, _ = call("POST", f"/cleanup-plans/{plan['id']}/execute")
    check("计划执行回填", status == 200 and json.loads(payload)["entry"]["status"] == "已执行")

    # 10. 审计叙述：交代新快照引用对原意见的影响与本轮处置
    status, payload, _ = call("GET", f"/audit-narrative?batch_code={BATCH}")
    narrative = json.loads(payload)
    f1_statement = next(s for s in narrative["statements"] if F1 in s)
    check("审计叙述说明原意见变化", "不再成立" in f1_statement and "新快照 snap-20261123" in f1_statement)
    check("审计叙述说明本轮处置", "不予清理" in f1_statement and "下一轮巡检待核" in f1_statement)
    check("审计汇总数字正确",
          narrative["summary"]["blocked_by_snapshot"] == 1
          and narrative["summary"]["in_cleanup_plan"] == 1)

    # 11. 审计轨迹完整留痕
    status, payload, _ = call("GET", f"/audit-trail?batch_code={BATCH}")
    events = json.loads(payload)["items"]
    types_ = {e["event_type"] for e in events}
    check("审计轨迹覆盖全流程",
          {"SCAN_BATCH_REGISTERED", "RECEIPT_ACCEPTED", "RECEIPT_BLOCKED_BY_SNAPSHOT",
           "CLEANUP_PLAN_BUILT", "CLEANUP_PLAN_EXECUTED"} <= types_)

    # 12. 错误路径
    status, _, _ = call("GET", "/checklist/export?batch_code=UNKNOWN")
    check("未知批次导出 404", status == 404)
    status, _, _ = call("POST", "/batches", {"batch_code": BATCH, "storage_root": "/x",
                                              "baseline_snapshot_ids": []})
    check("重复批次登记 400", status == 400)
    status, _, _ = call("POST", "/cleanup-plans",
                        {"plan_name": "p", "batch_code": "UNKNOWN"})
    check("未知批次建计划 404", status == 404)

    # 13. 通用资源路由未被专用路由干扰
    with urllib.request.urlopen("http://127.0.0.1:8018/api/health") as response:
        health = json.loads(response.read())
    check("健康检查正常", health["ok"] is True)
    with urllib.request.urlopen("http://127.0.0.1:8018/api/data_lake_audit") as response:
        generic = json.loads(response.read())
    check("通用模块列表路由仍可用", generic["total"] >= 1)

    print(f"\n共 {len(FAILURES)} 项失败" if FAILURES else "\nHTTP 端到端全部通过")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
