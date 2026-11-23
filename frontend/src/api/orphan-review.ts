/** 湖仓审计-孤儿文件巡检闭环接口封装。 */
import { request } from './client'

const BASE = '/api/orphan-review'

export type ScanBatch = {
  id: number
  batch_code: string
  storage_root: string
  scanned_at: string
  scanner_version: string
  baseline_snapshot_ids: string[]
  manifest_uri: string
  manifest_sha256: string
  scan_job_id: string
  candidate_count?: number
  receipt_count?: number
  pending_count?: number
}

export type Candidate = {
  id: number
  batch_code: string
  file_path: string
  orphan_reason: string
  size_bytes: number
  last_modified: string
  status: string
  receipt_id: number | null
  blocked_by_snapshot: string | null
  plan_id: number | null
  receipt_verdict: string | null
  reviewer: string | null
  currently_referenced: string | null
}

export type Receipt = {
  id: number
  batch_code: string
  file_path: string
  verdict: string
  reviewer: string
  remark: string
  received_at: string
  revision: number
}

export type SnapshotReference = {
  id: number
  snapshot_id: string
  dataset: string
  file_paths: string[]
  created_at: string
}

export type CleanupPlan = {
  id: number
  plan_name: string
  batch_code: string
  cleanup_job_id: number | null
  status: string
  created_at: string
  scan_credentials: Record<string, unknown>
  executable_paths: {
    file_path: string
    orphan_reason: string
    size_bytes: number
    receipt_id: number | null
    reviewer: string | null
  }[]
  blocked_paths: {
    file_path: string
    blocked_by_snapshot: string | null
    receipt_verdict: string | null
  }[]
  total_size_bytes: number
  executed_at?: string
}

export type AuditEvent = {
  id: number
  event_type: string
  batch_code: string | null
  file_path: string | null
  detail: string
  created_at: string
}

export type AuditNarrative = {
  batch_code: string
  generated_at: string
  summary: Record<string, number>
  statements: string[]
}

export type ReceiptImportResult = {
  ok: boolean
  message: string
  received_lines: number
  accepted: number
  rejected: number
  accepted_items: Receipt[]
  rejected_items: { scan_batch_id: string; file_path: string; 回执判定: string; error: string }[]
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(`${BASE}${path}`)
  if (!response.ok) throw new Error('巡检数据读取失败')
  return (await response.json()) as T
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(`${BASE}${path}`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  const data = (await response.json()) as T & { detail?: string }
  if (!response.ok) throw new Error((data as { detail?: string }).detail ?? '操作失败')
  return data
}

export const orphanReviewApi = {
  initDemo: () => postJson<{ ok: boolean; message: string }>('/demo/init', { reset: true }),

  listBatches: () => getJson<{ items: ScanBatch[] }>('/batches'),
  listCandidates: (batchCode?: string) =>
    getJson<{ items: Candidate[] }>(`/candidates${batchCode ? `?batch_code=${encodeURIComponent(batchCode)}` : ''}`),
  listReceipts: (batchCode?: string) =>
    getJson<{ items: Receipt[] }>(`/receipts${batchCode ? `?batch_code=${encodeURIComponent(batchCode)}` : ''}`),
  listSnapshotRefs: () => getJson<{ items: SnapshotReference[] }>('/snapshot-references'),
  listPlans: () => getJson<{ items: CleanupPlan[] }>('/cleanup-plans'),
  pendingChecklist: (batchCode?: string) =>
    getJson<{ batch_code: string; total_candidates: number; pending_count: number; items: Candidate[] }>(
      `/pending${batchCode ? `?batch_code=${encodeURIComponent(batchCode)}` : ''}`,
    ),
  auditTrail: (batchCode?: string) =>
    getJson<{ items: AuditEvent[] }>(`/audit-trail${batchCode ? `?batch_code=${encodeURIComponent(batchCode)}` : ''}`),
  auditNarrative: (batchCode: string) =>
    getJson<AuditNarrative>(`/audit-narrative?batch_code=${encodeURIComponent(batchCode)}`),

  registerSnapshotReference: (payload: { snapshot_id: string; file_paths: string[]; dataset?: string }) =>
    postJson<{ ok: boolean; message: string }>('/snapshot-references', payload),

  importReceipts: (receipts: { scan_batch_id: string; file_path: string; 回执判定: string; 核查人?: string; 核查备注?: string }[]) =>
    postJson<ReceiptImportResult>('/receipts/import', { receipts }),

  buildPlan: (planName: string, batchCode: string, cleanupJobId?: number) =>
    postJson<{ ok: boolean; message: string; entry: CleanupPlan }>('/cleanup-plans', {
      plan_name: planName,
      batch_code: batchCode,
      cleanup_job_id: cleanupJobId ?? null,
    }),

  executePlan: (planId: number) =>
    postJson<{ ok: boolean; message: string }>(`/cleanup-plans/${planId}/execute`, {}),

  checklistExportUrl: (batchCode: string) =>
    `${BASE}/checklist/export?batch_code=${encodeURIComponent(batchCode)}`,
  receiptTemplateUrl: () => `${BASE}/checklist/template`,
}
