<template>
  <section class="page orphan-review" data-testid="orphan-review">
    <header class="page-head">
      <div>
        <h2>湖仓审计 · 孤儿文件巡检</h2>
        <p class="page-desc">
          核查 Parquet 数据路径：核查单导出 → 分批逐行回执 → 当前快照引用约束 → 清理计划 → 审计留痕。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="demo-init" type="button" @click="resetDemo">
          重置演示场景
        </button>
        <button class="btn primary" data-testid="refresh" type="button" @click="reloadAll">
          刷新巡检数据
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error-banner">{{ errorMessage }}</p>
    <p v-if="infoMessage" class="info-text" data-testid="info-banner">{{ infoMessage }}</p>

    <!-- 批次选择与扫描凭据 -->
    <div class="panel">
      <h3>① 扫描批次与扫描凭据</h3>
      <div class="filter-bar">
        <label class="filter-item">
          <span>扫描批次</span>
          <select v-model="selectedBatch" data-testid="batch-select" @change="onBatchChange">
            <option v-for="batch in batches" :key="batch.batch_code" :value="batch.batch_code">
              {{ batch.batch_code }}（待核 {{ batch.pending_count ?? 0 }} / 共 {{ batch.candidate_count ?? 0 }}）
            </option>
          </select>
        </label>
        <button class="btn" data-testid="export-checklist" type="button" :disabled="!selectedBatch" @click="downloadChecklist">
          导出核查单 CSV
        </button>
        <a class="btn ghost" data-testid="receipt-template" :href="orphanReviewApi.receiptTemplateUrl()">
          下载回执模板
        </a>
      </div>
      <table v-if="currentBatch" class="data-table compact" data-testid="batch-credentials">
        <tbody>
          <tr><th>扫描时间</th><td>{{ currentBatch.scanned_at }}</td><th>扫描器版本</th><td>{{ currentBatch.scanner_version }}</td></tr>
          <tr><th>Manifest</th><td>{{ currentBatch.manifest_uri }}</td><th>校验和 SHA256</th><td class="mono">{{ currentBatch.manifest_sha256 }}</td></tr>
          <tr><th>扫描作业</th><td>{{ currentBatch.scan_job_id }}</td><th>基线快照</th><td>{{ currentBatch.baseline_snapshot_ids.join('、') || '无' }}</td></tr>
        </tbody>
      </table>
    </div>

    <!-- 核查单 -->
    <div class="panel">
      <h3>② 核查单（扫描批次 / 文件全路径 / 判作孤儿的依据）</h3>
      <table class="data-table" data-testid="candidate-table">
        <thead>
          <tr>
            <th>扫描批次</th><th>文件全路径</th><th>判作孤儿的依据</th>
            <th>大小</th><th>回执判定</th><th>当前快照引用</th><th>核查单状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in candidates" :key="item.id" :data-testid="`candidate-${item.id}`" :data-status="item.status">
            <td>{{ item.batch_code }}</td>
            <td class="mono path-cell">{{ item.file_path }}</td>
            <td>{{ item.orphan_reason }}</td>
            <td>{{ formatBytes(item.size_bytes) }}</td>
            <td>
              <span v-if="item.receipt_verdict">{{ item.receipt_verdict }}（{{ item.reviewer || '未署名' }}）</span>
              <span v-else class="muted">未回执</span>
            </td>
            <td>
              <span v-if="item.currently_referenced" class="tag warn" data-testid="snapshot-block-tag">
                {{ item.currently_referenced }}
              </span>
              <span v-else class="muted">无</span>
            </td>
            <td>
              <span class="tag" :class="statusClass(item.status)">{{ item.status }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 回执导入 -->
    <div class="panel">
      <h3>③ 逐行回执导入（可分几次回传）</h3>
      <form class="receipt-form" @submit.prevent="submitReceipt">
        <label class="filter-item">
          <span>文件全路径</span>
          <select v-model="receiptForm.file_path" data-testid="receipt-path">
            <option value="" disabled>选择核查单路径</option>
            <option v-for="item in candidates" :key="item.id" :value="item.file_path">{{ item.file_path }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>回执判定</span>
          <select v-model="receiptForm.verdict" data-testid="receipt-verdict">
            <option value="确认孤儿">确认孤儿</option>
            <option value="非孤儿">非孤儿</option>
            <option value="存疑">存疑</option>
          </select>
        </label>
        <label class="filter-item">
          <span>核查人</span>
          <input v-model="receiptForm.reviewer" data-testid="receipt-reviewer" placeholder="审计人工号" />
        </label>
        <label class="filter-item grow">
          <span>核查备注</span>
          <input v-model="receiptForm.remark" data-testid="receipt-remark" placeholder="核查情况说明" />
        </label>
        <button class="btn primary" data-testid="submit-receipt" type="submit">签收回执</button>
      </form>
      <div class="receipt-form">
        <label class="filter-item grow">
          <span>批量粘贴 CSV（首行表头：scan_batch_id,file_path,回执判定,核查人,核查备注）</span>
          <textarea v-model="receiptCsv" rows="4" data-testid="receipt-csv"
            placeholder="SCAN-20261123-01,/data/lakehouse/xxx.parquet,确认孤儿,auditor_li,现场核验"></textarea>
        </label>
        <button class="btn" data-testid="upload-receipt-csv" type="button" @click="submitCsvReceipt">
          导入 CSV 回执
        </button>
      </div>
      <div v-if="lastImport" class="import-summary" data-testid="import-summary">
        上次导入：接收 {{ lastImport.received_lines }} 行，归档 {{ lastImport.accepted }} 行，拒收 {{ lastImport.rejected }} 行
        <ul v-if="lastImport.rejected_items.length">
          <li v-for="(item, idx) in lastImport.rejected_items" :key="idx" class="error-text">
            {{ item.file_path || item.scan_batch_id }}：{{ item.error }}
          </li>
        </ul>
      </div>
    </div>

    <!-- 快照引用 -->
    <div class="panel">
      <h3>④ 当前快照引用（可执行范围的外部约束）</h3>
      <table class="data-table compact" data-testid="snapshot-table">
        <thead>
          <tr><th>快照 ID</th><th>数据集</th><th>引用文件全路径</th><th>登记时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="ref in snapshotRefs" :key="ref.id">
            <td class="mono">{{ ref.snapshot_id }}</td>
            <td>{{ ref.dataset || '—' }}</td>
            <td class="mono path-cell">{{ ref.file_paths.join('；') }}</td>
            <td>{{ ref.created_at }}</td>
          </tr>
          <tr v-if="!snapshotRefs.length"><td colspan="4" class="empty-state">当前无快照引用登记</td></tr>
        </tbody>
      </table>
    </div>

    <!-- 待核清单与清理计划 -->
    <div class="panel">
      <h3>⑤ 待核清单与清理计划</h3>
      <p class="muted">
        待核 {{ pending.pending_count ?? 0 }} 项：未收到回执、存疑待复核、被快照引用拦截的路径继续留在此清单，进入下一轮巡检。
      </p>
      <ul class="pending-list" data-testid="pending-list">
        <li v-for="item in pending.items" :key="item.id">
          <span class="tag" :class="statusClass(item.status)">{{ item.status }}</span>
          <span class="mono">{{ item.file_path }}</span>
          <span v-if="item.blocked_by_snapshot" class="muted">（拦截快照：{{ item.blocked_by_snapshot }}）</span>
        </li>
        <li v-if="!pending.items?.length" class="empty-state">待核清单已清空</li>
      </ul>
      <div class="filter-bar">
        <button class="btn primary" data-testid="build-plan" type="button" :disabled="!selectedBatch" @click="buildPlan">
          按回执生成清理计划
        </button>
        <span class="muted">仅「确认孤儿」且无当前快照引用的路径进入可执行范围</span>
      </div>
      <article v-for="plan in plans" :key="plan.id" class="plan-card" :data-testid="`plan-${plan.id}`">
        <header>
          <strong>{{ plan.plan_name }}</strong>
          <span class="tag" :class="plan.status === '已执行' ? 'ok' : ''">{{ plan.status }}</span>
          <span class="muted">批次 {{ plan.batch_code }} · 生成于 {{ plan.created_at }}</span>
          <button v-if="plan.status !== '已执行'" class="link" type="button" @click="executePlan(plan.id)">
            标记执行完成
          </button>
        </header>
        <p class="muted small">
          扫描凭据：{{ plan.scan_credentials.manifest_uri }}（sha256: {{ plan.scan_credentials.manifest_sha256 }}）
        </p>
        <table class="data-table compact">
          <thead><tr><th>可执行路径（{{ plan.executable_paths.length }}）</th><th>依据</th><th>回执核查人</th></tr></thead>
          <tbody>
            <tr v-for="path in plan.executable_paths" :key="path.file_path" data-testid="plan-executable-row">
              <td class="mono path-cell">{{ path.file_path }}</td><td>{{ path.orphan_reason }}</td><td>{{ path.reviewer || '—' }}</td>
            </tr>
            <tr v-if="!plan.executable_paths.length"><td colspan="3" class="empty-state">本轮无可执行路径</td></tr>
          </tbody>
        </table>
        <table v-if="plan.blocked_paths.length" class="data-table compact">
          <thead><tr><th>快照拦截路径（{{ plan.blocked_paths.length }}）</th><th>拦截快照</th><th>回执判定</th></tr></thead>
          <tbody>
            <tr v-for="path in plan.blocked_paths" :key="path.file_path" data-testid="plan-blocked-row">
              <td class="mono path-cell">{{ path.file_path }}</td>
              <td><span class="tag warn">{{ path.blocked_by_snapshot }}</span></td>
              <td>{{ path.receipt_verdict || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </article>
    </div>

    <!-- 审计记录 -->
    <div class="panel">
      <h3>⑥ 审计记录（新快照引用如何影响原意见及本轮处置）</h3>
      <button class="btn" data-testid="load-narrative" type="button" :disabled="!selectedBatch" @click="loadNarrative">
        生成审计说明
      </button>
      <div v-if="narrative" class="narrative" data-testid="narrative">
        <p v-for="(statement, idx) in narrative.statements" :key="idx" class="narrative-item">{{ statement }}</p>
      </div>
      <table class="data-table compact" data-testid="audit-trail">
        <thead><tr><th>时间</th><th>事件</th><th>批次</th><th>文件路径</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="event in auditEvents" :key="event.id">
            <td class="small">{{ event.created_at }}</td>
            <td class="mono small">{{ event.event_type }}</td>
            <td>{{ event.batch_code || '—' }}</td>
            <td class="mono path-cell small">{{ event.file_path || '—' }}</td>
            <td class="small">{{ event.detail }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  orphanReviewApi,
  type AuditEvent,
  type AuditNarrative,
  type Candidate,
  type CleanupPlan,
  type ReceiptImportResult,
  type ScanBatch,
  type SnapshotReference,
} from '@/api/orphan-review'

const batches = ref<ScanBatch[]>([])
const candidates = ref<Candidate[]>([])
const snapshotRefs = ref<SnapshotReference[]>([])
const plans = ref<CleanupPlan[]>([])
const auditEvents = ref<AuditEvent[]>([])
const pending = ref<{ pending_count: number; items: Candidate[] }>({ pending_count: 0, items: [] })
const narrative = ref<AuditNarrative | null>(null)
const selectedBatch = ref('')
const errorMessage = ref('')
const infoMessage = ref('')
const lastImport = ref<ReceiptImportResult | null>(null)
const receiptCsv = ref('')
const receiptForm = ref({ file_path: '', verdict: '确认孤儿', reviewer: '', remark: '' })

function statusClass(status: string): string {
  if (status === '已确认孤儿' || status === '已纳入清理计划') return 'ok'
  if (status === '快照引用拦截') return 'warn'
  if (status === '已排除') return 'muted-tag'
  if (status === '存疑待复核') return 'doubt'
  return ''
}

function formatBytes(value: number): string {
  if (!value) return '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = value
  let unit = 0
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024
    unit += 1
  }
  return `${size.toFixed(1)} ${units[unit]}`
}

const currentBatch = computed(() =>
  batches.value.find((batch) => batch.batch_code === selectedBatch.value) ?? null,
)

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [batchData, candidateData, refData, planData, trailData] = await Promise.all([
      orphanReviewApi.listBatches(),
      orphanReviewApi.listCandidates(selectedBatch.value || undefined),
      orphanReviewApi.listSnapshotRefs(),
      orphanReviewApi.listPlans(),
      orphanReviewApi.auditTrail(selectedBatch.value || undefined),
    ])
    batches.value = batchData.items
    if (!selectedBatch.value && batches.value.length) {
      selectedBatch.value = batches.value[0].batch_code
    }
    candidates.value = candidateData.items
    snapshotRefs.value = refData.items
    plans.value = planData.items
    auditEvents.value = trailData.items
    if (selectedBatch.value) {
      pending.value = await orphanReviewApi.pendingChecklist(selectedBatch.value)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '巡检数据读取失败'
  }
}

async function onBatchChange() {
  narrative.value = null
  await reloadAll()
}

async function resetDemo() {
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const result = await orphanReviewApi.initDemo()
    infoMessage.value = result.message
    narrative.value = null
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '演示场景初始化失败'
  }
}

function downloadChecklist() {
  if (!selectedBatch.value) return
  window.open(orphanReviewApi.checklistExportUrl(selectedBatch.value), '_blank')
}

async function submitReceipt() {
  if (!selectedBatch.value || !receiptForm.value.file_path) {
    errorMessage.value = '请先选择核查单路径'
    return
  }
  errorMessage.value = ''
  try {
    lastImport.value = await orphanReviewApi.importReceipts([{
      scan_batch_id: selectedBatch.value,
      file_path: receiptForm.value.file_path,
      回执判定: receiptForm.value.verdict,
      核查人: receiptForm.value.reviewer,
      核查备注: receiptForm.value.remark,
    }])
    infoMessage.value = lastImport.value.message
    receiptForm.value = { file_path: '', verdict: '确认孤儿', reviewer: '', remark: '' }
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执签收失败'
  }
}

async function submitCsvReceipt() {
  errorMessage.value = ''
  if (!receiptCsv.value.trim()) {
    errorMessage.value = '请先粘贴 CSV 内容'
    return
  }
  try {
    const response = await fetch('/api/orphan-review/receipts/import-csv', {
      method: 'POST',
      headers: { 'Content-Type': 'text/csv; charset=utf-8' },
      body: receiptCsv.value,
    })
    const result = (await response.json()) as ReceiptImportResult
    lastImport.value = result
    if (!response.ok) throw new Error(result.message ?? 'CSV 回执导入失败')
    infoMessage.value = result.message
    receiptCsv.value = ''
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'CSV 回执导入失败'
  }
}

async function buildPlan() {
  if (!selectedBatch.value) return
  errorMessage.value = ''
  try {
    const result = await orphanReviewApi.buildPlan(
      `plan-orphan-${selectedBatch.value}`,
      selectedBatch.value,
    )
    infoMessage.value = result.message
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '清理计划生成失败'
  }
}

async function executePlan(planId: number) {
  errorMessage.value = ''
  try {
    const result = await orphanReviewApi.executePlan(planId)
    infoMessage.value = result.message
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '计划执行回填失败'
  }
}

async function loadNarrative() {
  if (!selectedBatch.value) return
  errorMessage.value = ''
  try {
    narrative.value = await orphanReviewApi.auditNarrative(selectedBatch.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '审计说明生成失败'
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.orphan-review .panel {
  background: var(--card-bg, #fff);
  border: 1px solid var(--border-color, #e4e7ec);
  border-radius: 10px;
  padding: 16px;
  margin-bottom: 18px;
}
.orphan-review .panel h3 {
  margin: 0 0 12px;
  font-size: 15px;
}
.orphan-review .compact {
  font-size: 12.5px;
}
.orphan-review .mono {
  font-family: 'SFMono-Regular', Consolas, monospace;
}
.orphan-review .path-cell {
  max-width: 420px;
  word-break: break-all;
}
.orphan-review .muted {
  color: #98a2b3;
}
.orphan-review .small {
  font-size: 12px;
}
.orphan-review .tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 999px;
  background: #f2f4f7;
  color: #475467;
  font-size: 12px;
  white-space: nowrap;
}
.orphan-review .tag.ok {
  background: #ecfdf3;
  color: #027a48;
}
.orphan-review .tag.warn {
  background: #fffaeb;
  color: #b54708;
}
.orphan-review .tag.doubt {
  background: #eff4ff;
  color: #1d4ed8;
}
.orphan-review .tag.muted-tag {
  background: #f2f4f7;
  color: #98a2b3;
}
.orphan-review .receipt-form {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: flex-end;
  margin-bottom: 12px;
}
.orphan-review .receipt-form .grow {
  flex: 1 1 260px;
}
.orphan-review textarea {
  width: 100%;
  font-family: 'SFMono-Regular', Consolas, monospace;
  font-size: 12.5px;
}
.orphan-review .import-summary {
  background: #f9fafb;
  border-radius: 8px;
  padding: 10px 12px;
  font-size: 13px;
}
.orphan-review .pending-list {
  list-style: none;
  padding: 0;
  margin: 0 0 12px;
  display: grid;
  gap: 6px;
}
.orphan-review .pending-list li {
  display: flex;
  gap: 10px;
  align-items: center;
  font-size: 13px;
}
.orphan-review .plan-card {
  border: 1px solid #e4e7ec;
  border-radius: 10px;
  padding: 12px 14px;
  margin-top: 12px;
}
.orphan-review .plan-card header {
  display: flex;
  gap: 12px;
  align-items: center;
  margin-bottom: 8px;
}
.orphan-review .narrative {
  background: #f9fafb;
  border-left: 3px solid #1d4ed8;
  border-radius: 6px;
  padding: 10px 14px;
  margin: 10px 0;
}
.orphan-review .narrative-item {
  margin: 6px 0;
  font-size: 13px;
  line-height: 1.6;
}
.orphan-review .info-text {
  color: #027a48;
}
</style>
