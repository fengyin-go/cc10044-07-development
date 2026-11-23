import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import OrphanReviewView from '@/views/OrphanReviewView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 固定路径需排在动态 :moduleKey 之前，避免被通用工作台抢占匹配。
    { path: '/data_lake_audit/orphan-review', name: 'orphan-review', component: OrphanReviewView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
