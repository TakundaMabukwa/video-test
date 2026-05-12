#!/usr/bin/env node

const fs = require('fs')
const path = require('path')

function parseArgs(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || '')
    if (!token.startsWith('--')) {
      continue
    }
    const key = token.slice(2)
    const next = argv[i + 1]
    if (!next || String(next).startsWith('--')) {
      out[key] = true
      continue
    }
    out[key] = next
    i += 1
  }
  return out
}

function toPositiveInt(raw, fallback) {
  const value = Number(raw)
  if (!Number.isFinite(value) || value <= 0) {
    return fallback
  }
  return Math.round(value)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, '-')
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function toAbsUrl(baseUrl, maybeRelative) {
  if (!maybeRelative) {
    return null
  }
  if (/^https?:\/\//i.test(maybeRelative)) {
    return maybeRelative
  }
  return `${baseUrl}${String(maybeRelative).startsWith('/') ? '' : '/'}${maybeRelative}`
}

async function requestJson({
  baseUrl,
  method,
  route,
  body,
  timeoutMs,
  token,
}) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const headers = {
      Accept: 'application/json',
    }
    if (token) {
      headers['X-Internal-Token'] = token
    }
    const hasBody = body !== undefined && body !== null
    if (hasBody) {
      headers['Content-Type'] = 'application/json'
    }
    const response = await fetch(`${baseUrl}${route}`, {
      method,
      headers,
      body: hasBody ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    })
    const text = await response.text()
    let payload = null
    if (text) {
      try {
        payload = JSON.parse(text)
      } catch {
        payload = { raw: text }
      }
    }
    return {
      ok: response.ok,
      status: response.status,
      payload,
      route,
    }
  } catch (error) {
    return {
      ok: false,
      status: 0,
      payload: {
        success: false,
        message: error.message || String(error),
      },
      route,
    }
  } finally {
    clearTimeout(timeout)
  }
}

function hasStoredVideo(videosPayload) {
  if (!videosPayload || typeof videosPayload !== 'object') {
    return false
  }

  const totalVideos = Number(videosPayload.total_videos || 0)
  if (Number.isFinite(totalVideos) && totalVideos > 0) {
    return true
  }

  const linkedVideos = Number(videosPayload?.linked?.videosLinked || 0)
  if (Number.isFinite(linkedVideos) && linkedVideos > 0) {
    return true
  }

  const videos = videosPayload.videos || {}
  const keys = ['pre_event', 'post_event', 'camera_sd', 'camera_sd_pre', 'camera_sd_post']
  for (const key of keys) {
    const value = videos[key]
    if (!value || typeof value !== 'object') {
      continue
    }
    if (value.path) {
      return true
    }
  }

  const records = Array.isArray(videos.database_records) ? videos.database_records : []
  for (const record of records) {
    if (record && typeof record === 'object' && (record.path || record.filePath || record.url)) {
      return true
    }
  }

  return false
}

function pickVideoLinks(baseUrl, videosPayload) {
  const links = []
  if (!videosPayload || typeof videosPayload !== 'object') {
    return links
  }
  const videos = videosPayload.videos || {}
  for (const key of ['pre_event', 'post_event', 'camera_sd']) {
    const entry = videos[key]
    if (!entry || typeof entry !== 'object') {
      continue
    }
    if (entry.url) {
      links.push(toAbsUrl(baseUrl, entry.url))
    } else if (entry.raw_url) {
      links.push(toAbsUrl(baseUrl, entry.raw_url))
    }
  }
  const dbRows = Array.isArray(videos.database_records) ? videos.database_records : []
  for (const row of dbRows) {
    if (row?.url) {
      links.push(toAbsUrl(baseUrl, row.url))
    } else if (row?.raw_url) {
      links.push(toAbsUrl(baseUrl, row.raw_url))
    }
  }
  return [...new Set(links.filter(Boolean))]
}

async function runPool(items, concurrency, worker) {
  const results = new Array(items.length)
  let next = 0
  async function runOne() {
    while (true) {
      const index = next
      next += 1
      if (index >= items.length) {
        return
      }
      // eslint-disable-next-line no-await-in-loop
      results[index] = await worker(items[index], index)
    }
  }
  const runners = []
  for (let i = 0; i < concurrency; i += 1) {
    runners.push(runOne())
  }
  await Promise.all(runners)
  return results
}

function unresolvedOnly(alerts) {
  return alerts.filter((row) => {
    const status = String(row?.status || '').trim().toLowerCase()
    return status !== 'resolved' && status !== 'closed'
  })
}

function toSummaryRows(baseUrl, rows) {
  return rows.map((row) => ({
    id: row.id,
    vehicleId: row.vehicleId,
    status: row.status,
    priority: row.priority,
    type: row.type,
    timestamp: row.timestamp,
    collectEvidenceOk: row.collectEvidenceOk,
    collectEvidenceMessage: row.collectEvidenceMessage,
    reportVideoOk: row.reportVideoOk,
    reportVideoMessage: row.reportVideoMessage,
    reportQueued: row.reportQueued,
    hasVideoLinks: row.hasVideoLinks,
    videoLinks: row.videoLinks,
    videosEndpoint: `${baseUrl}/api/alerts/${row.id}/videos`,
    mediaEndpoint: `${baseUrl}/api/alerts/${row.id}/media`,
  }))
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const baseUrl = String(
    args.base ||
      process.env.ALERT_HUB_BASE_URL ||
      process.env.NEXT_PUBLIC_ALERT_HUB_BASE_URL ||
      'http://46.101.219.78:3100',
  ).trim().replace(/\/+$/, '')
  const token = String(args.token || process.env.ALERT_HUB_TOKEN || '').trim() || null

  const limit = toPositiveInt(args.limit, 100)
  const concurrency = toPositiveInt(args.concurrency, 4)
  const timeoutMs = toPositiveInt(args.timeoutMs, 25000)
  const pollRounds = Number.isFinite(Number(args.pollRounds))
    ? Math.max(0, Number(args.pollRounds))
    : 2
  const pollDelayMs = toPositiveInt(args.pollDelayMs, 15000)
  const includeReportRequest = String(args.requestReport || 'true').toLowerCase() !== 'false'

  const activeResp = await requestJson({
    baseUrl,
    method: 'GET',
    route: '/api/alerts/active',
    timeoutMs,
    token,
  })
  if (!activeResp.ok || !Array.isArray(activeResp?.payload?.alerts)) {
    console.error('[ensure-alert-media] failed to load active alerts')
    console.error(JSON.stringify(activeResp, null, 2))
    process.exit(1)
  }

  const unresolved = unresolvedOnly(activeResp.payload.alerts).slice(0, limit)
  console.log(
    `[ensure-alert-media] base=${baseUrl} unresolved=${unresolved.length} limit=${limit} concurrency=${concurrency}`,
  )

  const rows = await runPool(unresolved, concurrency, async (alert) => {
    const id = String(alert?.id || '').trim()
    const row = {
      id,
      vehicleId: String(alert?.vehicleId || alert?.device_id || '').trim(),
      status: String(alert?.status || '').trim(),
      priority: String(alert?.priority || '').trim(),
      type: String(alert?.type || alert?.alert_type || '').trim(),
      timestamp: String(alert?.timestamp || '').trim(),
      collectEvidenceOk: false,
      collectEvidenceMessage: null,
      reportVideoOk: false,
      reportVideoMessage: null,
      reportQueued: false,
      hasVideoLinks: false,
      videoLinks: [],
      pollChecks: [],
    }

    const collectResp = await requestJson({
      baseUrl,
      method: 'POST',
      route: `/api/alerts/${encodeURIComponent(id)}/collect-evidence`,
      body: { force: true },
      timeoutMs,
      token,
    })
    row.collectEvidenceOk = collectResp.ok && collectResp?.payload?.success !== false
    row.collectEvidenceMessage = collectResp?.payload?.message || `HTTP ${collectResp.status}`

    const checkVideos = async (attempt) => {
      const videosResp = await requestJson({
        baseUrl,
        method: 'GET',
        route: `/api/alerts/${encodeURIComponent(id)}/videos?ensureMedia=true`,
        timeoutMs,
        token,
      })
      const payload = videosResp.payload || {}
      const hasLinks = hasStoredVideo(payload)
      const links = pickVideoLinks(baseUrl, payload)
      row.pollChecks.push({
        attempt,
        at: new Date().toISOString(),
        ok: videosResp.ok,
        status: videosResp.status,
        hasVideoLinks: hasLinks,
        linkedVideos: Number(payload?.linked?.videosLinked || 0),
        totalVideos: Number(payload?.total_videos || 0),
        preferredSource: String(payload?.preferred_source || ''),
      })
      if (hasLinks) {
        row.hasVideoLinks = true
        row.videoLinks = links
      }
      return hasLinks
    }

    let done = await checkVideos(0)
    for (let i = 1; !done && i <= pollRounds; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await sleep(pollDelayMs)
      // eslint-disable-next-line no-await-in-loop
      done = await checkVideos(i)
    }

    if (!row.hasVideoLinks && includeReportRequest) {
      const reportResp = await requestJson({
        baseUrl,
        method: 'POST',
        route: `/api/alerts/${encodeURIComponent(id)}/request-report-video`,
        body: { force: true },
        timeoutMs,
        token,
      })
      row.reportVideoOk = reportResp.ok && reportResp?.payload?.success !== false
      row.reportVideoMessage = reportResp?.payload?.message || `HTTP ${reportResp.status}`
      row.reportQueued = Boolean(reportResp?.payload?.data?.requestQueued)
    }

    return row
  })

  const summaries = toSummaryRows(baseUrl, rows)
  const output = {
    checkedAt: new Date().toISOString(),
    baseUrl,
    totals: {
      unresolvedFetched: unresolved.length,
      collectEvidenceOk: summaries.filter((r) => r.collectEvidenceOk).length,
      hasVideoLinks: summaries.filter((r) => r.hasVideoLinks).length,
      reportVideoOk: summaries.filter((r) => r.reportVideoOk).length,
      reportQueued: summaries.filter((r) => r.reportQueued).length,
    },
    rows: summaries,
    raw: rows,
  }

  const outArg = String(args.out || '').trim()
  const outPath = outArg
    ? path.resolve(outArg)
    : path.join(process.cwd(), 'runtime', 'alert-captures', `ensure-alert-media-${nowStamp()}.json`)
  ensureDir(path.dirname(outPath))
  fs.writeFileSync(outPath, JSON.stringify(output, null, 2))
  const latestPath = path.join(path.dirname(outPath), 'ensure-alert-media-latest.json')
  fs.writeFileSync(latestPath, JSON.stringify(output, null, 2))

  console.log(`[ensure-alert-media] wrote ${outPath}`)
  console.log(`[ensure-alert-media] wrote ${latestPath}`)
  console.log(
    `[ensure-alert-media] totals unresolved=${output.totals.unresolvedFetched} collect_ok=${output.totals.collectEvidenceOk} with_links=${output.totals.hasVideoLinks} report_ok=${output.totals.reportVideoOk} report_queued=${output.totals.reportQueued}`,
  )
}

main().catch((error) => {
  console.error('[ensure-alert-media] fatal:', error.message || String(error))
  process.exit(1)
})
