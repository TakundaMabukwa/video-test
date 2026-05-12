const fs = require('fs')
const path = require('path')
const config = require('./config')
const { summarizeVehicleApproxCoverage } = require('./storage')

const ALERT_CAPTURE_ROOT = path.join(process.cwd(), 'runtime', 'alert-captures')
const ALERT_CAPTURE_JOB_ROOT = path.join(ALERT_CAPTURE_ROOT, 'jobs')
const ALERT_CAPTURE_MEDIA_ROOT = path.join(ALERT_CAPTURE_ROOT, 'media')

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function parseTimestampMs(value) {
  const numeric = Number(value)
  if (Number.isFinite(numeric) && String(value).trim() !== '') {
    return numeric
  }
  const parsed = new Date(value).getTime()
  return Number.isFinite(parsed) ? parsed : null
}

function sanitizePathSegment(value, fallback = 'unknown') {
  const normalized = String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '')
  return normalized || fallback
}

function clampPositiveInt(value, fallback) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback
  }
  return Math.round(parsed)
}

function loadJson(filePath) {
  try {
    if (!fs.existsSync(filePath)) {
      return null
    }
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

class AlertCaptureManager {
  constructor({ exportController }) {
    this.exportController = exportController
    this.jobs = new Map()
    this.timers = new Map()

    ensureDir(ALERT_CAPTURE_ROOT)
    ensureDir(ALERT_CAPTURE_JOB_ROOT)
    ensureDir(ALERT_CAPTURE_MEDIA_ROOT)
  }

  getJobPath(alertId) {
    return path.join(ALERT_CAPTURE_JOB_ROOT, `${sanitizePathSegment(alertId)}.json`)
  }

  getMediaRoot(alertId) {
    return path.join(ALERT_CAPTURE_MEDIA_ROOT, sanitizePathSegment(alertId))
  }

  readJob(alertId) {
    const fromMemory = this.jobs.get(alertId)
    if (fromMemory) {
      return fromMemory
    }
    const fromDisk = loadJson(this.getJobPath(alertId))
    if (fromDisk) {
      this.jobs.set(alertId, fromDisk)
      return fromDisk
    }
    return null
  }

  saveJob(job) {
    job.updatedAt = new Date().toISOString()
    this.jobs.set(job.alertId, job)
    fs.writeFileSync(this.getJobPath(job.alertId), JSON.stringify(job, null, 2))
  }

  normalizeChannels(rawChannels, fallbackChannel = 1) {
    const values = Array.isArray(rawChannels)
      ? rawChannels
      : String(rawChannels || '')
          .split(',')
          .map((value) => Number(String(value).trim()))
    const normalized = [...new Set(values.filter((value) => Number.isFinite(value) && value > 0))]
    if (normalized.length > 0) {
      return normalized
    }
    return [clampPositiveInt(fallbackChannel, 1)]
  }

  mergeChannels(...channelLists) {
    const merged = []
    for (const list of channelLists) {
      if (!Array.isArray(list)) {
        continue
      }
      for (const channel of list) {
        if (Number.isFinite(channel) && channel > 0) {
          merged.push(Number(channel))
        }
      }
    }
    return [...new Set(merged)]
  }

  normalizeRequest(input, existingJob = null) {
    const alertId = String(input?.alertId || existingJob?.alertId || '').trim()
    const vehicleId = String(input?.vehicleId || existingJob?.vehicleId || '').trim()
    const timestampMs =
      parseTimestampMs(input?.timestampMs ?? input?.timestamp ?? existingJob?.timestampMs) ??
      parseTimestampMs(existingJob?.timestamp)
    if (!alertId || !vehicleId || timestampMs === null) {
      throw new Error('alertId, vehicleId and timestamp are required')
    }

    const defaultPreRollMs = clampPositiveInt(
      config.alertVideoCapturePreRollMs,
      30000,
    )
    const defaultPostRollMs = clampPositiveInt(
      config.alertVideoCapturePostRollMs,
      30000,
    )

    const channel = clampPositiveInt(input?.channel ?? existingJob?.channel, 1)
    const requestedChannels = this.normalizeChannels(
      input?.channels ?? existingJob?.channels,
      channel,
    )
    const requiredChannels = this.normalizeChannels(
      config.alertVideoCaptureChannels,
      channel,
    )
    const channels = this.mergeChannels(requestedChannels, requiredChannels)

    return {
      alertId,
      vehicleId,
      channel,
      channels,
      timestampMs,
      timestamp: new Date(timestampMs).toISOString(),
      preRollMs: clampPositiveInt(
        input?.preRollMs ?? existingJob?.preRollMs,
        defaultPreRollMs,
      ),
      postRollMs: clampPositiveInt(
        input?.postRollMs ?? existingJob?.postRollMs,
        defaultPostRollMs,
      ),
      maxWaitMs: clampPositiveInt(
        input?.maxWaitMs ?? existingJob?.maxWaitMs,
        clampPositiveInt(config.alertVideoCaptureWaitTimeoutMs, 180000),
      ),
      retryIntervalMs: clampPositiveInt(
        input?.retryIntervalMs ?? existingJob?.retryIntervalMs,
        clampPositiveInt(config.alertVideoCaptureRetryIntervalMs, 5000),
      ),
      type: String(input?.type || existingJob?.type || '').trim() || null,
      priority: String(input?.priority || existingJob?.priority || '').trim() || null,
      metadata: input?.metadata || existingJob?.metadata || null,
    }
  }

  queueCapture(input) {
    if (!config.alertVideoCaptureEnabled) {
      throw new Error('Alert video capture is disabled')
    }

    const existing = this.readJob(String(input?.alertId || '').trim())
    const normalized = this.normalizeRequest(input, existing)
    const job = {
      ...(existing || {}),
      ...normalized,
      status: existing?.status || 'pending',
      createdAt: existing?.createdAt || new Date().toISOString(),
      updatedAt: existing?.updatedAt || new Date().toISOString(),
      startedAt: existing?.startedAt || null,
      completedAt: existing?.completedAt || null,
      attempts: Number(existing?.attempts || 0),
      lastAttemptAt: existing?.lastAttemptAt || null,
      nextAttemptAt: existing?.nextAttemptAt || null,
      results: Array.isArray(existing?.results) ? existing.results : [],
      lastError: existing?.lastError || null,
      mediaRoot: `/media/alert-captures/${encodeURIComponent(normalized.alertId)}`,
    }

    if (job.status === 'completed') {
      this.saveJob(job)
      return job
    }

    job.status = 'pending'
    job.completedAt = null
    this.saveJob(job)
    this.scheduleJob(job.alertId, 0)
    return job
  }

  scheduleJob(alertId, delayMs) {
    const existingTimer = this.timers.get(alertId)
    if (existingTimer) {
      clearTimeout(existingTimer)
    }

    const job = this.readJob(alertId)
    if (!job) {
      return
    }

    const safeDelayMs = Math.max(0, Math.min(delayMs, 60 * 60 * 1000))
    job.nextAttemptAt = new Date(Date.now() + safeDelayMs).toISOString()
    this.saveJob(job)

    const timer = setTimeout(() => {
      this.timers.delete(alertId)
      void this.runJob(alertId)
    }, safeDelayMs)
    this.timers.set(alertId, timer)
  }

  async runJob(alertId) {
    const job = this.readJob(alertId)
    if (!job || job.status === 'completed' || job.status === 'failed') {
      return
    }

    const now = Date.now()
    const captureReadyAt = job.timestampMs + job.postRollMs
    const deadlineMs = captureReadyAt + job.maxWaitMs

    if (now < captureReadyAt) {
      job.status = 'waiting_for_postroll'
      this.saveJob(job)
      this.scheduleJob(alertId, captureReadyAt - now + 500)
      return
    }

    job.status = 'running'
    job.startedAt = job.startedAt || new Date().toISOString()
    job.lastAttemptAt = new Date().toISOString()
    job.attempts = Number(job.attempts || 0) + 1
    job.lastError = null
    this.saveJob(job)

    const fromIso = new Date(job.timestampMs - job.preRollMs).toISOString()
    const toIso = new Date(job.timestampMs + job.postRollMs).toISOString()
    const results = []
    let successCount = 0

    for (const channel of job.channels) {
      const exportAttempt = await this.exportWithFallback(job, channel, fromIso, toIso)
      if (exportAttempt.success) {
        const exportResult = exportAttempt.exportResult
        const materialized = this.materializeChannelArtifacts(job, channel, exportResult)
        successCount += 1
        results.push({
          channel,
          success: true,
          packetCount: exportResult.packetCount,
          byteCount: exportResult.byteCount,
          frameCount: exportResult.frameCount,
          frameRate: exportResult.frameRate,
          exportFrom: exportResult.exportFrom,
          exportTo: exportResult.exportTo,
          transcodeError: exportResult.transcodeError,
          fallbackUsed: !!exportAttempt.fallbackUsed,
          fallbackWindow: exportAttempt.fallbackWindow || null,
          ...materialized,
        })
      } else {
        const error = exportAttempt.error
        results.push({
          channel,
          success: false,
          fallbackUsed: !!exportAttempt.fallbackUsed,
          fallbackWindow: exportAttempt.fallbackWindow || null,
          message: error.message || String(error),
        })
      }
    }

    job.results = results

    if (successCount === job.channels.length) {
      job.status = 'completed'
      job.completedAt = new Date().toISOString()
      job.lastError = null
      this.saveJob(job)
      return
    }

    if (Date.now() < deadlineMs) {
      job.status = successCount > 0 ? 'partial_pending' : 'pending'
      job.lastError = results
        .filter((result) => !result.success)
        .map((result) => result.message)
        .filter(Boolean)
        .join(' | ') || null
      this.saveJob(job)
      this.scheduleJob(alertId, job.retryIntervalMs)
      return
    }

    job.status = successCount > 0 ? 'partial' : 'failed'
    job.completedAt = new Date().toISOString()
    job.lastError = results
      .filter((result) => !result.success)
      .map((result) => result.message)
      .filter(Boolean)
      .join(' | ') || null
    this.saveJob(job)
  }

  materializeChannelArtifacts(job, channel, exportResult) {
    const channelDir = path.join(this.getMediaRoot(job.alertId), `ch${channel}`)
    ensureDir(channelDir)

    const manifest = {
      alertId: job.alertId,
      vehicleId: job.vehicleId,
      channel,
      type: job.type,
      priority: job.priority,
      timestamp: job.timestamp,
      requestedWindow: {
        from: new Date(job.timestampMs - job.preRollMs).toISOString(),
        to: new Date(job.timestampMs + job.postRollMs).toISOString(),
      },
      exportWindow: {
        from: exportResult.exportFrom,
        to: exportResult.exportTo,
      },
      packetCount: exportResult.packetCount,
      byteCount: exportResult.byteCount,
      frameCount: exportResult.frameCount,
      frameRate: exportResult.frameRate,
      transcodeError: exportResult.transcodeError,
      capturedAt: new Date().toISOString(),
    }

    const files = {}
    for (const [key, sourcePath, fileName] of [
      ['rawPacketsUrl', exportResult.rawPacketsPath, 'event.packets.bin'],
      ['h264Url', exportResult.h264Path, 'event.h264'],
      ['mp4Url', exportResult.mp4Path, 'event.mp4'],
    ]) {
      if (!sourcePath || !fs.existsSync(sourcePath)) {
        continue
      }
      const destPath = path.join(channelDir, fileName)
      fs.copyFileSync(sourcePath, destPath)
      files[key] = `/media/alert-captures/${encodeURIComponent(job.alertId)}/ch${channel}/${fileName}`
    }

    fs.writeFileSync(
      path.join(channelDir, 'manifest.json'),
      JSON.stringify({ ...manifest, files }, null, 2),
    )

    return {
      ...files,
      manifestUrl: `/media/alert-captures/${encodeURIComponent(job.alertId)}/ch${channel}/manifest.json`,
    }
  }

  shouldAttemptFallbackCapture(error) {
    const message = String(error?.message || error || '')
    return /No packets found|No H264 payload/i.test(message)
  }

  resolveFallbackWindow(job, channel) {
    if (!config.alertVideoCaptureFallbackEnabled) {
      return null
    }

    const rows = summarizeVehicleApproxCoverage(job.vehicleId)
    const channelCoverage = rows.find((row) => Number(row.channel) === Number(channel))
    if (!channelCoverage) {
      return null
    }

    const firstMs = Number(channelCoverage.approx_first_packet_timestamp_ms)
    const lastMs = Number(channelCoverage.approx_last_packet_timestamp_ms)
    if (!Number.isFinite(firstMs) || !Number.isFinite(lastMs) || lastMs <= firstMs) {
      return null
    }

    const fallbackWindowMs = clampPositiveInt(
      config.alertVideoCaptureFallbackWindowMs,
      339000,
    )
    const fallbackToMs = lastMs
    const fallbackFromMs = Math.max(firstMs, fallbackToMs - fallbackWindowMs)
    if (fallbackToMs <= fallbackFromMs) {
      return null
    }

    return {
      fromIso: new Date(fallbackFromMs).toISOString(),
      toIso: new Date(fallbackToMs).toISOString(),
    }
  }

  async exportWithFallback(job, channel, fromIso, toIso) {
    try {
      const primaryResult = await this.exportController.exportVideo({
        vehicleId: job.vehicleId,
        channel,
        from: fromIso,
        to: toIso,
        preRollMs: 0,
      })
      return {
        success: true,
        exportResult: primaryResult,
        fallbackUsed: false,
        fallbackWindow: null,
      }
    } catch (primaryError) {
      if (!this.shouldAttemptFallbackCapture(primaryError)) {
        return {
          success: false,
          error: primaryError,
          fallbackUsed: false,
          fallbackWindow: null,
        }
      }

      const fallbackWindow = this.resolveFallbackWindow(job, channel)
      if (!fallbackWindow) {
        return {
          success: false,
          error: primaryError,
          fallbackUsed: false,
          fallbackWindow: null,
        }
      }

      try {
        const fallbackResult = await this.exportController.exportVideo({
          vehicleId: job.vehicleId,
          channel,
          from: fallbackWindow.fromIso,
          to: fallbackWindow.toIso,
          preRollMs: 0,
        })
        return {
          success: true,
          exportResult: fallbackResult,
          fallbackUsed: true,
          fallbackWindow,
        }
      } catch (fallbackError) {
        return {
          success: false,
          error: new Error(
            `Primary capture failed: ${primaryError.message || String(primaryError)} | Fallback capture failed: ${fallbackError.message || String(fallbackError)}`,
          ),
          fallbackUsed: true,
          fallbackWindow,
        }
      }
    }
  }
}

module.exports = {
  AlertCaptureManager,
  ALERT_CAPTURE_ROOT,
  ALERT_CAPTURE_MEDIA_ROOT,
}
