const fs = require('fs')
const { summarizeCoverageForRange, summarizeVehicleApproxCoverage } = require('../helpers/storage')
const { listActivePreviewStreams, readLatestPreview } = require('../helpers/live-preview-state')
const {
  getPlaylistPath,
  listActiveLiveHlsStreams,
  readLiveHlsStatus,
  touchLiveHlsRequest,
} = require('../helpers/live-hls-state')
const config = require('../helpers/config')
const { readIngestStats, readIngestRelayStats } = require('../helpers/runtime-state')

class ApiController {
  constructor({ exportController, packetController, alertCaptureManager }) {
    this.exportController = exportController
    this.packetController = packetController
    this.alertCaptureManager = alertCaptureManager
  }

  parseTimestampMs(value) {
    const numeric = Number(value)
    if (Number.isFinite(numeric) && String(value).trim() !== '') {
      return numeric
    }
    const ms = new Date(value).getTime()
    return Number.isFinite(ms) ? ms : null
  }

  parseChannels(rawChannels, routeChannel = null) {
    if (routeChannel !== null && routeChannel !== undefined && routeChannel !== '') {
      const channel = Number(routeChannel)
      return Number.isFinite(channel) && channel > 0 ? [channel] : []
    }

    const source = rawChannels ?? [1, 2]
    const values = Array.isArray(source)
      ? source
      : String(source)
          .split(',')
          .map((value) => Number(String(value).trim()))

    return [...new Set(values.filter((value) => Number.isFinite(value) && value > 0))]
  }

  parseChannel(value, fallback = 1) {
    const parsed = Number(value ?? fallback)
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
  }

  buildExportRequest(req) {
    const vehicleId = String(
      req.params?.vehicleId || req.body?.vehicleId || req.query?.vehicleId || '',
    ).trim()
    const from = req.body?.from || req.query?.from
    const to = req.body?.to || req.query?.to
    const preRollMs = Number(req.body?.preRollMs || req.query?.preRollMs || 5000)
    const channels = this.parseChannels(
      req.body?.channels || req.query?.channels,
      req.params?.channel,
    )

    return {
      vehicleId,
      from,
      to,
      preRollMs,
      channels,
    }
  }

  buildAbsoluteMediaUrl(req, maybeRelativePath) {
    if (!maybeRelativePath) {
      return null
    }
    if (/^https?:\/\//i.test(maybeRelativePath)) {
      return maybeRelativePath
    }

    const protocolHeader = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim()
    const protocol = protocolHeader || req.protocol || 'http'
    const host = req.get('host')
    if (!host) {
      return maybeRelativePath
    }
    return `${protocol}://${host}${maybeRelativePath}`
  }

  health(req, res) {
    res.json({ success: true, status: 'ok' })
  }

  requestAlertCapture = async (req, res) => {
    try {
      if (!this.alertCaptureManager) {
        return res.status(503).json({
          success: false,
          message: 'Alert capture manager is not available',
        })
      }

      const job = this.alertCaptureManager.queueCapture(req.body || {})
      return res.status(202).json({
        success: true,
        job,
      })
    } catch (error) {
      return res.status(400).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  alertCaptureStatus = async (req, res) => {
    try {
      if (!this.alertCaptureManager) {
        return res.status(503).json({
          success: false,
          message: 'Alert capture manager is not available',
        })
      }

      const alertId = String(req.params?.alertId || '').trim()
      if (!alertId) {
        return res.status(400).json({
          success: false,
          message: 'alertId is required',
        })
      }

      const job = this.alertCaptureManager.readJob(alertId)
      if (!job) {
        return res.status(404).json({
          success: false,
          message: 'No alert capture job found for that alertId',
        })
      }

      const results = Array.isArray(job.results)
        ? job.results.map((result) => ({
            ...result,
            mp4UrlAbsolute: this.buildAbsoluteMediaUrl(req, result.mp4Url),
            h264UrlAbsolute: this.buildAbsoluteMediaUrl(req, result.h264Url),
            rawPacketsUrlAbsolute: this.buildAbsoluteMediaUrl(req, result.rawPacketsUrl),
            manifestUrlAbsolute: this.buildAbsoluteMediaUrl(req, result.manifestUrl),
          }))
        : []

      return res.status(200).json({
        success: true,
        job: {
          ...job,
          results,
        },
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  activeLiveStreams = (req, res) => {
    try {
      const maxAgeMs = Number(req.query?.maxAgeMs || config.livePreviewMaxAgeMs)
      const rows = listActivePreviewStreams({ maxAgeMs })
      return res.status(200).json({
        success: true,
        count: rows.length,
        rows,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  activeLiveHlsStreams = (req, res) => {
    try {
      const maxAgeMs = Number(req.query?.maxAgeMs || config.liveHlsMaxAgeMs)
      const rows = listActiveLiveHlsStreams({ maxAgeMs })
      return res.status(200).json({
        success: true,
        count: rows.length,
        rows,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  vehicleScreenshot = async (req, res) => {
    try {
      const vehicleId = String(req.params?.vehicleId || '').trim()
      const channel = this.parseChannel(req.query?.channel, 1)
      const maxAgeMs = Number(req.query?.maxAgeMs || 0)

      if (!vehicleId) {
        return res.status(400).json({
          success: false,
          message: 'vehicleId is required',
        })
      }

      const preview = readLatestPreview(vehicleId, channel)
      if (!preview) {
        return res.status(404).json({
          success: false,
          message: 'No live preview frame available yet for that vehicle/channel.',
        })
      }

      const updatedAtMs = Number(preview.meta?.updatedAtMs || 0)
      const isStale = Number.isFinite(maxAgeMs) && maxAgeMs > 0
        ? Date.now() - updatedAtMs > maxAgeMs
        : false
      if (isStale) {
        return res.status(404).json({
          success: false,
          message: 'Latest live preview frame is older than requested maxAgeMs.',
        })
      }

      res.setHeader('Content-Type', 'image/jpeg')
      res.setHeader('Content-Length', String(preview.jpegBuffer.length))
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, private')
      res.setHeader('X-Preview-Updated-At', String(preview.meta?.updatedAt || ''))
      res.setHeader('X-Preview-Sequence', String(preview.meta?.sequence ?? ''))
      return res.status(200).send(preview.jpegBuffer)
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  async waitForPreview(vehicleId, channel, waitMs, maxAgeMs) {
    const deadline = Date.now() + Math.max(0, Number(waitMs || 0))
    const pollMs = Math.max(100, Number(config.livePreviewPollMs || 250))

    while (Date.now() <= deadline) {
      const preview = readLatestPreview(vehicleId, channel)
      if (preview) {
        const updatedAtMs = Number(preview.meta?.updatedAtMs || 0)
        const isFresh =
          !Number.isFinite(Number(maxAgeMs)) ||
          Number(maxAgeMs) <= 0 ||
          Date.now() - updatedAtMs <= Number(maxAgeMs)
        if (isFresh) {
          return preview
        }
      }

      await new Promise((resolve) => setTimeout(resolve, pollMs))
    }

    return null
  }

  async waitForLiveHls(vehicleId, channel, waitMs, maxAgeMs) {
    const deadline = Date.now() + Math.max(0, Number(waitMs || 0))
    const pollMs = Math.max(100, Number(config.livePreviewPollMs || 250))

    while (Date.now() <= deadline) {
      const status = readLiveHlsStatus(vehicleId, channel)
      const playlistPath = getPlaylistPath(vehicleId, channel)
      const hasPlaylist =
        fs.existsSync(playlistPath) && fs.statSync(playlistPath).size > 0
      const updatedAtMs = Number(status?.updatedAtMs || 0)
      const isFresh =
        Number.isFinite(updatedAtMs) &&
        (
          !Number.isFinite(Number(maxAgeMs)) ||
          Number(maxAgeMs) <= 0 ||
          Date.now() - updatedAtMs <= Number(maxAgeMs)
        )

      if (status && hasPlaylist && isFresh) {
        return {
          status,
          playlistPath,
        }
      }

      await new Promise((resolve) => setTimeout(resolve, pollMs))
    }

    return null
  }

  vehicleMjpeg = async (req, res) => {
    const vehicleId = String(req.params?.vehicleId || '').trim()
    const channel = this.parseChannel(req.query?.channel, 1)
    const waitMs = Number(req.query?.waitMs || config.livePreviewWaitMs)
    const maxAgeMs = Number(req.query?.maxAgeMs || config.livePreviewMaxAgeMs)
    const boundary = 'frame'

    if (!vehicleId) {
      return res.status(400).json({
        success: false,
        message: 'vehicleId is required',
      })
    }

    try {
      let preview = await this.waitForPreview(vehicleId, channel, waitMs, maxAgeMs)
      if (!preview) {
        return res.status(404).json({
          success: false,
          message: 'No live preview frames became available in time for that vehicle/channel.',
        })
      }

      res.status(200)
      res.setHeader('Content-Type', `multipart/x-mixed-replace; boundary=${boundary}`)
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, private')
      res.setHeader('Connection', 'keep-alive')

      let lastSequence = Number(preview.meta?.sequence || 0)
      const writeFrame = (frame) => {
        res.write(
          `--${boundary}\r\n` +
            'Content-Type: image/jpeg\r\n' +
            `Content-Length: ${frame.jpegBuffer.length}\r\n` +
            `X-Preview-Updated-At: ${frame.meta?.updatedAt || ''}\r\n\r\n`,
        )
        res.write(frame.jpegBuffer)
        res.write('\r\n')
      }

      writeFrame(preview)

      const interval = setInterval(() => {
        preview = readLatestPreview(vehicleId, channel)
        if (!preview) {
          return
        }

        const updatedAtMs = Number(preview.meta?.updatedAtMs || 0)
        if (
          Number.isFinite(maxAgeMs) &&
          maxAgeMs > 0 &&
          Date.now() - updatedAtMs > maxAgeMs
        ) {
          return
        }

        const sequence = Number(preview.meta?.sequence || 0)
        if (sequence <= lastSequence) {
          return
        }

        lastSequence = sequence
        writeFrame(preview)
      }, Math.max(100, Number(config.livePreviewPollMs || 250)))

      const cleanup = () => {
        clearInterval(interval)
        try {
          res.end(`--${boundary}--\r\n`)
        } catch {}
      }

      req.on('close', cleanup)
      req.on('aborted', cleanup)
      return undefined
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  vehicleLiveHlsPlaylist = async (req, res) => {
    const vehicleId = String(req.params?.vehicleId || '').trim()
    const channel = this.parseChannel(req.params?.channel, 1)
    const waitMs = Number(req.query?.waitMs || config.liveHlsWaitMs)
    const maxAgeMs = Number(req.query?.maxAgeMs || config.liveHlsMaxAgeMs)

    if (!vehicleId) {
      return res.status(400).json({
        success: false,
        message: 'vehicleId is required',
      })
    }

    try {
      touchLiveHlsRequest({
        vehicleId,
        channel,
      })

      const ready = await this.waitForLiveHls(vehicleId, channel, waitMs, maxAgeMs)
      if (!ready) {
        return res.status(404).json({
          success: false,
          message: 'No live HLS stream became available in time for that vehicle/channel.',
        })
      }

      const playlistText = fs.readFileSync(ready.playlistPath, 'utf8')
      res.setHeader('Content-Type', 'application/vnd.apple.mpegurl')
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, private')
      res.setHeader('X-Live-Source', String(ready.status?.source || 'unknown'))
      res.setHeader('X-Live-Updated-At', String(ready.status?.updatedAt || ''))
      return res.status(200).send(playlistText)
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  ingestStats = (req, res) => {
    const workerStats = this.packetController?.getStats?.() || readIngestStats()?.stats || null
    const relayStats = readIngestRelayStats()?.stats || null
    return res.status(200).json({
      success: true,
      stats: workerStats,
      relayStats,
    })
  }

  coverage = async (req, res) => {
    try {
      const from = req.query?.from
      const to = req.query?.to
      const vehicleId = String(req.query?.vehicleId || '').trim()
      const fromMs = this.parseTimestampMs(from)
      const toMs = this.parseTimestampMs(to)
      const source = String(req.query?.source || 'file').trim().toLowerCase()

      if (!from || !to || fromMs === null || toMs === null) {
        return res.status(400).json({
          success: false,
          message: 'valid from and to are required',
        })
      }

      let rows = []
      let usedSource = 'file'

      if (source === 'db') {
        return res.status(410).json({
          success: false,
          message: 'DB-backed packet coverage is disabled in file-first mode. Use source=file.',
        })
      }

      rows = await summarizeCoverageForRange({ fromMs, toMs, vehicleId })

      return res.status(200).json({
        success: true,
        from,
        to,
        vehicleId: vehicleId || null,
        source: usedSource,
        count: rows.length,
        rows,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  vehicleAvailability = async (req, res) => {
    try {
      const vehicleId = String(req.params?.vehicleId || '').trim()
      if (!vehicleId) {
        return res.status(400).json({
          success: false,
          message: 'vehicleId is required',
        })
      }

      const hasChannelsFilter = /(?:\?|&)channels=/.test(String(req.originalUrl || ''))
      const channels = hasChannelsFilter
        ? this.parseChannels(req.query?.channels)
        : []
      let rows = summarizeVehicleApproxCoverage(vehicleId)
      if (channels.length) {
        rows = rows.filter((row) => channels.includes(row.channel))
      }

      return res.status(200).json({
        success: true,
        vehicleId,
        channels: hasChannelsFilter ? channels : null,
        count: rows.length,
        rows,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  exportVehicleRange = async (req, res) => {
    try {
      const { vehicleId, from, to, preRollMs, channels } = this.buildExportRequest(req)

      if (!vehicleId || !from || !to || !channels.length) {
        return res.status(400).json({
          success: false,
          message: 'vehicleId, from, to and at least one channel are required',
        })
      }

      const result = await this.exportController.exportVehicleRange({
        vehicleId,
        from,
        to,
        preRollMs,
        channels,
      })

      const channelsWithAbsoluteUrls = (result.channels || []).map((channelResult) => ({
        ...channelResult,
        playUrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.playUrl),
        mp4UrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.mp4Url),
        h264UrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.h264Url),
        rawPacketsUrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.rawPacketsUrl),
      }))

      return res.status(200).json({
        success: true,
        ...result,
        channels: channelsWithAbsoluteUrls,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }
}

module.exports = {
  ApiController,
}
