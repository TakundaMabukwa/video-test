const express = require('express')
const { WebSocketServer, WebSocket } = require('ws')
const config = require('./helpers/config')
const { ExportController, EXPORT_ROOT } = require('./controllers/export-controller')
const { ApiController } = require('./controllers/api-controller')
const { LIVE_HLS_ROOT } = require('./helpers/live-hls-state')
const { AlertCaptureManager, ALERT_CAPTURE_MEDIA_ROOT } = require('./helpers/alert-capture')
const { listActivePreviewStreams } = require('./helpers/live-preview-state')
const {
  createForwardedRtpIngestPipeline,
} = require('./helpers/forwarded-rtp-ingest')

function isAuthorized(req) {
  const expected = String(config.internalWorkerToken || '').trim()
  if (!expected) {
    return true
  }
  return String(req.header('X-Internal-Token') || '').trim() === expected
}

function buildScreenshotSnapshotPayload(maxAgeMs = null) {
  const rows = listActivePreviewStreams({ maxAgeMs })
  const results = rows
    .map((row) => {
      const vehicleId = String(row?.vehicleId || '').trim()
      const channel = Number(row?.channel || 0)
      const ok = Boolean(vehicleId && Number.isFinite(channel) && channel > 0)
      return {
        vehicleId,
        channel,
        ok,
        source: String(row?.source || 'ingest'),
        updatedAt: row?.updatedAt || null,
        updatedAtMs: Number(row?.updatedAtMs || 0),
        fileUrl:
          ok
            ? `/api/vehicles/${encodeURIComponent(vehicleId)}/screenshot?channel=${channel}`
            : null,
      }
    })
    .filter((result) => result.ok)

  return {
    type: 'snapshot',
    count: results.length,
    results,
    emittedAt: new Date().toISOString(),
  }
}

async function start() {
  console.log('API startup: file-first mode ready')
  const screenshotOnlyMode = !!config.screenshotOnlyMode

  const exportController = screenshotOnlyMode ? null : new ExportController()
  const alertCaptureManager =
    screenshotOnlyMode || !exportController
      ? null
      : new AlertCaptureManager({
          exportController,
        })
  const forwardedRtpIngest =
    screenshotOnlyMode
      ? {
          async initializeQueue() {},
          async handleBatch() {
            return {
              total: 0,
              accepted: 0,
              rejected: 0,
              results: [],
            }
          },
          getStats() {
            return null
          },
          async close() {},
        }
      : createForwardedRtpIngestPipeline({
          source: 'listener-forward',
        })
  await forwardedRtpIngest.initializeQueue()
  const apiController = new ApiController({
    exportController,
    packetController: forwardedRtpIngest,
    alertCaptureManager,
  })

  const app = express()
  app.use(express.json({ limit: '25mb' }))
  if (!screenshotOnlyMode) {
    app.use('/media/exports', express.static(EXPORT_ROOT))
    app.use('/media/alert-captures', express.static(ALERT_CAPTURE_MEDIA_ROOT))
    app.use('/media/live-hls', express.static(LIVE_HLS_ROOT, {
      setHeaders(res, filePath) {
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, private')
        if (filePath.endsWith('.m3u8')) {
          res.setHeader('Content-Type', 'application/vnd.apple.mpegurl')
        } else if (filePath.endsWith('.ts')) {
          res.setHeader('Content-Type', 'video/mp2t')
        }
      },
    }))
  }
  app.get('/health', apiController.health)
  app.get('/api/ingest/stats', apiController.ingestStats)
  app.get('/api/live/streams', apiController.activeLiveStreams)
  app.get('/api/live/screenshots/latest', apiController.latestScreenshots)
  app.get('/api/vehicles/:vehicleId/screenshot', apiController.vehicleScreenshot)
  if (!screenshotOnlyMode) {
    app.get('/api/live-hls/streams', apiController.activeLiveHlsStreams)
    app.get('/api/vehicles/:vehicleId/live.mjpeg', apiController.vehicleMjpeg)
    app.get('/api/vehicles/:vehicleId/live-hls/:channel/playlist.m3u8', apiController.vehicleLiveHlsPlaylist)
    app.post('/api/internal/ingest/rtp-batch', async (req, res) => {
      if (!isAuthorized(req)) {
        return res.status(403).json({
          success: false,
          message: 'Forbidden',
        })
      }

      try {
        const packets = Array.isArray(req.body?.packets) ? req.body.packets : []
        const summary = await forwardedRtpIngest.handleBatch(packets)
        return res.status(200).json({
          success: true,
          ...summary,
        })
      } catch (error) {
        return res.status(500).json({
          success: false,
          message: error.message || String(error),
        })
      }
    })
    app.post('/api/internal/alerts/capture', async (req, res) => {
      if (!isAuthorized(req)) {
        return res.status(403).json({
          success: false,
          message: 'Forbidden',
        })
      }
      return apiController.requestAlertCapture(req, res)
    })
    app.get('/api/alerts/:alertId/capture', apiController.alertCaptureStatus)
    app.get('/api/video/coverage', apiController.coverage)
    app.get('/api/vehicles/:vehicleId/video/availability', apiController.vehicleAvailability)
    app.get('/api/vehicles/:vehicleId/video', apiController.exportVehicleRange)
    app.post('/api/vehicles/:vehicleId/video', apiController.exportVehicleRange)
    app.get('/api/vehicles/:vehicleId/video/:channel', apiController.exportVehicleRange)
    app.post('/api/vehicles/:vehicleId/video/:channel', apiController.exportVehicleRange)
    app.get('/api/video/export', apiController.exportVehicleRange)
    app.post('/api/video/export', apiController.exportVehicleRange)
  } else {
    app.get('/api/live-hls/streams', (_req, res) =>
      res.status(410).json({
        success: false,
        message: 'Live HLS disabled in screenshot-only mode',
      })
    )
    app.get('/api/vehicles/:vehicleId/live.mjpeg', (_req, res) =>
      res.status(410).json({
        success: false,
        message: 'MJPEG disabled in screenshot-only mode',
      })
    )
    app.all('/api/internal/ingest/rtp-batch', (_req, res) =>
      res.status(410).json({
        success: false,
        message: 'Internal ingest batch endpoint disabled in screenshot-only mode',
      })
    )
  }

  let shuttingDown = false
  const apiServer = app.listen(config.apiPort, () => {
    console.log(`API listening on http://127.0.0.1:${config.apiPort}`)
  })

  const screenshotWsClients = new Set()
  const screenshotWss = new WebSocketServer({
    noServer: true,
    perMessageDeflate: false,
  })
  screenshotWss.on('connection', (ws) => {
    screenshotWsClients.add(ws)
    ws.on('close', () => screenshotWsClients.delete(ws))
    ws.on('error', () => screenshotWsClients.delete(ws))

    try {
      ws.send(JSON.stringify(buildScreenshotSnapshotPayload(config.livePreviewMaxAgeMs)))
    } catch {}
  })

  apiServer.on('upgrade', (request, socket, head) => {
    try {
      const host = request.headers.host || `127.0.0.1:${config.apiPort}`
      const url = new URL(request.url || '/', `http://${host}`)
      if (url.pathname !== '/ws/screenshots') {
        socket.destroy()
        return
      }
      screenshotWss.handleUpgrade(request, socket, head, (ws) => {
        screenshotWss.emit('connection', ws, request)
      })
    } catch {
      socket.destroy()
    }
  })

  const screenshotWsBroadcastTimer = setInterval(() => {
    if (screenshotWsClients.size === 0) return
    const payload = JSON.stringify(buildScreenshotSnapshotPayload(config.livePreviewMaxAgeMs))
    for (const ws of Array.from(screenshotWsClients)) {
      if (ws.readyState !== WebSocket.OPEN) {
        screenshotWsClients.delete(ws)
        continue
      }
      try {
        ws.send(payload)
      } catch {
        screenshotWsClients.delete(ws)
        try {
          ws.close()
        } catch {}
      }
    }
  }, 5000)
  screenshotWsBroadcastTimer.unref()

  const shutdown = async (reason = 'API shutting down...', exitCode = 0) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    console.log(reason)
    clearInterval(screenshotWsBroadcastTimer)
    for (const ws of Array.from(screenshotWsClients)) {
      try {
        ws.close()
      } catch {}
    }
    screenshotWsClients.clear()
    try {
      screenshotWss.close()
    } catch {}

    await new Promise((resolve) => apiServer.close(resolve))

    try {
      await forwardedRtpIngest.close()
    } catch (error) {
      console.error('Forwarded RTP ingest shutdown failed:', error.message || String(error))
    }

    try {
      const { closePool } = require('./helpers/db')
      await closePool()
    } catch (error) {
      console.error('API database shutdown failed:', error.message || String(error))
    }

    process.exit(exitCode)
  }

  process.on('SIGINT', () => {
    void shutdown('API shutting down...')
  })
  process.on('SIGTERM', () => {
    void shutdown('API shutting down...')
  })
}

start().catch(async (error) => {
  console.error('Fatal API startup error:', error.message || String(error))
  const { closePool } = require('./helpers/db')
  await closePool().catch(() => {})
  process.exit(1)
})
