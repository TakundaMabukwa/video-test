const express = require('express')
const config = require('./helpers/config')
const { ExportController, EXPORT_ROOT } = require('./controllers/export-controller')
const { ApiController } = require('./controllers/api-controller')

async function start() {
  console.log('API startup: file-first mode ready')

  const exportController = new ExportController()
  const apiController = new ApiController({
    exportController,
    packetController: null,
  })

  const app = express()
  app.use(express.json())
  app.use('/media/exports', express.static(EXPORT_ROOT))
  app.get('/health', apiController.health)
  app.get('/api/ingest/stats', apiController.ingestStats)
  app.get('/api/live/streams', apiController.activeLiveStreams)
  app.get('/api/vehicles/:vehicleId/live.mjpeg', apiController.vehicleMjpeg)
  app.get('/api/vehicles/:vehicleId/screenshot', apiController.vehicleScreenshot)
  app.get('/api/video/coverage', apiController.coverage)
  app.get('/api/vehicles/:vehicleId/video/availability', apiController.vehicleAvailability)
  app.get('/api/vehicles/:vehicleId/video', apiController.exportVehicleRange)
  app.post('/api/vehicles/:vehicleId/video', apiController.exportVehicleRange)
  app.get('/api/vehicles/:vehicleId/video/:channel', apiController.exportVehicleRange)
  app.post('/api/vehicles/:vehicleId/video/:channel', apiController.exportVehicleRange)
  app.get('/api/video/export', apiController.exportVehicleRange)
  app.post('/api/video/export', apiController.exportVehicleRange)

  let shuttingDown = false
  const apiServer = app.listen(config.apiPort, () => {
    console.log(`API listening on http://127.0.0.1:${config.apiPort}`)
  })

  const shutdown = async (reason = 'API shutting down...', exitCode = 0) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    console.log(reason)

    await new Promise((resolve) => apiServer.close(resolve))

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
