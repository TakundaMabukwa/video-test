const net = require('net')
const path = require('path')
const express = require('express')
const config = require('./helpers/config')
const { ensureSchema, closePool } = require('./helpers/db')
const { PacketController } = require('./controllers/packet-controller')
const { ExportController, EXPORT_ROOT } = require('./controllers/export-controller')
const { ApiController } = require('./controllers/api-controller')

const HOST = config.relayHost
const PORT = config.relayPort

async function start() {
  console.log('Startup: ensuring schema...')
  await ensureSchema()
  console.log('Startup: schema ready')

  const packetController = new PacketController()
  console.log('Startup: initializing packet controller...')
  await packetController.initialize()
  console.log('Startup: packet controller initialized')
  const exportController = new ExportController()
  const apiController = new ApiController({ exportController, packetController })
  let buffer = Buffer.alloc(0)
  let shuttingDown = false

  const app = express()
  app.use(express.json())
  app.use('/media/exports', express.static(EXPORT_ROOT))
  app.get('/health', apiController.health)
  app.get('/api/ingest/stats', apiController.ingestStats)
  app.get('/api/video/coverage', apiController.coverage)
  app.get('/api/vehicles/:vehicleId/video', apiController.exportVehicleRange)
  app.post('/api/vehicles/:vehicleId/video', apiController.exportVehicleRange)
  app.get('/api/vehicles/:vehicleId/video/:channel', apiController.exportVehicleRange)
  app.post('/api/vehicles/:vehicleId/video/:channel', apiController.exportVehicleRange)
  app.get('/api/video/export', apiController.exportVehicleRange)
  app.post('/api/video/export', apiController.exportVehicleRange)

  const apiServer = app.listen(config.apiPort, () => {
    console.log(`API listening on http://127.0.0.1:${config.apiPort}`)
  })

  console.log(`Startup: connecting to relay ${HOST}:${PORT}...`)
  const client = net.createConnection({ host: HOST, port: PORT }, () => {
    console.log(`Connected to ${HOST}:${PORT}`)
  })

  client.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk])

    while (buffer.length >= 8) {
      const metaLength = buffer.readUInt32BE(0)
      const payloadLength = buffer.readUInt32BE(4)
      const totalLength = 8 + metaLength + payloadLength

      if (buffer.length < totalLength) {
        return
      }

      const metaBuffer = buffer.slice(8, 8 + metaLength)
      const payloadStart = 8 + metaLength
      const payloadBuffer = buffer.slice(payloadStart, totalLength)

      try {
        const meta = JSON.parse(metaBuffer.toString('utf8'))
        packetController.handlePacket(meta, payloadBuffer)
      } catch (err) {
        packetController.recordUndurableDrop({
          reason: 'metadata-parse-failed',
          payloadLength: payloadBuffer.length,
          meta: {
            vehicleId: null,
            channel: null,
            timestamp: null,
          },
          error: err,
        })
        console.error('Failed to process packet:', err.message)
      }

      buffer = buffer.slice(totalLength)
    }
  })

  client.on('error', (err) => {
    console.error('TCP error:', err.message)
  })

  const shutdown = async (reason = 'Shutting down...', exitCode = 0) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    console.log(reason)

    try {
      if (!client.destroyed) {
        client.destroy()
      }
    } catch {}

    try {
      await packetController.close()
    } catch (error) {
      console.error('Packet controller shutdown failed:', error.message || String(error))
    }

    await new Promise((resolve) => apiServer.close(resolve))

    try {
      await closePool()
    } catch (error) {
      console.error('Database shutdown failed:', error.message || String(error))
    }

    process.exit(exitCode)
  }

  client.on('close', async () => {
    await shutdown('Connection closed')
  })

  process.on('SIGINT', () => {
    void shutdown('Shutting down...')
  })
  process.on('SIGTERM', () => {
    void shutdown('Shutting down...')
  })
}

start().catch(async (error) => {
  console.error('Fatal startup error:', error.message || String(error))
  await closePool().catch(() => {})
  process.exit(1)
})

// const http = require('http')
// const net = require('net')

// let clients = []

// // HTTP server for VLC
// http
//   .createServer((req, res) => {
//     console.log('VLC connected')
//     res.writeHead(200, {
//       'Content-Type': 'video/h264',
//       Connection: 'keep-alive',
//     })
//     clients.push(res)

//     req.on('close', () => {
//       clients = clients.filter((c) => c !== res)
//     })
//   })
//   .listen(9000, () => {
//     console.log('HTTP stream on http://localhost:9000')
//   })

// // TCP raw feed client
// const client = net.createConnection({ host: '209.38.206.44', port: 3000 })

// let buffer = Buffer.alloc(0)

// client.on('data', (chunk) => {
//   buffer = Buffer.concat([buffer, chunk])

//   while (buffer.length >= 8) {
//     const metaLength = buffer.readUInt32BE(0)
//     const payloadLength = buffer.readUInt32BE(4)
//     const totalLength = 8 + metaLength + payloadLength

//     if (buffer.length < totalLength) return

//     const payload = buffer.slice(8 + metaLength, totalLength)

//     // Send to all VLC clients
//     clients.forEach((c) => c.write(payload))

//     buffer = buffer.slice(totalLength)
//   }
// })
