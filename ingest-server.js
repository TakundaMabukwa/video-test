const net = require('net')
const config = require('./helpers/config')
const { PacketController } = require('./controllers/packet-controller')

const HOST = config.relayHost
const PORT = config.relayPort

async function start() {
  console.log('Ingest startup: file-first mode ready')

  const packetController = new PacketController()
  console.log('Ingest startup: initializing packet controller...')
  await packetController.initialize()
  packetController.persistRuntimeStats()
  console.log('Ingest startup: packet controller initialized')

  let buffer = Buffer.alloc(0)
  let shuttingDown = false

  console.log(`Ingest startup: connecting to relay ${HOST}:${PORT}...`)
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

  const shutdown = async (reason = 'Ingest shutting down...', exitCode = 0) => {
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

    try {
      const { closePool } = require('./helpers/db')
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
    void shutdown('Ingest shutting down...')
  })
  process.on('SIGTERM', () => {
    void shutdown('Ingest shutting down...')
  })
}

start().catch(async (error) => {
  console.error('Fatal ingest startup error:', error.message || String(error))
  const { closePool } = require('./helpers/db')
  await closePool().catch(() => {})
  process.exit(1)
})
