const config = require('./helpers/config')
const { PacketController } = require('./controllers/packet-controller')
const {
  createPacketQueue,
  decodePacketQueueMessage,
} = require('./helpers/packet-queue')

async function start() {
  if (!config.queueWorkerEnabled) {
    console.log('Queue worker is disabled by QUEUE_WORKER_ENABLED=false')
    process.exit(0)
  }

  console.log('Queue worker startup: initializing packet controller...')
  const packetController = new PacketController()
  await packetController.initialize()
  packetController.persistRuntimeStats()
  console.log('Queue worker startup: packet controller ready')

  console.log('Queue worker startup: connecting to JetStream...')
  const packetQueue = await createPacketQueue({ role: 'worker' })
  await packetQueue.ensureStream()
  await packetQueue.ensureConsumer()
  const consumer = await packetQueue.getConsumer()
  console.log('Queue worker startup: JetStream consumer ready')

  let shuttingDown = false
  let messages = null

  const startConsuming = async () => {
    messages = await consumer.consume({
      max_messages: config.natsConsumeBatchSize,
      expires: config.natsConsumeExpiresMs,
      idle_heartbeat: config.natsConsumeIdleHeartbeatMs,
    })

    for await (const message of messages) {
      if (shuttingDown) {
        break
      }

      try {
        const { meta, payloadBuffer } = decodePacketQueueMessage(message.data)
        packetController.handlePacket(meta, payloadBuffer)
        message.ack()
      } catch (error) {
        const detail = error.message || String(error)
        console.error('Queue worker packet processing failed:', detail)
        try {
          message.term(detail)
        } catch {}
      }
    }
  }

  const shutdown = async (reason = 'Queue worker shutting down...', exitCode = 0) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    console.log(reason)

    try {
      if (messages) {
        await messages.close()
      }
    } catch (error) {
      console.error('Queue consumer shutdown failed:', error.message || String(error))
    }

    try {
      await packetController.close()
    } catch (error) {
      console.error('Packet controller shutdown failed:', error.message || String(error))
    }

    try {
      await packetQueue.close()
    } catch (error) {
      console.error('Queue connection shutdown failed:', error.message || String(error))
    }

    try {
      const { closePool } = require('./helpers/db')
      await closePool()
    } catch (error) {
      console.error('Database shutdown failed:', error.message || String(error))
    }

    process.exit(exitCode)
  }

  process.on('SIGINT', () => {
    void shutdown('Queue worker shutting down...')
  })
  process.on('SIGTERM', () => {
    void shutdown('Queue worker shutting down...')
  })

  try {
    await startConsuming()
  } catch (error) {
    console.error('Fatal queue worker runtime error:', error.message || String(error))
    await shutdown('Queue worker shutting down after fatal runtime error...', 1)
  }
}

start().catch(async (error) => {
  console.error('Fatal queue worker startup error:', error.message || String(error))
  const { closePool } = require('./helpers/db')
  await closePool().catch(() => {})
  process.exit(1)
})
