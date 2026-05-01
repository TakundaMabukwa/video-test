const net = require('net')
const config = require('./helpers/config')
const { createPacketQueue } = require('./helpers/packet-queue')
const { writeIngestRelayStats } = require('./helpers/runtime-state')
const { createLivePreviewManager } = require('./helpers/live-preview')
const { createLiveHlsManager } = require('./helpers/live-hls')

const HOST = config.relayHost
const PORT = config.relayPort
const RECONNECT_BASE_DELAY_MS = 1000
const RECONNECT_MAX_DELAY_MS = 30000
const RECONNECT_JITTER_MS = 250
const MAX_INFLIGHT_PUBLISH = 10000
const RESUME_INFLIGHT_PUBLISH = 3000

async function start() {
  console.log('Ingest startup: queue-first mode ready')
  console.log('Ingest startup: connecting to JetStream...')

  const packetQueue = await createPacketQueue({ role: 'ingest' })
  await packetQueue.ensureStream()
  await packetQueue.ensureConsumer()
  console.log('Ingest startup: JetStream ready')
  const livePreviewManager = config.livePreviewFromIngest
    ? createLivePreviewManager({ source: 'ingest' })
    : {
        handlePacket() {},
        async close() {},
      }
  const liveHlsManager = createLiveHlsManager({ source: 'ingest' })

  let buffer = Buffer.alloc(0)
  let shuttingDown = false
  let reconnectAttempt = 0
  let reconnectTimer = null
  let client = null
  let inflightPublishes = 0
  let pausedForBackpressure = false
  let statsRefreshInProgress = false

  const relayStats = {
    relayHost: HOST,
    relayPort: PORT,
    natsUrl: config.natsUrl,
    natsStreamName: config.natsStreamName,
    natsSubject: config.natsSubject,
    receivedPackets: 0,
    receivedBytes: 0,
    enqueuedPackets: 0,
    enqueueErrors: 0,
    droppedPackets: 0,
    metadataParseErrors: 0,
    inflightPublishes: 0,
    pausedForBackpressure: false,
    relayReconnectAttempt: 0,
    queueDepthMessages: null,
    lastQueueInfoError: null,
    lastRelayConnectAt: null,
    lastRelayDisconnectAt: null,
    lastRelayError: null,
    lastEnqueueError: null,
    lastEnqueueAckSequence: null,
  }

  const clearReconnectTimer = () => {
    if (!reconnectTimer) {
      return
    }
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }

  const getReconnectDelayMs = () => {
    const exponential = Math.min(
      RECONNECT_MAX_DELAY_MS,
      RECONNECT_BASE_DELAY_MS * 2 ** Math.min(reconnectAttempt, 8),
    )
    const jitter = Math.floor(Math.random() * RECONNECT_JITTER_MS)
    return exponential + jitter
  }

  const persistRelayStats = async ({ refreshQueueDepth = false } = {}) => {
    if (refreshQueueDepth && !statsRefreshInProgress) {
      statsRefreshInProgress = true
      try {
        const info = await packetQueue.getStreamInfo()
        relayStats.queueDepthMessages = Number(info?.state?.messages ?? 0)
        relayStats.lastQueueInfoError = null
      } catch (error) {
        relayStats.lastQueueInfoError = error.message || String(error)
      } finally {
        statsRefreshInProgress = false
      }
    }

    relayStats.inflightPublishes = inflightPublishes
    relayStats.pausedForBackpressure = pausedForBackpressure
    relayStats.relayReconnectAttempt = reconnectAttempt

    try {
      writeIngestRelayStats(relayStats)
    } catch (error) {
      console.error('Failed to persist relay ingest stats:', error.message || String(error))
    }
  }

  const resumeIfBackpressureCleared = () => {
    if (!client || client.destroyed || !pausedForBackpressure) {
      return
    }
    if (inflightPublishes > RESUME_INFLIGHT_PUBLISH) {
      return
    }

    pausedForBackpressure = false
    try {
      client.resume()
    } catch {}
  }

  const scheduleReconnect = (reason = 'socket-closed') => {
    if (shuttingDown || reconnectTimer) {
      return
    }

    reconnectAttempt += 1
    relayStats.lastRelayDisconnectAt = new Date().toISOString()
    const delayMs = getReconnectDelayMs()
    console.warn(
      `Relay connection dropped (${reason}). Reconnecting in ${delayMs}ms (attempt ${reconnectAttempt})...`,
    )

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      connectToRelay()
    }, delayMs)
  }

  const recordDrop = (reason, error = null) => {
    relayStats.droppedPackets += 1
    relayStats.lastEnqueueError = error ? error.message || String(error) : reason
    console.error(
      `Dropped packet before queue persist: reason=${reason}${relayStats.lastEnqueueError ? ` detail=${relayStats.lastEnqueueError}` : ''}`,
    )
  }

  const queuePacket = (meta, payloadBuffer) => {
    inflightPublishes += 1

    if (
      client &&
      !client.destroyed &&
      !pausedForBackpressure &&
      inflightPublishes >= MAX_INFLIGHT_PUBLISH
    ) {
      pausedForBackpressure = true
      try {
        client.pause()
      } catch {}
      console.warn(
        `Backpressure engaged: inflight queue publishes reached ${inflightPublishes}`,
      )
    }

    void packetQueue
      .publishPacket({
        meta,
        payloadBuffer,
        receivedAtMs: Date.now(),
      })
      .then((ack) => {
        relayStats.enqueuedPackets += 1
        relayStats.lastEnqueueAckSequence = Number(ack?.seq ?? relayStats.lastEnqueueAckSequence)
      })
      .catch((error) => {
        relayStats.enqueueErrors += 1
        recordDrop('queue-publish-failed', error)
      })
      .finally(() => {
        inflightPublishes = Math.max(0, inflightPublishes - 1)
        resumeIfBackpressureCleared()
      })
  }

  const handleData = (chunk) => {
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
      relayStats.receivedPackets += 1
      relayStats.receivedBytes += payloadBuffer.length

      try {
        const meta = JSON.parse(metaBuffer.toString('utf8'))
        try {
          livePreviewManager.handlePacket(meta, payloadBuffer)
        } catch (error) {
          console.error(
            'Ingest live preview pipeline failed:',
            error?.message || String(error),
          )
        }
        try {
          liveHlsManager.handlePacket(meta, payloadBuffer)
        } catch (error) {
          console.error(
            'Ingest live HLS pipeline failed:',
            error?.message || String(error),
          )
        }
        queuePacket(meta, payloadBuffer)
      } catch (error) {
        relayStats.metadataParseErrors += 1
        recordDrop('metadata-parse-failed', error)
      }

      buffer = buffer.slice(totalLength)
    }
  }

  const connectToRelay = () => {
    if (shuttingDown) {
      return
    }
    if (client && !client.destroyed) {
      return
    }

    clearReconnectTimer()
    buffer = Buffer.alloc(0)

    console.log(`Ingest startup: connecting to relay ${HOST}:${PORT}...`)
    try {
      client = net.createConnection({ host: HOST, port: PORT })
    } catch (error) {
      relayStats.lastRelayError = error.message || String(error)
      console.error('TCP connect setup error:', relayStats.lastRelayError)
      client = null
      scheduleReconnect('connect-setup-failed')
      return
    }

    client.on('connect', () => {
      reconnectAttempt = 0
      relayStats.lastRelayConnectAt = new Date().toISOString()
      relayStats.lastRelayError = null
      console.log(`Connected to ${HOST}:${PORT}`)
    })

    client.on('data', handleData)

    client.on('error', (error) => {
      relayStats.lastRelayError = error.message || String(error)
      console.error('TCP error:', relayStats.lastRelayError)
    })

    client.on('close', (hadError) => {
      client = null
      buffer = Buffer.alloc(0)
      pausedForBackpressure = false
      if (shuttingDown) {
        return
      }
      scheduleReconnect(hadError ? 'socket-error' : 'socket-closed')
    })
  }

  const statsTimer = setInterval(() => {
    void persistRelayStats({ refreshQueueDepth: true })
  }, config.statsLogMs)
  statsTimer.unref()

  await persistRelayStats({ refreshQueueDepth: true })
  connectToRelay()

  const shutdown = async (reason = 'Ingest shutting down...', exitCode = 0) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    console.log(reason)
    clearReconnectTimer()
    clearInterval(statsTimer)

    try {
      if (client && !client.destroyed) {
        client.destroy()
      }
    } catch {}

    const waitUntil = Date.now() + 10000
    while (inflightPublishes > 0 && Date.now() < waitUntil) {
      await new Promise((resolve) => setTimeout(resolve, 100))
    }

    await persistRelayStats({ refreshQueueDepth: true })

    try {
      await livePreviewManager.close()
    } catch (error) {
      console.error('Live preview shutdown failed:', error.message || String(error))
    }

    try {
      await liveHlsManager.close()
    } catch (error) {
      console.error('Live HLS shutdown failed:', error.message || String(error))
    }

    try {
      await packetQueue.close()
    } catch (error) {
      console.error('Queue shutdown failed:', error.message || String(error))
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
