const net = require('net')
const WebSocket = require('ws')
const config = require('./helpers/config')
const { createPacketQueue } = require('./helpers/packet-queue')
const { writeIngestRelayStats } = require('./helpers/runtime-state')
const { createLivePreviewManager } = require('./helpers/live-preview')
const { createLiveHlsManager } = require('./helpers/live-hls')
const { appendPacket, closeStorageStreams } = require('./helpers/storage')
const { parsePacket, extractPackets } = require('./helpers/jt1078')

const RELAY_HOST = config.relayHost
const RELAY_PORT = config.relayPort
const SOURCE_WS_URL = String(config.sourceWsUrl || '').trim()
const RECONNECT_BASE_DELAY_MS = 1000
const RECONNECT_MAX_DELAY_MS = 30000
const RECONNECT_JITTER_MS = 250
const MAX_INFLIGHT_PUBLISH = 10000
const RESUME_INFLIGHT_PUBLISH = 3000

function decodePayload(payload, encoding) {
  if (typeof payload !== 'string' || !payload.length) {
    return null
  }

  const normalizedEncoding = String(encoding || 'base64').trim().toLowerCase()
  if (normalizedEncoding === 'hex') {
    const compact = payload.replace(/\s+/g, '')
    if (compact.length % 2 !== 0) {
      return null
    }
    try {
      return Buffer.from(compact, 'hex')
    } catch {
      return null
    }
  }

  const compact = payload.replace(/\s+/g, '')
  try {
    const decoded = Buffer.from(compact, 'base64')
    return decoded.length ? decoded : null
  } catch {
    return null
  }
}

async function start() {
  const ingestFromRelay = !!config.relayIngestEnabled
  const ingestFromWs = !!config.sourceWsEnabled && !!SOURCE_WS_URL
  if (!ingestFromRelay && !ingestFromWs) {
    console.log(
      'Ingest startup: no input source enabled (set INGEST_SOURCE_MODE=relay|ws|both)',
    )
    writeIngestRelayStats({
      ingestSourceMode: config.ingestSourceMode,
      relayHost: RELAY_HOST,
      relayPort: RELAY_PORT,
      sourceWsEnabled: false,
      sourceWsUrl: SOURCE_WS_URL || null,
      relayIngestEnabled: false,
      receivedPackets: 0,
      receivedBytes: 0,
      relayReceivedPackets: 0,
      relayReceivedBytes: 0,
      wsReceivedPackets: 0,
      wsReceivedBytes: 0,
      wsMessages: 0,
      wsBinaryMessages: 0,
      wsTextMessages: 0,
      enqueuedPackets: 0,
      enqueueErrors: 0,
      droppedPackets: 0,
      archivedPackets: 0,
      archiveWriteErrors: 0,
      metadataParseErrors: 0,
      wsParseErrors: 0,
      wsDroppedBytes: 0,
      inflightPublishes: 0,
      pausedForBackpressure: false,
      relayReconnectAttempt: 0,
      wsReconnectAttempt: 0,
      wsConnects: 0,
      queueDepthMessages: null,
      lastQueueInfoError: null,
      lastRelayConnectAt: null,
      lastRelayDisconnectAt: null,
      lastRelayError: null,
      lastWsConnectAt: null,
      lastWsDisconnectAt: null,
      lastWsError: null,
      lastEnqueueError: null,
      lastArchiveError: null,
      lastEnqueueAckSequence: null,
      lastPacketVehicleId: null,
      lastPacketChannel: null,
      lastPacketTimestampMs: null,
      lastPacketDeviceTimestampMs: null,
      lastPacketTransport: null,
      lastPacketSource: null,
      lastPacketAt: null,
    })

    let keepAliveTimer = setInterval(() => {}, 60 * 60 * 1000)
    const shutdown = () => {
      if (keepAliveTimer) {
        clearInterval(keepAliveTimer)
        keepAliveTimer = null
      }
      console.log('Ingest shutting down...')
      process.exit(0)
    }

    process.on('SIGINT', shutdown)
    process.on('SIGTERM', shutdown)
    return
  }

  const queueEnabled = !!config.queueWorkerEnabled
  let packetQueue = null
  if (queueEnabled) {
    console.log(
      `Ingest startup: queue-first mode ready (sources: relay=${ingestFromRelay}, ws=${ingestFromWs})`,
    )
    console.log('Ingest startup: connecting to JetStream...')
    packetQueue = await createPacketQueue({ role: 'ingest' })
    await packetQueue.ensureStream()
    await packetQueue.ensureConsumer()
    console.log('Ingest startup: JetStream ready')
  } else {
    console.log(
      `Ingest startup: queue disabled (QUEUE_WORKER_ENABLED=false); running archive/live only (sources: relay=${ingestFromRelay}, ws=${ingestFromWs})`,
    )
  }
  const livePreviewManager = config.livePreviewFromIngest
    ? createLivePreviewManager({ source: 'ingest' })
    : {
        handlePacket() {},
        async close() {},
      }
  const liveHlsManager = createLiveHlsManager({ source: 'ingest' })

  let shuttingDown = false

  let relayBuffer = Buffer.alloc(0)
  let relayReconnectAttempt = 0
  let relayReconnectTimer = null
  let relayClient = null

  let wsCarryBuffer = Buffer.alloc(0)
  let wsReconnectAttempt = 0
  let wsReconnectTimer = null
  let wsClient = null
  let wsPingTimer = null
  let wsPongTimer = null
  let wsPendingPong = false

  let inflightPublishes = 0
  let relayPausedForBackpressure = false
  let statsRefreshInProgress = false

  const relayStats = {
    ingestSourceMode: config.ingestSourceMode,
    relayHost: RELAY_HOST,
    relayPort: RELAY_PORT,
    sourceWsEnabled: ingestFromWs,
    sourceWsUrl: ingestFromWs ? SOURCE_WS_URL : null,
    natsUrl: config.natsUrl,
    natsStreamName: config.natsStreamName,
    natsSubject: config.natsSubject,
    receivedPackets: 0,
    receivedBytes: 0,
    relayReceivedPackets: 0,
    relayReceivedBytes: 0,
    wsReceivedPackets: 0,
    wsReceivedBytes: 0,
    wsMessages: 0,
    wsBinaryMessages: 0,
    wsTextMessages: 0,
    wsConnects: 0,
    enqueuedPackets: 0,
    enqueueErrors: 0,
    droppedPackets: 0,
    archivedPackets: 0,
    archiveWriteErrors: 0,
    metadataParseErrors: 0,
    wsParseErrors: 0,
    wsDroppedBytes: 0,
    inflightPublishes: 0,
    pausedForBackpressure: false,
    relayReconnectAttempt: 0,
    wsReconnectAttempt: 0,
    queueDepthMessages: null,
    lastQueueInfoError: null,
    lastRelayConnectAt: null,
    lastRelayDisconnectAt: null,
    lastRelayError: null,
    lastWsConnectAt: null,
    lastWsDisconnectAt: null,
    lastWsError: null,
    lastEnqueueError: null,
    lastArchiveError: null,
    lastEnqueueAckSequence: null,
    lastPacketVehicleId: null,
    lastPacketChannel: null,
    lastPacketTimestampMs: null,
    lastPacketDeviceTimestampMs: null,
    lastPacketTransport: null,
    lastPacketSource: null,
    lastPacketAt: null,
  }

  const clearRelayReconnectTimer = () => {
    if (!relayReconnectTimer) {
      return
    }
    clearTimeout(relayReconnectTimer)
    relayReconnectTimer = null
  }

  const clearWsReconnectTimer = () => {
    if (!wsReconnectTimer) {
      return
    }
    clearTimeout(wsReconnectTimer)
    wsReconnectTimer = null
  }

  const clearWsHeartbeatTimers = () => {
    if (wsPingTimer) {
      clearInterval(wsPingTimer)
      wsPingTimer = null
    }
    if (wsPongTimer) {
      clearTimeout(wsPongTimer)
      wsPongTimer = null
    }
    wsPendingPong = false
  }

  const getReconnectDelayMs = (attempt) => {
    const exponential = Math.min(
      RECONNECT_MAX_DELAY_MS,
      RECONNECT_BASE_DELAY_MS * 2 ** Math.min(attempt, 8),
    )
    const jitter = Math.floor(Math.random() * RECONNECT_JITTER_MS)
    return exponential + jitter
  }

  const persistRelayStats = async ({ refreshQueueDepth = false } = {}) => {
    if (refreshQueueDepth && !statsRefreshInProgress) {
      statsRefreshInProgress = true
      try {
        if (!packetQueue) {
          relayStats.queueDepthMessages = null
          relayStats.lastQueueInfoError = null
        } else {
          const info = await packetQueue.getStreamInfo()
          relayStats.queueDepthMessages = Number(info?.state?.messages ?? 0)
          relayStats.lastQueueInfoError = null
        }
      } catch (error) {
        relayStats.lastQueueInfoError = error.message || String(error)
      } finally {
        statsRefreshInProgress = false
      }
    }

    relayStats.inflightPublishes = inflightPublishes
    relayStats.pausedForBackpressure = relayPausedForBackpressure
    relayStats.relayReconnectAttempt = relayReconnectAttempt
    relayStats.wsReconnectAttempt = wsReconnectAttempt

    try {
      writeIngestRelayStats(relayStats)
    } catch (error) {
      console.error('Failed to persist relay ingest stats:', error.message || String(error))
    }
  }

  const resumeIfBackpressureCleared = () => {
    if (!relayClient || relayClient.destroyed || !relayPausedForBackpressure) {
      return
    }
    if (inflightPublishes > RESUME_INFLIGHT_PUBLISH) {
      return
    }

    relayPausedForBackpressure = false
    try {
      relayClient.resume()
    } catch {}
  }

  const scheduleRelayReconnect = (reason = 'socket-closed') => {
    if (shuttingDown || relayReconnectTimer || !ingestFromRelay) {
      return
    }

    relayReconnectAttempt += 1
    relayStats.lastRelayDisconnectAt = new Date().toISOString()
    const delayMs = getReconnectDelayMs(relayReconnectAttempt)
    console.warn(
      `Relay connection dropped (${reason}). Reconnecting in ${delayMs}ms (attempt ${relayReconnectAttempt})...`,
    )

    relayReconnectTimer = setTimeout(() => {
      relayReconnectTimer = null
      connectToRelay()
    }, delayMs)
  }

  const scheduleWsReconnect = (reason = 'socket-closed') => {
    if (shuttingDown || wsReconnectTimer || !ingestFromWs) {
      return
    }

    wsReconnectAttempt += 1
    relayStats.lastWsDisconnectAt = new Date().toISOString()
    const delayMs = getReconnectDelayMs(wsReconnectAttempt)
    console.warn(
      `Listener websocket dropped (${reason}). Reconnecting in ${delayMs}ms (attempt ${wsReconnectAttempt})...`,
    )

    wsReconnectTimer = setTimeout(() => {
      wsReconnectTimer = null
      connectToSourceWs()
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
    if (!queueEnabled || !packetQueue) {
      return
    }

    inflightPublishes += 1

    if (
      relayClient &&
      !relayClient.destroyed &&
      !relayPausedForBackpressure &&
      inflightPublishes >= MAX_INFLIGHT_PUBLISH
    ) {
      relayPausedForBackpressure = true
      try {
        relayClient.pause()
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

  const archivePacket = (meta, payloadBuffer) => {
    if (!config.archiveWriteFromIngest) {
      return
    }

    try {
      const record = appendPacket(meta, payloadBuffer)
      if (!record) {
        relayStats.archiveWriteErrors += 1
        relayStats.lastArchiveError = 'invalid-storage-identity'
        console.error('Primary archive write skipped: invalid storage identity')
        return
      }
      relayStats.archivedPackets += 1
      relayStats.lastArchiveError = null
    } catch (error) {
      relayStats.archiveWriteErrors += 1
      relayStats.lastArchiveError = error.message || String(error)
      console.error(
        `Primary archive write failed: ${relayStats.lastArchiveError}`,
      )
    }
  }

  const handleLiveOutputs = (meta, payloadBuffer) => {
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
  }

  const processPacket = (meta, payloadBuffer, sourceType) => {
    relayStats.receivedPackets += 1
    relayStats.receivedBytes += payloadBuffer.length
    if (sourceType === 'relay') {
      relayStats.relayReceivedPackets += 1
      relayStats.relayReceivedBytes += payloadBuffer.length
    } else if (sourceType === 'ws') {
      relayStats.wsReceivedPackets += 1
      relayStats.wsReceivedBytes += payloadBuffer.length
    }

    relayStats.lastPacketVehicleId = String(meta?.vehicleId || '')
    relayStats.lastPacketChannel = Number(meta?.channel || 0)
    relayStats.lastPacketTimestampMs = Number(
      meta?.archiveTimestampMs ?? meta?.receivedAtMs ?? meta?.timestamp ?? Date.now(),
    )
    relayStats.lastPacketDeviceTimestampMs = Number(
      meta?.deviceTimestampMs ?? meta?.timestamp ?? 0,
    )
    relayStats.lastPacketTransport = String(meta?.transport || 'tcp')
    relayStats.lastPacketSource = String(meta?.source || sourceType)
    relayStats.lastPacketAt = new Date().toISOString()

    archivePacket(meta, payloadBuffer)
    handleLiveOutputs(meta, payloadBuffer)
    queuePacket(meta, payloadBuffer)
  }

  const handleRelayData = (chunk) => {
    relayBuffer = Buffer.concat([relayBuffer, chunk])

    while (relayBuffer.length >= 8) {
      const metaLength = relayBuffer.readUInt32BE(0)
      const payloadLength = relayBuffer.readUInt32BE(4)
      const totalLength = 8 + metaLength + payloadLength

      if (relayBuffer.length < totalLength) {
        return
      }

      const metaBuffer = relayBuffer.slice(8, 8 + metaLength)
      const payloadStart = 8 + metaLength
      const payloadBuffer = relayBuffer.slice(payloadStart, totalLength)

      try {
        const meta = JSON.parse(metaBuffer.toString('utf8'))
        processPacket(meta, payloadBuffer, 'relay')
      } catch (error) {
        relayStats.metadataParseErrors += 1
        recordDrop('metadata-parse-failed', error)
      }

      relayBuffer = relayBuffer.slice(totalLength)
    }
  }

  const handleWsPacketBuffer = (packetBuffer, transport = 'tcp') => {
    const parsed = parsePacket(packetBuffer)
    if (!parsed) {
      relayStats.wsParseErrors += 1
      recordDrop('ws-invalid-jt1078-packet')
      return
    }

    const nowMs = Date.now()
    const deviceTs = Number(parsed.timestamp)
    const meta = {
      vehicleId: String(parsed.sim || '').trim(),
      channel: Number(parsed.channel || 0),
      timestamp: nowMs,
      deviceTimestampMs: Number.isFinite(deviceTs) && deviceTs > 0 ? deviceTs : null,
      receivedAtMs: nowMs,
      archiveTimestampMs: nowMs,
      transport: transport === 'udp' ? 'udp' : 'tcp',
      source: 'listener-ws',
    }

    if (!meta.vehicleId || !meta.channel) {
      relayStats.wsParseErrors += 1
      recordDrop('ws-missing-vehicle-or-channel')
      return
    }

    processPacket(meta, packetBuffer, 'ws')
  }

  const handleWsPayloadBuffer = (payloadBuffer, transport = 'tcp') => {
    if (!Buffer.isBuffer(payloadBuffer) || !payloadBuffer.length) {
      return
    }

    const extracted = extractPackets(payloadBuffer, wsCarryBuffer, {
      maxBodyLength: config.sourceWsMaxBodyLength,
    })
    wsCarryBuffer = extracted.remainder
    relayStats.wsDroppedBytes += Number(extracted.droppedBytes || 0)
    relayStats.wsParseErrors += Number(extracted.parseErrors || 0)

    for (const packetBuffer of extracted.packets) {
      handleWsPacketBuffer(packetBuffer, transport)
    }
  }

  const handleWsTextMessage = (text) => {
    let parsed = null
    try {
      parsed = JSON.parse(text)
    } catch {
      parsed = null
    }

    if (!parsed || typeof parsed !== 'object') {
      return
    }

    const decoded =
      decodePayload(parsed.chunk, parsed.encoding) ||
      decodePayload(parsed.payload, parsed.encoding) ||
      decodePayload(parsed.data, parsed.encoding)
    if (!decoded) {
      return
    }

    const transport = parsed.transport === 'udp' ? 'udp' : 'tcp'
    handleWsPayloadBuffer(decoded, transport)
  }

  const connectToRelay = () => {
    if (shuttingDown || !ingestFromRelay) {
      return
    }
    if (relayClient && !relayClient.destroyed) {
      return
    }

    clearRelayReconnectTimer()
    relayBuffer = Buffer.alloc(0)

    console.log(`Ingest startup: connecting to relay ${RELAY_HOST}:${RELAY_PORT}...`)
    try {
      relayClient = net.createConnection({ host: RELAY_HOST, port: RELAY_PORT })
    } catch (error) {
      relayStats.lastRelayError = error.message || String(error)
      console.error('TCP connect setup error:', relayStats.lastRelayError)
      relayClient = null
      scheduleRelayReconnect('connect-setup-failed')
      return
    }

    relayClient.on('connect', () => {
      relayReconnectAttempt = 0
      relayStats.lastRelayConnectAt = new Date().toISOString()
      relayStats.lastRelayError = null
      console.log(`Connected to ${RELAY_HOST}:${RELAY_PORT}`)
    })

    relayClient.on('data', handleRelayData)

    relayClient.on('error', (error) => {
      relayStats.lastRelayError = error.message || String(error)
      console.error('TCP error:', relayStats.lastRelayError)
    })

    relayClient.on('close', (hadError) => {
      relayClient = null
      relayBuffer = Buffer.alloc(0)
      relayPausedForBackpressure = false
      if (shuttingDown) {
        return
      }
      scheduleRelayReconnect(hadError ? 'socket-error' : 'socket-closed')
    })
  }

  const startWsHeartbeat = () => {
    clearWsHeartbeatTimers()
    wsPendingPong = false
    wsPingTimer = setInterval(() => {
      if (!wsClient || wsClient.readyState !== WebSocket.OPEN) {
        return
      }
      if (wsPendingPong) {
        relayStats.lastWsError = 'pong-timeout'
        try {
          wsClient.terminate()
        } catch {}
        return
      }

      wsPendingPong = true
      try {
        wsClient.ping()
      } catch {
        return
      }
      wsPongTimer = setTimeout(() => {
        if (wsPendingPong && wsClient && wsClient.readyState === WebSocket.OPEN) {
          relayStats.lastWsError = 'pong-timeout'
          try {
            wsClient.terminate()
          } catch {}
        }
      }, Math.max(1000, Number(config.sourceWsPongTimeoutMs || 10000)))
    }, Math.max(5000, Number(config.sourceWsPingIntervalMs || 30000)))
    wsPingTimer.unref()
  }

  const connectToSourceWs = () => {
    if (shuttingDown || !ingestFromWs) {
      return
    }
    if (wsClient && (wsClient.readyState === WebSocket.OPEN || wsClient.readyState === WebSocket.CONNECTING)) {
      return
    }

    clearWsReconnectTimer()
    wsCarryBuffer = Buffer.alloc(0)

    console.log(`Ingest startup: connecting to listener websocket ${SOURCE_WS_URL}...`)
    wsClient = new WebSocket(SOURCE_WS_URL, {
      perMessageDeflate: false,
      handshakeTimeout: Math.max(5000, Number(config.sourceWsPongTimeoutMs || 10000)),
    })

    wsClient.on('open', () => {
      wsReconnectAttempt = 0
      relayStats.wsConnects += 1
      relayStats.lastWsConnectAt = new Date().toISOString()
      relayStats.lastWsError = null
      console.log(`Connected to websocket source ${SOURCE_WS_URL}`)
      startWsHeartbeat()
    })

    wsClient.on('pong', () => {
      wsPendingPong = false
      if (wsPongTimer) {
        clearTimeout(wsPongTimer)
        wsPongTimer = null
      }
    })

    wsClient.on('message', (data, isBinary) => {
      relayStats.wsMessages += 1
      if (isBinary) {
        relayStats.wsBinaryMessages += 1
      } else {
        relayStats.wsTextMessages += 1
      }

      if (isBinary) {
        const payloadBuffer = Buffer.isBuffer(data) ? data : Buffer.from(data)
        handleWsPayloadBuffer(payloadBuffer, 'tcp')
        return
      }

      const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data)
      handleWsTextMessage(text)
    })

    wsClient.on('error', (error) => {
      relayStats.lastWsError = error?.message || String(error)
      console.error('Source websocket error:', relayStats.lastWsError)
    })

    wsClient.on('close', () => {
      clearWsHeartbeatTimers()
      relayStats.lastWsDisconnectAt = new Date().toISOString()
      wsClient = null
      wsCarryBuffer = Buffer.alloc(0)
      if (shuttingDown) {
        return
      }
      scheduleWsReconnect('socket-closed')
    })
  }

  const statsTimer = setInterval(() => {
    void persistRelayStats({ refreshQueueDepth: true })
  }, config.statsLogMs)
  statsTimer.unref()

  await persistRelayStats({ refreshQueueDepth: true })
  if (ingestFromRelay) {
    connectToRelay()
  }
  if (ingestFromWs) {
    connectToSourceWs()
  }

  const shutdown = async (reason = 'Ingest shutting down...', exitCode = 0) => {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    console.log(reason)
    clearRelayReconnectTimer()
    clearWsReconnectTimer()
    clearWsHeartbeatTimers()
    clearInterval(statsTimer)

    try {
      if (relayClient && !relayClient.destroyed) {
        relayClient.destroy()
      }
    } catch {}

    try {
      if (wsClient) {
        wsClient.close()
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
      if (config.archiveWriteFromIngest) {
        await closeStorageStreams()
      }
    } catch (error) {
      console.error('Archive storage shutdown failed:', error.message || String(error))
    }

    try {
      if (packetQueue) {
        await packetQueue.close()
      }
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
