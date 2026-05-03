const config = require('./config')
const { appendPacket, closeStorageStreams } = require('./storage')
const { parsePacket } = require('./jt1078')
const { createPacketQueue } = require('./packet-queue')
const { createLivePreviewManager } = require('./live-preview')
const { createLiveHlsManager } = require('./live-hls')
const { writeForwardedIngestStats } = require('./runtime-state')

function createForwardedRtpIngestPipeline({ source = 'listener-forward' } = {}) {
  const livePreviewManager = config.livePreviewFromIngest
    ? createLivePreviewManager({ source })
    : {
        handlePacket() {},
        async close() {},
      }
  const liveHlsManager = createLiveHlsManager({ source })

  let packetQueue = null
  let queueReady = false
  let queueInitAttempted = false

  const stats = {
    source,
    receivedPackets: 0,
    receivedBytes: 0,
    archivedPackets: 0,
    archiveWriteErrors: 0,
    enqueuedPackets: 0,
    enqueueErrors: 0,
    parseErrors: 0,
    droppedPackets: 0,
    lastArchiveError: null,
    lastEnqueueError: null,
    lastParseError: null,
    lastPacketVehicleId: null,
    lastPacketChannel: null,
    lastPacketTimestampMs: null,
    lastPacketAt: null,
  }

  function persistStats() {
    try {
      writeForwardedIngestStats(stats)
    } catch (error) {
      console.error(
        'Failed to persist forwarded RTP ingest stats:',
        error?.message || String(error),
      )
    }
  }

  async function initializeQueue() {
    if (queueInitAttempted) {
      return
    }

    queueInitAttempted = true
    if (!config.queueWorkerEnabled) {
      queueReady = false
      stats.lastEnqueueError = null
      persistStats()
      return
    }

    try {
      packetQueue = await createPacketQueue({ role: 'api-forwarded' })
      await packetQueue.ensureStream()
      await packetQueue.ensureConsumer()
      queueReady = true
      stats.lastEnqueueError = null
    } catch (error) {
      queueReady = false
      stats.lastEnqueueError = error?.message || String(error)
      console.error(
        'Forwarded RTP ingest queue initialization failed:',
        stats.lastEnqueueError,
      )
    } finally {
      persistStats()
    }
  }

  function buildMeta({ vehicleId, transport, parsedPacket }) {
    return {
      vehicleId,
      channel: parsedPacket.channel,
      timestamp: parsedPacket.timestamp,
      transport,
      source,
    }
  }

  function archivePacket(meta, payloadBuffer) {
    if (!config.archiveWriteFromIngest) {
      return
    }

    try {
      const record = appendPacket(meta, payloadBuffer)
      if (!record) {
        stats.archiveWriteErrors += 1
        stats.droppedPackets += 1
        stats.lastArchiveError = 'invalid-storage-identity'
        return
      }
      stats.archivedPackets += 1
      stats.lastArchiveError = null
    } catch (error) {
      stats.archiveWriteErrors += 1
      stats.droppedPackets += 1
      stats.lastArchiveError = error?.message || String(error)
      console.error('Forwarded RTP archive write failed:', stats.lastArchiveError)
    }
  }

  function mirrorToQueue(meta, payloadBuffer) {
    if (!queueReady || !packetQueue) {
      return
    }

    void packetQueue
      .publishPacket({
        meta,
        payloadBuffer,
        receivedAtMs: Date.now(),
      })
      .then(() => {
        stats.enqueuedPackets += 1
        stats.lastEnqueueError = null
        persistStats()
      })
      .catch((error) => {
        stats.enqueueErrors += 1
        stats.lastEnqueueError = error?.message || String(error)
        console.error(
          'Forwarded RTP queue publish failed:',
          stats.lastEnqueueError,
        )
        persistStats()
      })
  }

  function handleLiveOutputs(meta, payloadBuffer) {
    try {
      livePreviewManager.handlePacket(meta, payloadBuffer)
    } catch (error) {
      console.error(
        'Forwarded RTP live preview pipeline failed:',
        error?.message || String(error),
      )
    }

    try {
      liveHlsManager.handlePacket(meta, payloadBuffer)
    } catch (error) {
      console.error(
        'Forwarded RTP live HLS pipeline failed:',
        error?.message || String(error),
      )
    }
  }

  function handlePacketEntry(entry) {
    const vehicleId = String(entry?.vehicleId || '').trim()
    const transport = entry?.transport === 'udp' ? 'udp' : 'tcp'
    const packetBase64 = String(entry?.packetBase64 || '').trim()

    if (!vehicleId || !packetBase64) {
      stats.droppedPackets += 1
      stats.lastParseError = 'missing-vehicle-or-packet'
      return {
        success: false,
        reason: 'missing-vehicle-or-packet',
      }
    }

    let payloadBuffer = null
    try {
      payloadBuffer = Buffer.from(packetBase64, 'base64')
    } catch (error) {
      stats.parseErrors += 1
      stats.droppedPackets += 1
      stats.lastParseError = error?.message || String(error)
      return {
        success: false,
        reason: 'invalid-base64',
      }
    }

    stats.receivedPackets += 1
    stats.receivedBytes += payloadBuffer.length
    stats.lastPacketVehicleId = vehicleId
    stats.lastPacketAt = new Date().toISOString()

    const parsedPacket = parsePacket(payloadBuffer)
    if (!parsedPacket) {
      stats.parseErrors += 1
      stats.droppedPackets += 1
      stats.lastParseError = 'unsupported-or-invalid-jt1078-packet'
      return {
        success: false,
        reason: 'unsupported-or-invalid-jt1078-packet',
      }
    }

    const meta = buildMeta({ vehicleId, transport, parsedPacket })
    stats.lastPacketChannel = Number(meta.channel || 0)
    stats.lastPacketTimestampMs = Number(meta.timestamp || 0)
    stats.lastParseError = null

    archivePacket(meta, payloadBuffer)
    handleLiveOutputs(meta, payloadBuffer)
    mirrorToQueue(meta, payloadBuffer)

    return {
      success: true,
      vehicleId,
      channel: meta.channel,
      timestamp: meta.timestamp,
      transport,
    }
  }

  async function handleBatch(entries) {
    const packets = Array.isArray(entries) ? entries : []
    const results = []

    for (const entry of packets) {
      results.push(handlePacketEntry(entry))
    }

    persistStats()
    return {
      total: packets.length,
      accepted: results.filter((result) => result.success).length,
      rejected: results.filter((result) => !result.success).length,
      results,
    }
  }

  function getStats() {
    return {
      ...stats,
      queueReady,
      queueInitAttempted,
      archiveWriteFromIngest: !!config.archiveWriteFromIngest,
      livePreviewFromIngest: !!config.livePreviewFromIngest,
      liveHlsEnabled: !!config.liveHlsEnabled,
    }
  }

  async function close() {
    if (packetQueue) {
      await packetQueue.close().catch(() => {})
    }
    await livePreviewManager.close().catch(() => {})
    await liveHlsManager.close().catch(() => {})
    await closeStorageStreams().catch(() => {})
    persistStats()
  }

  return {
    initializeQueue,
    handleBatch,
    getStats,
    close,
  }
}

module.exports = {
  createForwardedRtpIngestPipeline,
}
