const fs = require('fs')
const path = require('path')
const config = require('../helpers/config')
const {
  appendPacket,
  closeStorageStreams,
  cleanupExpiredPacketFiles,
  RETENTION_DAYS,
} = require('../helpers/storage')
const { parsePacket } = require('../helpers/jt1078')
const { writeIngestStats } = require('../helpers/runtime-state')

class PacketController {
  constructor() {
    this.sequenceResetGapThreshold = 1000
    this.sequenceResetTimeThresholdMs = 5000
    this.rawOutput = config.mirrorRawOutput
      ? fs.createWriteStream(path.join(process.cwd(), 'output.bin'), { flags: 'a' })
      : null
    this.processedPackets = 0
    this.processedBytes = 0
    this.insertedPackets = 0
    this.insertedGapEvents = 0
    this.sequenceGapPackets = 0
    this.sequenceGapEvents = 0
    this.lastDbError = null
    this.unrecoverablePackets = 0
    this.unrecoverableBytes = 0
    this.lastUnrecoverableError = null
    this.unrecoverableSamples = []
    this.streamStates = new Map()
    this.lastStatsPackets = 0
    this.lastStatsBytes = 0
    this.lastStatsAt = Date.now()
    this.lastLoggedGapEvents = 0
    this.lastLoggedMissingPackets = 0
    this.lastLoggedUnrecoverablePackets = 0
    this.lastRetentionRunAt = 0
    this.retentionTimer = setInterval(() => {
      void this.runRetentionCleanup()
    }, 60 * 60 * 1000)
    this.retentionTimer.unref()
    this.statsTimer = setInterval(() => {
      this.logStats()
    }, config.statsLogMs)
    this.statsTimer.unref()
  }

  async initialize() {
    void this.runRetentionCleanup().catch((error) => {
      this.lastUnrecoverableError = error.message || String(error)
      console.error('Background retention cleanup failed:', this.lastUnrecoverableError)
    })
    this.persistRuntimeStats()
  }

  handlePacket(meta, payloadBuffer) {
    this.processedPackets += 1
    this.processedBytes += payloadBuffer.length

    if (config.rawDebugLogs) {
      console.log('META:', meta)
      console.log('PAYLOAD BYTES:', payloadBuffer.length)
      console.log('HEX PREVIEW:', payloadBuffer.toString('hex').slice(0, 120))
    }

    if (this.rawOutput) {
      this.rawOutput.write(payloadBuffer)
    }

    let storageRecord = null
    try {
      storageRecord = appendPacket(meta, payloadBuffer)
    } catch (error) {
      this.recordUndurableDrop({
        reason: 'file-write-failed',
        payloadLength: payloadBuffer.length,
        meta,
        error,
      })
      return null
    }

    if (!storageRecord) {
      this.recordUndurableDrop({
        reason: 'invalid-storage-identity',
        payloadLength: payloadBuffer.length,
        meta,
      })
      return null
    }

    const vehicleId = String(meta.vehicleId || '').trim()
    const channel = Number(meta.channel || 0)
    const packetTimestampMs = Number(meta.timestamp || Date.now())
    const parsedPacket = parsePacket(payloadBuffer)

    if (parsedPacket) {
      this.trackSequence({
        vehicleId,
        channel,
        sequenceNumber: parsedPacket.sequence,
        packetTimestampMs,
      })
    }

    return storageRecord.filePath
  }

  recordUndurableDrop({ reason, payloadLength = 0, meta = null, error = null }) {
    this.unrecoverablePackets += 1
    this.unrecoverableBytes += Number(payloadLength || 0)
    this.lastUnrecoverableError = error
      ? error.message || String(error)
      : reason

    const sample = {
      at: new Date().toISOString(),
      reason,
      payloadLength: Number(payloadLength || 0),
      vehicleId: meta?.vehicleId ? String(meta.vehicleId) : null,
      channel: Number.isFinite(Number(meta?.channel)) ? Number(meta.channel) : null,
      timestamp: Number.isFinite(Number(meta?.timestamp)) ? Number(meta.timestamp) : null,
      error: error ? error.message || String(error) : null,
    }

    this.unrecoverableSamples.push(sample)
    if (this.unrecoverableSamples.length > 20) {
      this.unrecoverableSamples.shift()
    }

    console.error(
      `Unrecoverable packet loss: reason=${sample.reason} vehicleId=${sample.vehicleId || 'unknown'} channel=${sample.channel ?? 'unknown'} payloadBytes=${sample.payloadLength}${sample.error ? ` error=${sample.error}` : ''}`,
    )
    this.persistRuntimeStats()
  }

  async runRetentionCleanup() {
    const now = Date.now()
    if (now - this.lastRetentionRunAt < 60 * 1000) {
      return
    }
    this.lastRetentionRunAt = now

    const { deletedFiles } = cleanupExpiredPacketFiles(now)
    if (deletedFiles.length > 0) {
      console.log(
        `Retention cleanup complete: removed ${deletedFiles.length} packet file(s) older than ${RETENTION_DAYS} day(s)`,
      )
    }
  }

  logStats() {
    const now = Date.now()
    const elapsedMs = Math.max(1, now - this.lastStatsAt)
    const packetDelta = this.processedPackets - this.lastStatsPackets
    const byteDelta = this.processedBytes - this.lastStatsBytes
    const packetsPerSecond = ((packetDelta * 1000) / elapsedMs).toFixed(0)
    const megabytesPerSecond = ((byteDelta / 1024 / 1024) * (1000 / elapsedMs)).toFixed(2)
    const hasNewGapEvent = this.sequenceGapEvents > this.lastLoggedGapEvents
    const hasNewMissingPackets =
      this.sequenceGapPackets > this.lastLoggedMissingPackets
    const hasNewUnrecoverablePackets =
      this.unrecoverablePackets > this.lastLoggedUnrecoverablePackets

    if (hasNewGapEvent || hasNewMissingPackets || hasNewUnrecoverablePackets) {
      const parts = [
        `Packets stored: ${this.processedPackets}`,
        `rate=${packetsPerSecond}/s`,
        `throughput=${megabytesPerSecond} MB/s`,
        `gapEvents=${this.sequenceGapEvents}`,
        `missingPackets=${this.sequenceGapPackets}`,
        `unrecoverablePackets=${this.unrecoverablePackets}`,
      ]
      if (this.lastUnrecoverableError) {
        parts.push(`lastUnrecoverableError=${this.lastUnrecoverableError}`)
      }
      console.warn(parts.join(' | '))
    }

    this.lastStatsPackets = this.processedPackets
    this.lastStatsBytes = this.processedBytes
    this.lastStatsAt = now
    this.lastLoggedGapEvents = this.sequenceGapEvents
    this.lastLoggedMissingPackets = this.sequenceGapPackets
    this.lastLoggedUnrecoverablePackets = this.unrecoverablePackets
    this.persistRuntimeStats()
  }

  persistRuntimeStats() {
    try {
      writeIngestStats(this.getStats())
    } catch (error) {
      console.error('Failed to persist ingest stats:', error.message || String(error))
    }
  }

  async close() {
    clearInterval(this.statsTimer)
    clearInterval(this.retentionTimer)

    if (this.rawOutput) {
      await new Promise((resolve) => this.rawOutput.end(resolve))
    }

    await closeStorageStreams()
    this.persistRuntimeStats()
  }

  trackSequence({ vehicleId, channel, sequenceNumber, packetTimestampMs }) {
    if (!vehicleId || !Number.isFinite(channel) || channel <= 0 || !Number.isFinite(sequenceNumber)) {
      return null
    }

    const key = `${vehicleId}:${channel}`
    const previous = this.streamStates.get(key)

    if (previous && Number.isFinite(previous.lastSequence)) {
      const expected = (previous.lastSequence + 1) & 0xffff
      if (sequenceNumber !== expected) {
        const missingPacketCount =
          sequenceNumber > expected
            ? sequenceNumber - expected
            : 0x10000 - expected + sequenceNumber

        const timestampGap = packetTimestampMs - previous.lastPacketTimestampMs
        const looksLikeReset =
          missingPacketCount > this.sequenceResetGapThreshold ||
          timestampGap > this.sequenceResetTimeThresholdMs

        if (missingPacketCount > 0 && !looksLikeReset) {
          this.sequenceGapEvents += 1
          this.sequenceGapPackets += missingPacketCount
        }
      }
    }

    this.streamStates.set(key, {
      lastSequence: sequenceNumber,
      lastPacketTimestampMs: packetTimestampMs,
    })
  }

  getStats() {
    const streams = [...this.streamStates.entries()]
      .map(([key, state]) => {
        const [vehicleId, channel] = key.split(':')
        return {
          vehicleId,
          channel: Number(channel),
          lastSequence: state.lastSequence,
          lastPacketTimestampMs: state.lastPacketTimestampMs,
        }
      })
      .sort((a, b) => b.lastPacketTimestampMs - a.lastPacketTimestampMs)
      .slice(0, 20)

    return {
      receivedPackets: this.processedPackets,
      receivedBytes: this.processedBytes,
      insertedPackets: 0,
      packetWriteLag: 0,
      pendingDbRows: 0,
      pendingGapRows: 0,
      reindexInProgress: false,
      retentionDays: RETENTION_DAYS,
      sequenceGapEvents: this.sequenceGapEvents,
      sequenceGapPackets: this.sequenceGapPackets,
      insertedGapEvents: 0,
      lastDbError: null,
      unrecoverablePackets: this.unrecoverablePackets,
      unrecoverableBytes: this.unrecoverableBytes,
      lastUnrecoverableError: this.lastUnrecoverableError,
      unrecoverableSamples: this.unrecoverableSamples,
      trackedStreams: streams,
    }
  }
}

module.exports = {
  PacketController,
}
