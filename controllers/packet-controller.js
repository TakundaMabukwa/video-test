const fs = require('fs')
const path = require('path')
const config = require('../helpers/config')
const {
  appendPacket,
  closeStorageStreams,
  cleanupExpiredPacketFiles,
  listPacketFiles,
  parseStoragePath,
  RETENTION_DAYS,
  RETENTION_MS,
  scanPacketFile,
} = require('../helpers/storage')
const { query } = require('../helpers/db')
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
    this.pendingRows = []
    this.pendingGapRows = []
    this.processingQueue = false
    this.reindexInProgress = false
    this.lastStatsPackets = 0
    this.lastStatsBytes = 0
    this.lastStatsAt = Date.now()
    this.lastLoggedGapEvents = 0
    this.lastLoggedMissingPackets = 0
    this.lastLoggedDbError = null
    this.lastLoggedPendingRows = 0
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
      this.lastDbError = error.message || String(error)
      console.error('Background retention cleanup failed:', this.lastDbError)
    })
    void this.rebuildRecentIndexFromFiles().catch((error) => {
      this.lastDbError = error.message || String(error)
      console.error('Background reindex failed:', this.lastDbError)
    })
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
    const relayType = String(meta.type || 'tcp-rtp')
    const packetSize = Number(meta.size || payloadBuffer.length || 0)
    const payloadHexPreview = config.storeHexPreview
      ? payloadBuffer.toString('hex').slice(0, 120)
      : null
    const parsedPacket = parsePacket(payloadBuffer)

    let sequenceNumber = null
    let dataType = null
    let fragmentType = null
    let gapEvent = null

    if (parsedPacket) {
      sequenceNumber = parsedPacket.sequence
      dataType = parsedPacket.dataType
      fragmentType = parsedPacket.fragmentType
      gapEvent = this.trackSequence({
        vehicleId,
        channel,
        sequenceNumber,
        packetTimestampMs,
      })
    }

    this.pendingRows.push({
      relayType,
      vehicleId,
      channel,
      packetTimestampMs,
      sequenceNumber,
      dataType,
      fragmentType,
      packetSize,
      fileOffsetBytes: storageRecord.fileOffsetBytes,
      payloadHexPreview,
      payloadPath: storageRecord.filePath,
    })

    if (gapEvent) {
      this.pendingGapRows.push(gapEvent)
    }

    void this.flushPendingRows()
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
  }

  async flushPendingRows() {
    if (this.processingQueue) {
      return
    }

    this.processingQueue = true
    let currentPacketBatch = null
    let currentGapBatch = null
    try {
      while (this.pendingRows.length > 0) {
        currentPacketBatch = this.pendingRows.splice(0, config.packetBatchSize)
        const packetValues = []
        const packetPlaceholders = currentPacketBatch.map((record, rowIndex) => {
          const base = rowIndex * 11
          packetValues.push(
            record.relayType,
            record.vehicleId,
            record.channel,
            record.packetTimestampMs,
            record.sequenceNumber,
            record.dataType,
            record.fragmentType,
            record.packetSize,
            record.fileOffsetBytes,
            record.payloadHexPreview,
            record.payloadPath,
          )
          return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11})`
        })

        const packetInsertResult = await query(
          `INSERT INTO raw_video_packets (
             relay_type,
             vehicle_id,
             channel,
             packet_timestamp_ms,
             sequence_number,
             data_type,
             fragment_type,
             packet_size,
             file_offset_bytes,
             payload_hex_preview,
             payload_path
           )
           SELECT
             v.relay_type::text,
             v.vehicle_id::text,
             v.channel::integer,
             v.packet_timestamp_ms::bigint,
             v.sequence_number::integer,
             v.data_type::integer,
             v.fragment_type::integer,
             v.packet_size::integer,
             v.file_offset_bytes::bigint,
             v.payload_hex_preview::text,
             v.payload_path::text
           FROM (VALUES ${packetPlaceholders.join(', ')}) AS v(
             relay_type,
             vehicle_id,
             channel,
             packet_timestamp_ms,
             sequence_number,
             data_type,
             fragment_type,
             packet_size,
             file_offset_bytes,
             payload_hex_preview,
             payload_path
           )
           WHERE NOT EXISTS (
             SELECT 1
             FROM raw_video_packets existing
             WHERE existing.payload_path = v.payload_path::text
               AND existing.file_offset_bytes = v.file_offset_bytes::bigint
          )`,
          packetValues,
        )

        this.insertedPackets += packetInsertResult.rowCount
        await this.persistIndexState(currentPacketBatch)
        currentPacketBatch = null
      }

      while (this.pendingGapRows.length > 0) {
        currentGapBatch = this.pendingGapRows.splice(0, config.packetBatchSize)
        const gapValues = []
        const gapPlaceholders = currentGapBatch.map((gapEvent, rowIndex) => {
          const base = rowIndex * 7
          gapValues.push(
            gapEvent.vehicleId,
            gapEvent.channel,
            gapEvent.previousSequenceNumber,
            gapEvent.expectedSequenceNumber,
            gapEvent.actualSequenceNumber,
            gapEvent.missingPacketCount,
            gapEvent.packetTimestampMs,
          )
          return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`
        })

        await query(
          `INSERT INTO packet_sequence_gaps (
             vehicle_id,
             channel,
             previous_sequence_number,
             expected_sequence_number,
             actual_sequence_number,
             missing_packet_count,
             packet_timestamp_ms
           )
           SELECT
             v.vehicle_id::text,
             v.channel::integer,
             v.previous_sequence_number::integer,
             v.expected_sequence_number::integer,
             v.actual_sequence_number::integer,
             v.missing_packet_count::integer,
             v.packet_timestamp_ms::bigint
           FROM (VALUES ${gapPlaceholders.join(', ')}) AS v(
             vehicle_id,
             channel,
             previous_sequence_number,
             expected_sequence_number,
             actual_sequence_number,
             missing_packet_count,
             packet_timestamp_ms
          )`,
          gapValues,
        )

        this.insertedGapEvents += currentGapBatch.length
        currentGapBatch = null
      }

      this.lastDbError = null
    } catch (error) {
      if (currentPacketBatch && currentPacketBatch.length > 0) {
        this.pendingRows.unshift(...currentPacketBatch)
      }
      if (currentGapBatch && currentGapBatch.length > 0) {
        this.pendingGapRows.unshift(...currentGapBatch)
      }
      this.lastDbError = error.message || String(error)
      console.error('Durable DB write failed:', this.lastDbError)
    } finally {
      this.processingQueue = false
    }
  }

  async rebuildRecentIndexFromFiles() {
    if (this.reindexInProgress) {
      return
    }

    this.reindexInProgress = true
    try {
      const cutoffMs = Date.now() - RETENTION_MS
      const stateResult = await query(
        `SELECT payload_path, last_indexed_offset_bytes FROM storage_index_state`,
      )
      const indexState = new Map(
        stateResult.rows.map((row) => [
          row.payload_path,
          Number(row.last_indexed_offset_bytes || 0),
        ]),
      )

      const packetFiles = listPacketFiles()
        .map((filePath) => ({
          filePath,
          stats: fs.statSync(filePath),
        }))
        .filter((entry) => entry.stats.mtimeMs >= cutoffMs)
        .sort((a, b) => a.filePath.localeCompare(b.filePath))

      for (const entry of packetFiles) {
        const identity = parseStoragePath(entry.filePath)
        if (!identity) {
          continue
        }

        const startOffset = indexState.get(entry.filePath) || 0
        if (startOffset >= entry.stats.size) {
          continue
        }

        const rows = []
        const gapRows = []
        const scanResult = await scanPacketFile(entry.filePath, async ({ timestampMs, packet, fileOffsetBytes, packetSize }) => {
          const parsedPacket = parsePacket(packet)
          let sequenceNumber = null
          let dataType = null
          let fragmentType = null
          let gapEvent = null

          if (parsedPacket) {
            sequenceNumber = parsedPacket.sequence
            dataType = parsedPacket.dataType
            fragmentType = parsedPacket.fragmentType
            gapEvent = this.trackSequence({
              vehicleId: identity.vehicleId,
              channel: identity.channel,
              sequenceNumber,
              packetTimestampMs: timestampMs,
            })
          }

          rows.push({
            relayType: 'tcp-rtp',
            vehicleId: identity.vehicleId,
            channel: identity.channel,
            packetTimestampMs: timestampMs,
            sequenceNumber,
            dataType,
            fragmentType,
            packetSize,
            fileOffsetBytes,
            payloadHexPreview: null,
            payloadPath: entry.filePath,
          })

          if (gapEvent) {
            gapRows.push(gapEvent)
          }

          if (rows.length >= config.packetBatchSize) {
            await this.insertRecoveredRows(rows.splice(0, rows.length))
          }
          if (gapRows.length >= config.packetBatchSize) {
            await this.insertRecoveredGaps(gapRows.splice(0, gapRows.length))
          }
        }, startOffset)

        if (rows.length) {
          await this.insertRecoveredRows(rows)
        }
        if (gapRows.length) {
          await this.insertRecoveredGaps(gapRows)
        }

        await query(
          `INSERT INTO storage_index_state (payload_path, last_indexed_offset_bytes, updated_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT (payload_path)
           DO UPDATE SET
             last_indexed_offset_bytes = EXCLUDED.last_indexed_offset_bytes,
             updated_at = NOW()`,
          [entry.filePath, scanResult.finalOffsetBytes],
        )
      }
    } finally {
      this.reindexInProgress = false
    }
  }

  async insertRecoveredRows(rows) {
    if (!rows.length) {
      return
    }

    const packetValues = []
    const packetPlaceholders = rows.map((record, rowIndex) => {
      const base = rowIndex * 11
      packetValues.push(
        record.relayType,
        record.vehicleId,
        record.channel,
        record.packetTimestampMs,
        record.sequenceNumber,
        record.dataType,
        record.fragmentType,
        record.packetSize,
        record.fileOffsetBytes,
        record.payloadHexPreview,
        record.payloadPath,
      )
      return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11})`
    })

    const result = await query(
      `INSERT INTO raw_video_packets (
         relay_type,
         vehicle_id,
         channel,
         packet_timestamp_ms,
         sequence_number,
         data_type,
         fragment_type,
         packet_size,
         file_offset_bytes,
         payload_hex_preview,
         payload_path
       )
       SELECT
         v.relay_type::text,
         v.vehicle_id::text,
         v.channel::integer,
         v.packet_timestamp_ms::bigint,
         v.sequence_number::integer,
         v.data_type::integer,
         v.fragment_type::integer,
         v.packet_size::integer,
         v.file_offset_bytes::bigint,
         v.payload_hex_preview::text,
         v.payload_path::text
       FROM (VALUES ${packetPlaceholders.join(', ')}) AS v(
         relay_type,
         vehicle_id,
         channel,
         packet_timestamp_ms,
         sequence_number,
         data_type,
         fragment_type,
         packet_size,
         file_offset_bytes,
         payload_hex_preview,
         payload_path
       )
       WHERE NOT EXISTS (
         SELECT 1
         FROM raw_video_packets existing
         WHERE existing.payload_path = v.payload_path::text
           AND existing.file_offset_bytes = v.file_offset_bytes::bigint
       )`,
      packetValues,
    )

    this.insertedPackets += result.rowCount
    await this.persistIndexState(rows)
  }

  async persistIndexState(rows) {
    if (!rows.length) {
      return
    }

    const latestByPath = new Map()
    for (const row of rows) {
      const nextOffset = Number(row.fileOffsetBytes || 0) + 12 + Number(row.packetSize || 0)
      const existing = latestByPath.get(row.payloadPath)
      if (!existing || nextOffset > existing.lastIndexedOffsetBytes) {
        latestByPath.set(row.payloadPath, {
          payloadPath: row.payloadPath,
          lastIndexedOffsetBytes: nextOffset,
        })
      }
    }

    const values = []
    const placeholders = [...latestByPath.values()].map((row, index) => {
      const base = index * 2
      values.push(row.payloadPath, row.lastIndexedOffsetBytes)
      return `($${base + 1}, $${base + 2}, NOW())`
    })

    await query(
      `INSERT INTO storage_index_state (payload_path, last_indexed_offset_bytes, updated_at)
       VALUES ${placeholders.join(', ')}
       ON CONFLICT (payload_path)
       DO UPDATE SET
         last_indexed_offset_bytes = GREATEST(storage_index_state.last_indexed_offset_bytes, EXCLUDED.last_indexed_offset_bytes),
         updated_at = NOW()`,
      values,
    )
  }

  async insertRecoveredGaps(gapRows) {
    if (!gapRows.length) {
      return
    }

    const gapValues = []
    const gapPlaceholders = gapRows.map((gapEvent, rowIndex) => {
      const base = rowIndex * 7
      gapValues.push(
        gapEvent.vehicleId,
        gapEvent.channel,
        gapEvent.previousSequenceNumber,
        gapEvent.expectedSequenceNumber,
        gapEvent.actualSequenceNumber,
        gapEvent.missingPacketCount,
        gapEvent.packetTimestampMs,
      )
      return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`
    })

    await query(
      `INSERT INTO packet_sequence_gaps (
         vehicle_id,
         channel,
         previous_sequence_number,
         expected_sequence_number,
         actual_sequence_number,
         missing_packet_count,
         packet_timestamp_ms
       )
       VALUES ${gapPlaceholders.join(', ')}`,
      gapValues,
    )

    this.insertedGapEvents += gapRows.length
  }

  async runRetentionCleanup() {
    const now = Date.now()
    if (now - this.lastRetentionRunAt < 60 * 1000) {
      return
    }
    this.lastRetentionRunAt = now

    const { cutoffMs, deletedFiles } = cleanupExpiredPacketFiles(now)
    await query(
      `DELETE FROM raw_video_packets WHERE packet_timestamp_ms < $1`,
      [cutoffMs],
    )
    await query(
      `DELETE FROM packet_sequence_gaps WHERE packet_timestamp_ms < $1`,
      [cutoffMs],
    )
    if (deletedFiles.length > 0) {
      await query(
        `DELETE FROM storage_index_state WHERE payload_path = ANY($1::text[])`,
        [deletedFiles],
      )
    }

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
    const pendingDbRows = this.pendingRows.length
    const hasNewGapEvent = this.sequenceGapEvents > this.lastLoggedGapEvents
    const hasNewMissingPackets =
      this.sequenceGapPackets > this.lastLoggedMissingPackets
    const hasDbError =
      !!this.lastDbError && this.lastDbError !== this.lastLoggedDbError
    const hasBacklog =
      pendingDbRows > 0 &&
      (this.lastLoggedPendingRows === 0 ||
        pendingDbRows >= this.lastLoggedPendingRows + config.packetBatchSize)

    if (hasDbError || hasBacklog || hasNewGapEvent || hasNewMissingPackets) {
      const parts = [
        `Packets stored: ${this.processedPackets}`,
        `rate=${packetsPerSecond}/s`,
        `throughput=${megabytesPerSecond} MB/s`,
        `insertedDbRows=${this.insertedPackets}`,
        `pendingDbRows=${pendingDbRows}`,
        `gapEvents=${this.sequenceGapEvents}`,
        `missingPackets=${this.sequenceGapPackets}`,
      ]

      if (this.lastDbError) {
        parts.push(`dbError=${this.lastDbError}`)
      }
      if (this.unrecoverablePackets > 0) {
        parts.push(`unrecoverablePackets=${this.unrecoverablePackets}`)
      }

      console.warn(parts.join(' | '))
    }

    this.lastStatsPackets = this.processedPackets
    this.lastStatsBytes = this.processedBytes
    this.lastStatsAt = now
    this.lastLoggedGapEvents = this.sequenceGapEvents
    this.lastLoggedMissingPackets = this.sequenceGapPackets
    this.lastLoggedDbError = this.lastDbError
    this.lastLoggedPendingRows = pendingDbRows
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

    while (this.processingQueue) {
      await new Promise((resolve) => setTimeout(resolve, 50))
    }

    while (this.pendingRows.length > 0 || this.pendingGapRows.length > 0) {
      await this.flushPendingRows()
      if (this.processingQueue) {
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
      if (this.lastDbError) {
        break
      }
    }

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
    let gapEvent = null

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
          gapEvent = {
            vehicleId,
            channel,
            previousSequenceNumber: previous.lastSequence,
            expectedSequenceNumber: expected,
            actualSequenceNumber: sequenceNumber,
            missingPacketCount,
            packetTimestampMs,
          }
        }
      }
    }

    this.streamStates.set(key, {
      lastSequence: sequenceNumber,
      lastPacketTimestampMs: packetTimestampMs,
    })

    return gapEvent
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
      insertedPackets: this.insertedPackets,
      packetWriteLag: this.processedPackets - this.insertedPackets,
      pendingDbRows: this.pendingRows.length,
      pendingGapRows: this.pendingGapRows.length,
      reindexInProgress: this.reindexInProgress,
      retentionDays: RETENTION_DAYS,
      sequenceGapEvents: this.sequenceGapEvents,
      sequenceGapPackets: this.sequenceGapPackets,
      insertedGapEvents: this.insertedGapEvents,
      lastDbError: this.lastDbError,
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
