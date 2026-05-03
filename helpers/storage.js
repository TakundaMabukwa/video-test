const fs = require('fs')
const path = require('path')
const config = require('./config')

const HEADER_SIZE = 12
const STORAGE_ROOT = path.join(process.cwd(), 'storage')
const RETENTION_DAYS = Math.max(0, Number(config.retentionDays || 0))
const RETENTION_MS = RETENTION_DAYS * 24 * 60 * 60 * 1000

const fileStates = new Map()

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function toDateOnly(timestampMs) {
  return new Date(timestampMs).toISOString().slice(0, 10)
}

function toHourStamp(timestampMs) {
  return new Date(timestampMs).toISOString().slice(11, 13)
}

function resolvePacketFile(vehicleId, channel, timestampMs) {
  const date = toDateOnly(timestampMs)
  const hour = toHourStamp(timestampMs)
  return path.join(STORAGE_ROOT, vehicleId, `ch${channel}`, date, `${hour}.packets`)
}

function getOrCreateFileState(filePath) {
  let state = fileStates.get(filePath)
  if (state) {
    return state
  }

  ensureDir(path.dirname(filePath))
  const exists = fs.existsSync(filePath)
  const fd = fs.openSync(filePath, exists ? 'r+' : 'w+')
  const size = exists ? fs.fstatSync(fd).size : 0

  state = {
    fd,
    size,
  }
  fileStates.set(filePath, state)
  return state
}

function appendPacket(meta, payloadBuffer) {
  const vehicleId = String(meta.vehicleId || '').trim()
  const channel = Number(meta.channel || 0)
  const timestampMs = Number(
    meta.archiveTimestampMs ?? meta.receivedAtMs ?? meta.timestamp ?? Date.now(),
  )
  if (!vehicleId || !Number.isFinite(channel) || channel <= 0) {
    return null
  }

  const filePath = resolvePacketFile(vehicleId, channel, timestampMs)
  const state = getOrCreateFileState(filePath)
  const fileOffsetBytes = state.size

  const header = Buffer.allocUnsafe(HEADER_SIZE)
  header.writeBigUInt64BE(BigInt(timestampMs), 0)
  header.writeUInt32BE(payloadBuffer.length, 8)

  fs.writeSync(state.fd, header, 0, header.length, state.size)
  state.size += header.length
  fs.writeSync(state.fd, payloadBuffer, 0, payloadBuffer.length, state.size)
  state.size += payloadBuffer.length
  fs.fsyncSync(state.fd)

  return {
    filePath,
    fileOffsetBytes,
  }
}

function enumerateHours(fromMs, toMs) {
  const hours = []
  const cursor = new Date(fromMs)
  cursor.setUTCMinutes(0, 0, 0)
  const end = new Date(toMs)
  end.setUTCMinutes(0, 0, 0)

  while (cursor.getTime() <= end.getTime()) {
    hours.push({
      date: cursor.toISOString().slice(0, 10),
      hour: cursor.toISOString().slice(11, 13),
    })
    cursor.setUTCHours(cursor.getUTCHours() + 1)
  }

  return hours
}

function buildRangeFilePaths({ vehicleId, channel, fromMs, toMs }) {
  const hours = enumerateHours(fromMs, toMs)
  const filePaths = []

  for (const hour of hours) {
    filePaths.push(
      path.join(
        STORAGE_ROOT,
        vehicleId,
        `ch${channel}`,
        hour.date,
        `${hour.hour}.packets`,
      ),
    )
  }

  return filePaths
}

async function readPacketsForRange({ vehicleId, channel, fromMs, toMs, onPacket }) {
  for (const filePath of buildRangeFilePaths({ vehicleId, channel, fromMs, toMs })) {
    if (!fs.existsSync(filePath)) {
      continue
    }

    await scanPacketFile(filePath, async ({ timestampMs, packet, fileOffsetBytes }) => {
      if (timestampMs >= fromMs && timestampMs <= toMs) {
        await onPacket({ timestampMs, packet, filePath, fileOffsetBytes })
      }
    })
  }
}

async function summarizeCoverageForRange({ fromMs, toMs, vehicleId = null }) {
  const summary = new Map()
  const targetVehicleId = vehicleId ? String(vehicleId).trim() : null
  const packetFiles = listPacketFiles()

  for (const filePath of packetFiles) {
    const identity = parseStoragePath(filePath)
    if (!identity) {
      continue
    }

    if (targetVehicleId && identity.vehicleId !== targetVehicleId) {
      continue
    }

    const matchingWindow = buildRangeFilePaths({
      vehicleId: identity.vehicleId,
      channel: identity.channel,
      fromMs,
      toMs,
    })
    if (!matchingWindow.includes(filePath)) {
      continue
    }

    await scanPacketFile(filePath, async ({ timestampMs, packetSize }) => {
      if (timestampMs < fromMs || timestampMs > toMs) {
        return
      }

      const key = `${identity.vehicleId}:${identity.channel}`
      const existing = summary.get(key) || {
        vehicle_id: identity.vehicleId,
        channel: identity.channel,
        packet_count: 0,
        byte_count: 0,
        first_packet_timestamp_ms: null,
        last_packet_timestamp_ms: null,
      }

      existing.packet_count += 1
      existing.byte_count += Number(packetSize || 0)
      existing.first_packet_timestamp_ms =
        existing.first_packet_timestamp_ms === null
          ? timestampMs
          : Math.min(existing.first_packet_timestamp_ms, timestampMs)
      existing.last_packet_timestamp_ms =
        existing.last_packet_timestamp_ms === null
          ? timestampMs
          : Math.max(existing.last_packet_timestamp_ms, timestampMs)

      summary.set(key, existing)
    })
  }

  return [...summary.values()]
    .map((row) => ({
      ...row,
      first_packet_time:
        row.first_packet_timestamp_ms !== null
          ? new Date(row.first_packet_timestamp_ms).toISOString()
          : null,
      last_packet_time:
        row.last_packet_timestamp_ms !== null
          ? new Date(row.last_packet_timestamp_ms).toISOString()
          : null,
    }))
    .sort((a, b) => {
      if (b.packet_count !== a.packet_count) {
        return b.packet_count - a.packet_count
      }
      return (b.last_packet_timestamp_ms || 0) - (a.last_packet_timestamp_ms || 0)
    })
}

function readPacketAt(payloadPath, fileOffsetBytes, packetSize) {
  const totalLength = HEADER_SIZE + packetSize
  const fd = fs.openSync(payloadPath, 'r')

  try {
    const recordBuffer = Buffer.allocUnsafe(totalLength)
    const bytesRead = fs.readSync(fd, recordBuffer, 0, totalLength, fileOffsetBytes)
    if (bytesRead < totalLength) {
      return null
    }

    const storedTimestampMs = Number(recordBuffer.readBigUInt64BE(0))
    const storedPacketLength = recordBuffer.readUInt32BE(8)
    if (storedPacketLength !== packetSize) {
      return null
    }

    return {
      timestampMs: storedTimestampMs,
      packet: recordBuffer.slice(HEADER_SIZE),
      payloadPath,
      fileOffsetBytes,
    }
  } finally {
    fs.closeSync(fd)
  }
}

async function scanPacketFile(filePath, onRecord, startOffset = 0) {
  const fd = fs.openSync(filePath, 'r')

  try {
    const stats = fs.fstatSync(fd)
    let offset = Math.max(0, Number(startOffset || 0))
    const header = Buffer.allocUnsafe(HEADER_SIZE)

    while (offset + HEADER_SIZE <= stats.size) {
      const headerRead = fs.readSync(fd, header, 0, HEADER_SIZE, offset)
      if (headerRead < HEADER_SIZE) {
        break
      }

      const timestampMs = Number(header.readBigUInt64BE(0))
      const packetLength = header.readUInt32BE(8)
      const packetBuffer = Buffer.allocUnsafe(packetLength)
      const packetRead = fs.readSync(fd, packetBuffer, 0, packetLength, offset + HEADER_SIZE)
      if (packetRead < packetLength) {
        break
      }

      await onRecord({
        timestampMs,
        packet: packetBuffer,
        fileOffsetBytes: offset,
        packetSize: packetLength,
      })

      offset += HEADER_SIZE + packetLength
    }

    return {
      finalOffsetBytes: offset,
      fileSizeBytes: stats.size,
    }
  } finally {
    fs.closeSync(fd)
  }
}

function parseStoragePath(filePath) {
  const relative = path.relative(STORAGE_ROOT, filePath)
  const parts = relative.split(path.sep)
  if (parts.length !== 4) {
    return null
  }

  const [vehicleId, channelPart] = parts
  const channel = Number(String(channelPart || '').replace(/^ch/i, ''))
  if (!vehicleId || !Number.isFinite(channel) || channel <= 0) {
    return null
  }

  return {
    vehicleId,
    channel,
  }
}

function parseStorageHourFromPath(filePath) {
  const relative = path.relative(STORAGE_ROOT, filePath)
  const parts = relative.split(path.sep)
  if (parts.length !== 4) {
    return null
  }

  const [vehicleId, channelPart, datePart, filePart] = parts
  const channel = Number(String(channelPart || '').replace(/^ch/i, ''))
  const hourPart = String(filePart || '').replace(/\.packets$/i, '')

  if (!vehicleId || !Number.isFinite(channel) || channel <= 0) {
    return null
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
    return null
  }
  if (!/^\d{2}$/.test(hourPart)) {
    return null
  }

  const startMs = Date.parse(`${datePart}T${hourPart}:00:00.000Z`)
  if (!Number.isFinite(startMs)) {
    return null
  }

  return {
    vehicleId,
    channel,
    startMs,
    endMs: startMs + (60 * 60 * 1000) - 1,
    date: datePart,
    hour: hourPart,
  }
}

function listPacketFiles(root = STORAGE_ROOT) {
  const files = []

  function walk(current) {
    if (!fs.existsSync(current)) {
      return
    }

    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        walk(fullPath)
        continue
      }
      if (entry.isFile() && fullPath.endsWith('.packets')) {
        files.push(fullPath)
      }
    }
  }

  walk(root)
  return files
}

function summarizeVehicleApproxCoverage(vehicleId) {
  const normalizedVehicleId = String(vehicleId || '').trim()
  if (!normalizedVehicleId) {
    return []
  }

  const targetRoot = path.join(STORAGE_ROOT, normalizedVehicleId)
  if (!fs.existsSync(targetRoot)) {
    return []
  }

  const summary = new Map()
  const files = listPacketFiles(targetRoot)
  for (const filePath of files) {
    const parsed = parseStorageHourFromPath(filePath)
    if (!parsed || parsed.vehicleId !== normalizedVehicleId) {
      continue
    }

    const key = `${parsed.vehicleId}:${parsed.channel}`
    const existing = summary.get(key) || {
      vehicle_id: parsed.vehicleId,
      channel: parsed.channel,
      file_count: 0,
      approx_first_packet_timestamp_ms: null,
      approx_last_packet_timestamp_ms: null,
    }

    existing.file_count += 1
    existing.approx_first_packet_timestamp_ms =
      existing.approx_first_packet_timestamp_ms === null
        ? parsed.startMs
        : Math.min(existing.approx_first_packet_timestamp_ms, parsed.startMs)
    existing.approx_last_packet_timestamp_ms =
      existing.approx_last_packet_timestamp_ms === null
        ? parsed.endMs
        : Math.max(existing.approx_last_packet_timestamp_ms, parsed.endMs)

    summary.set(key, existing)
  }

  return [...summary.values()]
    .map((row) => ({
      ...row,
      approx_first_packet_time:
        row.approx_first_packet_timestamp_ms !== null
          ? new Date(row.approx_first_packet_timestamp_ms).toISOString()
          : null,
      approx_last_packet_time:
        row.approx_last_packet_timestamp_ms !== null
          ? new Date(row.approx_last_packet_timestamp_ms).toISOString()
          : null,
    }))
    .sort((a, b) => a.channel - b.channel)
}

function removeEmptyDirectories(startPath, stopAt = STORAGE_ROOT) {
  let current = startPath

  while (current && current.startsWith(stopAt) && current !== stopAt) {
    if (!fs.existsSync(current)) {
      current = path.dirname(current)
      continue
    }

    const entries = fs.readdirSync(current)
    if (entries.length > 0) {
      break
    }

    fs.rmdirSync(current)
    current = path.dirname(current)
  }
}

function cleanupExpiredPacketFiles(nowMs = Date.now()) {
  if (RETENTION_DAYS <= 0 || RETENTION_MS <= 0) {
    return {
      cutoffMs: null,
      deletedFiles: [],
    }
  }

  const cutoffMs = nowMs - RETENTION_MS
  const deletedFiles = []

  for (const filePath of listPacketFiles()) {
    const stats = fs.statSync(filePath)
    if (stats.mtimeMs > cutoffMs) {
      continue
    }

    const openState = fileStates.get(filePath)
    if (openState) {
      try {
        fs.closeSync(openState.fd)
      } catch {}
      fileStates.delete(filePath)
    }

    fs.unlinkSync(filePath)
    deletedFiles.push(filePath)
    removeEmptyDirectories(path.dirname(filePath))
  }

  return {
    cutoffMs,
    deletedFiles,
  }
}

async function closeStorageStreams() {
  for (const [filePath, state] of fileStates.entries()) {
    try {
      fs.fsyncSync(state.fd)
    } catch {}
    try {
      fs.closeSync(state.fd)
    } catch {}
    fileStates.delete(filePath)
  }
}

module.exports = {
  HEADER_SIZE,
  STORAGE_ROOT,
  RETENTION_DAYS,
  RETENTION_MS,
  appendPacket,
  readPacketsForRange,
  readPacketAt,
  scanPacketFile,
  parseStoragePath,
  summarizeVehicleApproxCoverage,
  listPacketFiles,
  summarizeCoverageForRange,
  cleanupExpiredPacketFiles,
  closeStorageStreams,
}
