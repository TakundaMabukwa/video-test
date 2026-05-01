const fs = require('fs')
const path = require('path')

const RUNTIME_DIR = path.join(process.cwd(), 'runtime')
const LIVE_HLS_ROOT = path.join(RUNTIME_DIR, 'live-hls')
const LIVE_HLS_REQUEST_ROOT = path.join(RUNTIME_DIR, 'live-hls-requests')

function sanitizeSegment(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '_')
}

function getStreamDir(vehicleId, channel) {
  return path.join(
    LIVE_HLS_ROOT,
    sanitizeSegment(vehicleId),
    `ch${Number(channel) || 0}`,
  )
}

function getPlaylistPath(vehicleId, channel) {
  return path.join(getStreamDir(vehicleId, channel), 'playlist.m3u8')
}

function getStatusPath(vehicleId, channel) {
  return path.join(getStreamDir(vehicleId, channel), 'status.json')
}

function getRequestPath(vehicleId, channel) {
  return path.join(
    LIVE_HLS_REQUEST_ROOT,
    `${sanitizeSegment(vehicleId)}_ch${Number(channel) || 0}.json`,
  )
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function ensureStreamDir(vehicleId, channel) {
  ensureDir(getStreamDir(vehicleId, channel))
}

function ensureRequestDir() {
  ensureDir(LIVE_HLS_REQUEST_ROOT)
}

function writeAtomicFile(targetPath, bufferOrText) {
  const tempPath = `${targetPath}.tmp`
  fs.writeFileSync(tempPath, bufferOrText)
  fs.renameSync(tempPath, targetPath)
}

function clearStreamFiles(vehicleId, channel) {
  const dirPath = getStreamDir(vehicleId, channel)
  ensureDir(dirPath)

  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    if (!entry.isFile()) {
      continue
    }
    const filePath = path.join(dirPath, entry.name)
    try {
      fs.unlinkSync(filePath)
    } catch {}
  }
}

function writeLiveHlsStatus({
  vehicleId,
  channel,
  updatedAtMs = Date.now(),
  frameTimestampMs = null,
  source = null,
  sequence = null,
}) {
  ensureStreamDir(vehicleId, channel)

  const playlistPath = getPlaylistPath(vehicleId, channel)
  const meta = {
    vehicleId: String(vehicleId),
    channel: Number(channel),
    updatedAt: new Date(updatedAtMs).toISOString(),
    updatedAtMs,
    frameTimestampMs: Number.isFinite(Number(frameTimestampMs))
      ? Number(frameTimestampMs)
      : null,
    source: source ? String(source) : null,
    sequence: Number.isFinite(Number(sequence)) ? Number(sequence) : null,
    playlistExists: fs.existsSync(playlistPath),
  }

  writeAtomicFile(getStatusPath(vehicleId, channel), JSON.stringify(meta, null, 2))
  return meta
}

function readJsonFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return null
  }

  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

function readLiveHlsStatus(vehicleId, channel) {
  return readJsonFile(getStatusPath(vehicleId, channel))
}

function listActiveLiveHlsStreams({ maxAgeMs = null } = {}) {
  if (!fs.existsSync(LIVE_HLS_ROOT)) {
    return []
  }

  const rows = []
  const now = Date.now()
  const vehicleDirs = fs.readdirSync(LIVE_HLS_ROOT, { withFileTypes: true })

  for (const vehicleEntry of vehicleDirs) {
    if (!vehicleEntry.isDirectory()) {
      continue
    }

    const vehiclePath = path.join(LIVE_HLS_ROOT, vehicleEntry.name)
    const channelDirs = fs.readdirSync(vehiclePath, { withFileTypes: true })
    for (const channelEntry of channelDirs) {
      if (!channelEntry.isDirectory()) {
        continue
      }

      const statusPath = path.join(vehiclePath, channelEntry.name, 'status.json')
      if (!fs.existsSync(statusPath)) {
        continue
      }

      try {
        const meta = JSON.parse(fs.readFileSync(statusPath, 'utf8'))
        const channel = Number(meta?.channel || 0)
        const vehicleId = String(meta?.vehicleId || '').trim()
        const updatedAtMs = Number(meta?.updatedAtMs || 0)
        const playlistPath = getPlaylistPath(vehicleId, channel)

        if (!vehicleId || !Number.isFinite(channel) || channel <= 0) {
          continue
        }
        if (!fs.existsSync(playlistPath) || fs.statSync(playlistPath).size <= 0) {
          continue
        }
        if (
          Number.isFinite(Number(maxAgeMs)) &&
          Number(maxAgeMs) > 0 &&
          Number.isFinite(updatedAtMs) &&
          now - updatedAtMs > Number(maxAgeMs)
        ) {
          continue
        }

        rows.push(meta)
      } catch {}
    }
  }

  return rows.sort((a, b) => Number(b.updatedAtMs || 0) - Number(a.updatedAtMs || 0))
}

function touchLiveHlsRequest({
  vehicleId,
  channel,
  requestedAtMs = Date.now(),
}) {
  ensureRequestDir()

  const payload = {
    vehicleId: String(vehicleId),
    channel: Number(channel),
    requestedAt: new Date(requestedAtMs).toISOString(),
    requestedAtMs,
  }

  writeAtomicFile(getRequestPath(vehicleId, channel), JSON.stringify(payload, null, 2))
  return payload
}

function readLiveHlsRequest(vehicleId, channel) {
  return readJsonFile(getRequestPath(vehicleId, channel))
}

function hasRecentLiveHlsRequest(vehicleId, channel, maxAgeMs) {
  if (!Number.isFinite(Number(maxAgeMs)) || Number(maxAgeMs) <= 0) {
    return false
  }

  const request = readLiveHlsRequest(vehicleId, channel)
  const requestedAtMs = Number(request?.requestedAtMs || 0)
  if (!Number.isFinite(requestedAtMs) || requestedAtMs <= 0) {
    return false
  }

  return Date.now() - requestedAtMs <= Number(maxAgeMs)
}

module.exports = {
  LIVE_HLS_ROOT,
  getStreamDir,
  getPlaylistPath,
  getStatusPath,
  clearStreamFiles,
  writeLiveHlsStatus,
  readLiveHlsStatus,
  listActiveLiveHlsStreams,
  touchLiveHlsRequest,
  readLiveHlsRequest,
  hasRecentLiveHlsRequest,
}
