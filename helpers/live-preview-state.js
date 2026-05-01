const fs = require('fs')
const path = require('path')

const RUNTIME_DIR = path.join(process.cwd(), 'runtime')
const LIVE_PREVIEW_ROOT = path.join(RUNTIME_DIR, 'live-preview')

function sanitizeSegment(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '_')
}

function getStreamDir(vehicleId, channel) {
  return path.join(
    LIVE_PREVIEW_ROOT,
    sanitizeSegment(vehicleId),
    `ch${Number(channel) || 0}`,
  )
}

function getFramePath(vehicleId, channel) {
  return path.join(getStreamDir(vehicleId, channel), 'latest.jpg')
}

function getMetaPath(vehicleId, channel) {
  return path.join(getStreamDir(vehicleId, channel), 'latest.json')
}

function ensureDir(vehicleId, channel) {
  fs.mkdirSync(getStreamDir(vehicleId, channel), { recursive: true })
}

function writeAtomicFile(targetPath, bufferOrText) {
  const tempPath = `${targetPath}.tmp`
  fs.writeFileSync(tempPath, bufferOrText)
  fs.renameSync(tempPath, targetPath)
}

function writeLatestPreview({
  vehicleId,
  channel,
  jpegBuffer,
  frameTimestampMs = null,
  updatedAtMs = Date.now(),
  sequence = null,
}) {
  ensureDir(vehicleId, channel)

  const meta = {
    vehicleId: String(vehicleId),
    channel: Number(channel),
    updatedAt: new Date(updatedAtMs).toISOString(),
    updatedAtMs,
    frameTimestampMs: Number.isFinite(Number(frameTimestampMs))
      ? Number(frameTimestampMs)
      : null,
    sequence: Number.isFinite(Number(sequence)) ? Number(sequence) : null,
    byteLength: Buffer.byteLength(jpegBuffer),
  }

  writeAtomicFile(getFramePath(vehicleId, channel), jpegBuffer)
  writeAtomicFile(getMetaPath(vehicleId, channel), JSON.stringify(meta, null, 2))
  return meta
}

function readLatestPreviewMeta(vehicleId, channel) {
  const metaPath = getMetaPath(vehicleId, channel)
  if (!fs.existsSync(metaPath)) {
    return null
  }

  try {
    return JSON.parse(fs.readFileSync(metaPath, 'utf8'))
  } catch {
    return null
  }
}

function readLatestPreview(vehicleId, channel) {
  const meta = readLatestPreviewMeta(vehicleId, channel)
  const framePath = getFramePath(vehicleId, channel)
  if (!meta || !fs.existsSync(framePath)) {
    return null
  }

  try {
    return {
      meta,
      jpegBuffer: fs.readFileSync(framePath),
    }
  } catch {
    return null
  }
}

function listActivePreviewStreams({ maxAgeMs = null } = {}) {
  if (!fs.existsSync(LIVE_PREVIEW_ROOT)) {
    return []
  }

  const rows = []
  const now = Date.now()
  const vehicleDirs = fs.readdirSync(LIVE_PREVIEW_ROOT, { withFileTypes: true })

  for (const vehicleEntry of vehicleDirs) {
    if (!vehicleEntry.isDirectory()) {
      continue
    }

    const vehiclePath = path.join(LIVE_PREVIEW_ROOT, vehicleEntry.name)
    const channelDirs = fs.readdirSync(vehiclePath, { withFileTypes: true })
    for (const channelEntry of channelDirs) {
      if (!channelEntry.isDirectory()) {
        continue
      }

      const metaPath = path.join(vehiclePath, channelEntry.name, 'latest.json')
      if (!fs.existsSync(metaPath)) {
        continue
      }

      try {
        const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'))
        if (
          Number.isFinite(Number(maxAgeMs)) &&
          Number(maxAgeMs) > 0 &&
          Number.isFinite(Number(meta.updatedAtMs)) &&
          now - Number(meta.updatedAtMs) > Number(maxAgeMs)
        ) {
          continue
        }
        rows.push(meta)
      } catch {}
    }
  }

  return rows.sort((a, b) => Number(b.updatedAtMs || 0) - Number(a.updatedAtMs || 0))
}

module.exports = {
  LIVE_PREVIEW_ROOT,
  getFramePath,
  getMetaPath,
  writeLatestPreview,
  readLatestPreviewMeta,
  readLatestPreview,
  listActivePreviewStreams,
}
