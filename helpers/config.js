const fs = require('fs')
const path = require('path')

function loadEnvFile() {
  const envPath = path.join(process.cwd(), '.env')
  if (!fs.existsSync(envPath)) {
    return
  }

  const content = fs.readFileSync(envPath, 'utf8')
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) {
      continue
    }

    const separatorIndex = line.indexOf('=')
    if (separatorIndex <= 0) {
      continue
    }

    const key = line.slice(0, separatorIndex).trim()
    const value = line.slice(separatorIndex + 1).trim()
    if (process.env[key] === undefined) {
      process.env[key] = value
    }
  }
}

loadEnvFile()

function getString(name, fallback = '') {
  const value = process.env[name]
  if (value === undefined || String(value).trim() === '') {
    return fallback
  }
  return String(value).trim()
}

function getNumber(name, fallback) {
  const parsed = Number(getString(name, String(fallback)))
  return Number.isFinite(parsed) ? parsed : fallback
}

function getBoolean(name, fallback = false) {
  const value = getString(name, String(fallback))
  if (/^(1|true|yes|on)$/i.test(value)) {
    return true
  }
  if (/^(0|false|no|off)$/i.test(value)) {
    return false
  }
  return fallback
}

function getLivePreviewSource() {
  const value = getString('LIVE_PREVIEW_SOURCE', 'ingest').toLowerCase()
  if (['ingest', 'worker', 'both', 'none'].includes(value)) {
    return value
  }
  return 'ingest'
}

function getArchiveWriteSource() {
  const value = getString('ARCHIVE_WRITE_SOURCE', 'ingest').toLowerCase()
  if (['ingest', 'worker', 'both', 'none'].includes(value)) {
    return value
  }
  return 'ingest'
}

const livePreviewSource = getLivePreviewSource()
const archiveWriteSource = getArchiveWriteSource()

module.exports = {
  relayHost: getString('RELAY_HOST', '209.38.206.44'),
  relayPort: getNumber('RELAY_PORT', 7081),
  relayIngestEnabled: getBoolean('RELAY_INGEST_ENABLED', true),
  apiPort: getNumber('API_PORT', 3201),
  internalWorkerToken: getString('INTERNAL_WORKER_TOKEN', ''),
  dbHost: getString('DB_HOST', '127.0.0.1'),
  dbPort: getNumber('DB_PORT', 5432),
  dbName: getString('DB_NAME', 'video_storage'),
  dbUser: getString('DB_USER', 'postgres'),
  dbPassword: getString('DB_PASSWORD', ''),
  dbPoolMax: getNumber('DB_POOL_MAX', 10),
  packetBatchSize: 500,
  packetFlushMs: 250,
  statsLogMs: 5000,
  mirrorRawOutput: false,
  storeHexPreview: false,
  rawDebugLogs: false,
  natsUrl: getString('NATS_URL', 'nats://127.0.0.1:4222'),
  natsStreamName: getString('NATS_STREAM_NAME', 'VIDEO_PACKET_STREAM'),
  natsSubject: getString('NATS_SUBJECT', 'video.packet'),
  natsConsumerName: getString('NATS_CONSUMER_NAME', 'video-packet-writer'),
  natsPublishTimeoutMs: getNumber('NATS_PUBLISH_TIMEOUT_MS', 5000),
  natsStreamMaxAgeMs: getNumber('NATS_STREAM_MAX_AGE_MS', 0),
  natsDuplicateWindowMs: getNumber('NATS_DUPLICATE_WINDOW_MS', 2 * 60 * 1000),
  natsStreamMaxBytes: getNumber('NATS_STREAM_MAX_BYTES', 0),
  natsAckWaitMs: getNumber('NATS_ACK_WAIT_MS', 5 * 60 * 1000),
  natsConsumerMaxAckPending: getNumber('NATS_CONSUMER_MAX_ACK_PENDING', 20000),
  natsConsumerMaxDeliver: getNumber('NATS_CONSUMER_MAX_DELIVER', -1),
  natsConsumerInactiveThresholdMs: getNumber(
    'NATS_CONSUMER_INACTIVE_THRESHOLD_MS',
    24 * 60 * 60 * 1000,
  ),
  natsConsumeBatchSize: getNumber('NATS_CONSUME_BATCH_SIZE', 500),
  natsConsumeExpiresMs: getNumber('NATS_CONSUME_EXPIRES_MS', 30000),
  natsConsumeIdleHeartbeatMs: getNumber('NATS_CONSUME_IDLE_HEARTBEAT_MS', 5000),
  queueWorkerEnabled: getBoolean('QUEUE_WORKER_ENABLED', true),
  livePreviewEnabled: getBoolean('LIVE_PREVIEW_ENABLED', true),
  livePreviewSource,
  archiveWriteSource,
  archiveWriteFromIngest:
    archiveWriteSource === 'ingest' || archiveWriteSource === 'both',
  archiveWriteFromWorker:
    archiveWriteSource === 'worker' || archiveWriteSource === 'both',
  retentionDays: Math.max(0, getNumber('RETENTION_DAYS', 0)),
  livePreviewFromIngest:
    getBoolean('LIVE_PREVIEW_ENABLED', true) &&
    (livePreviewSource === 'ingest' || livePreviewSource === 'both'),
  livePreviewFromWorker:
    getBoolean('LIVE_PREVIEW_ENABLED', true) &&
    (livePreviewSource === 'worker' || livePreviewSource === 'both'),
  livePreviewFps: getNumber('LIVE_PREVIEW_FPS', 4),
  livePreviewWidth: getNumber('LIVE_PREVIEW_WIDTH', 960),
  livePreviewJpegQuality: getNumber('LIVE_PREVIEW_JPEG_QUALITY', 6),
  livePreviewIdleMs: getNumber('LIVE_PREVIEW_IDLE_MS', 15000),
  livePreviewPollMs: getNumber('LIVE_PREVIEW_POLL_MS', 250),
  livePreviewWaitMs: getNumber('LIVE_PREVIEW_WAIT_MS', 10000),
  livePreviewMaxAgeMs: getNumber('LIVE_PREVIEW_MAX_AGE_MS', 45000),
  liveHlsEnabled: getBoolean('LIVE_HLS_ENABLED', true),
  liveHlsAlwaysOn: getBoolean('LIVE_HLS_ALWAYS_ON', true),
  liveHlsIdleMs: getNumber('LIVE_HLS_IDLE_MS', 20000),
  liveHlsRequestTtlMs: getNumber('LIVE_HLS_REQUEST_TTL_MS', 30000),
  liveHlsWaitMs: getNumber('LIVE_HLS_WAIT_MS', 15000),
  liveHlsMaxAgeMs: getNumber('LIVE_HLS_MAX_AGE_MS', 20000),
  liveHlsSegmentTimeSec: getNumber('LIVE_HLS_SEGMENT_TIME_SEC', 1),
  liveHlsListSize: getNumber('LIVE_HLS_LIST_SIZE', 6),
  liveHlsDeleteThreshold: getNumber('LIVE_HLS_DELETE_THRESHOLD', 2),
  alertVideoCaptureEnabled: getBoolean('ALERT_VIDEO_CAPTURE_ENABLED', true),
  alertVideoCapturePreRollMs: getNumber('ALERT_VIDEO_CAPTURE_PRE_ROLL_MS', 30000),
  alertVideoCapturePostRollMs: getNumber('ALERT_VIDEO_CAPTURE_POST_ROLL_MS', 30000),
  alertVideoCaptureRetryIntervalMs: getNumber('ALERT_VIDEO_CAPTURE_RETRY_INTERVAL_MS', 5000),
  alertVideoCaptureWaitTimeoutMs: getNumber('ALERT_VIDEO_CAPTURE_WAIT_TIMEOUT_MS', 180000),
}
