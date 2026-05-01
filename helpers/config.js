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

module.exports = {
  relayHost: getString('RELAY_HOST', '209.38.206.44'),
  relayPort: getNumber('RELAY_PORT', 7081),
  apiPort: getNumber('API_PORT', 3201),
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
  natsStreamMaxAgeMs: getNumber('NATS_STREAM_MAX_AGE_MS', 72 * 60 * 60 * 1000),
  natsDuplicateWindowMs: getNumber('NATS_DUPLICATE_WINDOW_MS', 2 * 60 * 1000),
  natsStreamMaxBytes: getNumber('NATS_STREAM_MAX_BYTES', 50 * 1024 * 1024 * 1024),
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
  livePreviewFps: getNumber('LIVE_PREVIEW_FPS', 4),
  livePreviewWidth: getNumber('LIVE_PREVIEW_WIDTH', 960),
  livePreviewJpegQuality: getNumber('LIVE_PREVIEW_JPEG_QUALITY', 6),
  livePreviewIdleMs: getNumber('LIVE_PREVIEW_IDLE_MS', 15000),
  livePreviewPollMs: getNumber('LIVE_PREVIEW_POLL_MS', 250),
  livePreviewWaitMs: getNumber('LIVE_PREVIEW_WAIT_MS', 10000),
  livePreviewMaxAgeMs: getNumber('LIVE_PREVIEW_MAX_AGE_MS', 45000),
}
