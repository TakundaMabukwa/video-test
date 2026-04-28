const {
  AckPolicy,
  DiscardPolicy,
  DeliverPolicy,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  connect,
  nanos,
} = require('nats')
const config = require('./config')

const META_LENGTH_BYTES = 4

function encodePacketQueueMessage({ meta, payloadBuffer, receivedAtMs = Date.now() }) {
  const safeMeta = {
    ...(meta || {}),
    receivedAtMs,
  }

  const metaBytes = Buffer.from(JSON.stringify(safeMeta), 'utf8')
  const payloadBytes = Buffer.isBuffer(payloadBuffer)
    ? payloadBuffer
    : Buffer.from(payloadBuffer || [])

  const header = Buffer.allocUnsafe(META_LENGTH_BYTES)
  header.writeUInt32BE(metaBytes.length, 0)
  return Buffer.concat([header, metaBytes, payloadBytes])
}

function decodePacketQueueMessage(data) {
  const source = Buffer.from(data || [])
  if (source.length < META_LENGTH_BYTES) {
    throw new Error('queue message too short')
  }

  const metaLength = source.readUInt32BE(0)
  const metaEnd = META_LENGTH_BYTES + metaLength
  if (source.length < metaEnd) {
    throw new Error('queue metadata truncated')
  }

  const meta = JSON.parse(source.slice(META_LENGTH_BYTES, metaEnd).toString('utf8'))
  const payloadBuffer = source.slice(metaEnd)
  return {
    meta,
    payloadBuffer,
  }
}

function isResourceMissingError(error) {
  const message = String(error?.message || error || '').toLowerCase()
  return (
    message.includes('stream not found') ||
    message.includes('consumer not found') ||
    message.includes('404')
  )
}

function createStreamConfig() {
  return {
    name: config.natsStreamName,
    subjects: [config.natsSubject],
    retention: RetentionPolicy.Workqueue,
    storage: StorageType.File,
    discard: DiscardPolicy.Old,
    max_age: nanos(config.natsStreamMaxAgeMs),
    max_bytes: config.natsStreamMaxBytes,
    duplicate_window: nanos(config.natsDuplicateWindowMs),
  }
}

function createConsumerConfig() {
  return {
    durable_name: config.natsConsumerName,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
    replay_policy: ReplayPolicy.Instant,
    ack_wait: nanos(config.natsAckWaitMs),
    max_ack_pending: config.natsConsumerMaxAckPending,
    max_deliver: config.natsConsumerMaxDeliver,
    inactive_threshold: nanos(config.natsConsumerInactiveThresholdMs),
    filter_subject: config.natsSubject,
  }
}

async function createPacketQueue({ role = 'app' } = {}) {
  const nc = await connect({
    servers: config.natsUrl,
    name: `video-feed-${role}-${process.pid}`,
  })
  const js = nc.jetstream()
  const jsm = await nc.jetstreamManager()

  async function ensureStream() {
    try {
      await jsm.streams.info(config.natsStreamName)
    } catch (error) {
      if (!isResourceMissingError(error)) {
        throw error
      }
      await jsm.streams.add(createStreamConfig())
    }
  }

  async function ensureConsumer() {
    try {
      await jsm.consumers.info(config.natsStreamName, config.natsConsumerName)
    } catch (error) {
      if (!isResourceMissingError(error)) {
        throw error
      }
      await jsm.consumers.add(config.natsStreamName, createConsumerConfig())
    }
  }

  async function publishPacket({ meta, payloadBuffer, receivedAtMs = Date.now() }) {
    const payload = encodePacketQueueMessage({ meta, payloadBuffer, receivedAtMs })
    return js.publish(config.natsSubject, payload, {
      timeout: config.natsPublishTimeoutMs,
    })
  }

  async function getConsumer() {
    return js.consumers.get(config.natsStreamName, config.natsConsumerName)
  }

  async function getStreamInfo() {
    return jsm.streams.info(config.natsStreamName)
  }

  async function close() {
    await nc.close()
  }

  return {
    publishPacket,
    ensureStream,
    ensureConsumer,
    getConsumer,
    getStreamInfo,
    close,
    decodePacketQueueMessage,
  }
}

module.exports = {
  createPacketQueue,
  encodePacketQueueMessage,
  decodePacketQueueMessage,
}
