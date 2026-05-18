const JT1078_MAGIC = 0x30316364
const HEADER_LENGTH = 30
const JT1078_MAGIC_BYTES = Buffer.from([0x30, 0x31, 0x63, 0x64])

function findAnnexBStart(buffer, fromIndex = 0) {
  for (let i = fromIndex; i <= buffer.length - 4; i += 1) {
    const isFour =
      buffer[i] === 0x00 &&
      buffer[i + 1] === 0x00 &&
      buffer[i + 2] === 0x00 &&
      buffer[i + 3] === 0x01
    if (isFour) {
      return { index: i, startCodeLength: 4 }
    }

    const isThree =
      buffer[i] === 0x00 &&
      buffer[i + 1] === 0x00 &&
      buffer[i + 2] === 0x01
    if (isThree) {
      return { index: i, startCodeLength: 3 }
    }
  }

  return null
}

function splitAnnexBNals(buffer) {
  const parts = []
  let cursor = 0

  while (cursor < buffer.length) {
    const start = findAnnexBStart(buffer, cursor)
    if (!start) {
      break
    }

    const next = findAnnexBStart(buffer, start.index + start.startCodeLength)
    const end = next ? next.index : buffer.length
    parts.push(buffer.slice(start.index, end))
    cursor = end
  }

  return parts
}

function getNalType(nalBuffer) {
  const start = findAnnexBStart(nalBuffer)
  if (!start) {
    return null
  }

  const headerIndex = start.index + start.startCodeLength
  if (headerIndex >= nalBuffer.length) {
    return null
  }

  const header = nalBuffer[headerIndex]
  return {
    h264: header & 0x1f,
  }
}

function isKeyFrameNal(nalType) {
  if (!nalType) {
    return false
  }

  return nalType.h264 === 5
}

function prependParameterSets(frameBuffer, parameterSets) {
  const existingNals = splitAnnexBNals(frameBuffer)
  const existingTypes = new Set(
    existingNals
      .map((nal) => getNalType(nal))
      .filter(Boolean)
      .map((nalType) => nalType.h264),
  )

  const prefixes = []
  for (const type of ['vps', 'sps', 'pps']) {
    const value = parameterSets[type]
    if (!value) {
      continue
    }

    const nalType = getNalType(value)
    if (!nalType) {
      continue
    }

    if (existingTypes.has(nalType.h264)) {
      continue
    }

    prefixes.push(value)
  }

  if (!prefixes.length) {
    return frameBuffer
  }

  return Buffer.concat([...prefixes, frameBuffer])
}

function cacheParameterSets(frameBuffer, parameterSets) {
  for (const nal of splitAnnexBNals(frameBuffer)) {
    const nalType = getNalType(nal)
    if (!nalType) {
      continue
    }

    if (nalType.h264 === 7) {
      parameterSets.sps = Buffer.from(nal)
    } else if (nalType.h264 === 8) {
      parameterSets.pps = Buffer.from(nal)
    }
  }
}

function parsePacket(packetBuffer) {
  if (!Buffer.isBuffer(packetBuffer) || packetBuffer.length < HEADER_LENGTH) {
    return null
  }

  if (packetBuffer.readUInt32BE(0) !== JT1078_MAGIC) {
    return null
  }

  const declaredPayloadLength = packetBuffer.readUInt16BE(28)
  const payloadEnd = Math.min(packetBuffer.length, HEADER_LENGTH + declaredPayloadLength)
  const payload = packetBuffer.slice(HEADER_LENGTH, payloadEnd)

  if (!payload.length) {
    return null
  }

  return {
    sequence: packetBuffer.readUInt16BE(6),
    sim: packetBuffer.slice(8, 14).toString('hex'),
    channel: packetBuffer[14],
    dataType: packetBuffer[15] >> 4,
    fragmentType: packetBuffer[15] & 0x0f,
    timestamp: Number(packetBuffer.readBigUInt64BE(16)),
    payload,
  }
}

function createFrameAssembler() {
  const parameterSets = {
    sps: null,
    pps: null,
  }

  let fragments = []
  let expectedSequence = null
  let waitingForKeyFrame = true
  let fragmentTimestamp = null

  function resetFragments() {
    fragments = []
    expectedSequence = null
    fragmentTimestamp = null
  }

  function finalizeFrame(frameBuffer, timestamp) {
    if (!frameBuffer?.length) {
      return []
    }

    if (!findAnnexBStart(frameBuffer)) {
      return []
    }

    cacheParameterSets(frameBuffer, parameterSets)

    const isKeyFrame = splitAnnexBNals(frameBuffer).some((nal) => isKeyFrameNal(getNalType(nal)))
    if (isKeyFrame) {
      waitingForKeyFrame = false
      return [{ buffer: prependParameterSets(frameBuffer, parameterSets), timestamp }]
    }

    if (waitingForKeyFrame) {
      return []
    }

    return [{ buffer: frameBuffer, timestamp }]
  }

  function pushPacket(packetBuffer) {
    const parsed = parsePacket(packetBuffer)
    if (!parsed) {
      resetFragments()
      return []
    }

    const { fragmentType, sequence, payload, timestamp } = parsed

    if (
      fragments.length &&
      expectedSequence !== null &&
      sequence !== expectedSequence &&
      fragmentType !== 1
    ) {
      resetFragments()
    }

    if (fragmentType === 0) {
      resetFragments()
      return finalizeFrame(payload, timestamp)
    }

    if (fragmentType === 1) {
      fragments = [payload]
      expectedSequence = (sequence + 1) & 0xffff
      fragmentTimestamp = timestamp
      return []
    }

    if (fragmentType === 3) {
      if (!fragments.length) {
        return []
      }

      fragments.push(payload)
      expectedSequence = (sequence + 1) & 0xffff
      return []
    }

    if (fragmentType === 2) {
      if (!fragments.length) {
        return finalizeFrame(payload, timestamp)
      }

      fragments.push(payload)
      const frameBuffer = Buffer.concat(fragments)
      const frameTimestamp = fragmentTimestamp ?? timestamp
      resetFragments()
      return finalizeFrame(frameBuffer, frameTimestamp)
    }

    resetFragments()
    return finalizeFrame(payload, timestamp)
  }

  return {
    pushPacket,
  }
}

function extractPackets(inputBuffer, carryBuffer = Buffer.alloc(0), options = {}) {
  const maxBodyLength = Number(options.maxBodyLength || 0) > 0
    ? Number(options.maxBodyLength)
    : 1048576

  let buffer =
    Buffer.isBuffer(carryBuffer) && carryBuffer.length
      ? Buffer.concat([carryBuffer, inputBuffer])
      : inputBuffer

  const packets = []
  let droppedBytes = 0
  let parseErrors = 0

  while (Buffer.isBuffer(buffer) && buffer.length >= HEADER_LENGTH) {
    const magicOffset = buffer.indexOf(JT1078_MAGIC_BYTES)
    if (magicOffset === -1) {
      const keepTailLength = Math.min(buffer.length, JT1078_MAGIC_BYTES.length - 1)
      droppedBytes += Math.max(0, buffer.length - keepTailLength)
      buffer = buffer.subarray(Math.max(0, buffer.length - keepTailLength))
      break
    }

    if (magicOffset > 0) {
      droppedBytes += magicOffset
      buffer = buffer.subarray(magicOffset)
    }

    if (buffer.length < HEADER_LENGTH) {
      break
    }

    const bodyLength = buffer.readUInt16BE(28)
    if (bodyLength <= 0 || bodyLength > maxBodyLength) {
      parseErrors += 1
      droppedBytes += 1
      buffer = buffer.subarray(1)
      continue
    }

    const fullLength = HEADER_LENGTH + bodyLength
    if (buffer.length < fullLength) {
      break
    }

    packets.push(buffer.subarray(0, fullLength))
    buffer = buffer.subarray(fullLength)
  }

  return {
    packets,
    remainder: Buffer.isBuffer(buffer) ? buffer : Buffer.alloc(0),
    droppedBytes,
    parseErrors,
  }
}

function isKeyFrameBuffer(frameBuffer) {
  if (!frameBuffer?.length || !findAnnexBStart(frameBuffer)) {
    return false
  }

  return splitAnnexBNals(frameBuffer).some((nal) => isKeyFrameNal(getNalType(nal)))
}

function isPreviewKeyFrameBuffer(frameBuffer) {
  if (!frameBuffer?.length || !findAnnexBStart(frameBuffer)) {
    return false
  }

  return splitAnnexBNals(frameBuffer).some((nal) => {
    const nalType = getNalType(nal)
    if (!nalType) {
      return false
    }

    return (
      nalType.h264 === 5
    )
  })
}

function hasRequiredParameterSets(frameBuffer) {
  if (!frameBuffer?.length || !findAnnexBStart(frameBuffer)) {
    return false
  }

  let hasH264Sps = false
  let hasH264Pps = false

  for (const nal of splitAnnexBNals(frameBuffer)) {
    const nalType = getNalType(nal)
    if (!nalType) {
      continue
    }

    if (nalType.h264 === 7) {
      hasH264Sps = true
    } else if (nalType.h264 === 8) {
      hasH264Pps = true
    }
  }

  if (hasH264Sps || hasH264Pps) {
    return hasH264Sps && hasH264Pps
  }

  return false
}

function isDecodableSyncFrameBuffer(frameBuffer) {
  return isPreviewKeyFrameBuffer(frameBuffer) && hasRequiredParameterSets(frameBuffer)
}

module.exports = {
  JT1078_MAGIC,
  HEADER_LENGTH,
  parsePacket,
  extractPackets,
  createFrameAssembler,
  findAnnexBStart,
  splitAnnexBNals,
  isKeyFrameBuffer,
  isPreviewKeyFrameBuffer,
  hasRequiredParameterSets,
  isDecodableSyncFrameBuffer,
}
