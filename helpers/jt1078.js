const JT1078_MAGIC = 0x30316364
const HEADER_LENGTH = 30

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
    h265: (header >> 1) & 0x3f,
  }
}

function isKeyFrameNal(nalType) {
  if (!nalType) {
    return false
  }

  return (
    nalType.h264 === 5 ||
    nalType.h264 === 7 ||
    nalType.h264 === 8 ||
    nalType.h265 === 19 ||
    nalType.h265 === 20 ||
    nalType.h265 === 32 ||
    nalType.h265 === 33 ||
    nalType.h265 === 34
  )
}

function prependParameterSets(frameBuffer, parameterSets) {
  const existingNals = splitAnnexBNals(frameBuffer)
  const existingTypes = new Set(
    existingNals
      .map((nal) => getNalType(nal))
      .filter(Boolean)
      .flatMap((nalType) => [nalType.h264, nalType.h265]),
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

    if (existingTypes.has(nalType.h264) || existingTypes.has(nalType.h265)) {
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

    if (nalType.h264 === 7 || nalType.h265 === 33) {
      parameterSets.sps = Buffer.from(nal)
    } else if (nalType.h264 === 8 || nalType.h265 === 34) {
      parameterSets.pps = Buffer.from(nal)
    } else if (nalType.h265 === 32) {
      parameterSets.vps = Buffer.from(nal)
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
    vps: null,
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
      nalType.h264 === 5 ||
      nalType.h265 === 19 ||
      nalType.h265 === 20 ||
      nalType.h265 === 21
    )
  })
}

module.exports = {
  parsePacket,
  createFrameAssembler,
  findAnnexBStart,
  splitAnnexBNals,
  isKeyFrameBuffer,
  isPreviewKeyFrameBuffer,
}
