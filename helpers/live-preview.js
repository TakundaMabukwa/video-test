const { spawn } = require('child_process')
const config = require('./config')
const { createFrameAssembler } = require('./jt1078')
const { writeLatestPreview } = require('./live-preview-state')

const JPEG_SOI = Buffer.from([0xff, 0xd8])
const JPEG_EOI = Buffer.from([0xff, 0xd9])

function buildVideoFilter() {
  if (!Number.isFinite(config.livePreviewWidth) || config.livePreviewWidth <= 0) {
    return ''
  }
  return `scale=${config.livePreviewWidth}:-1`
}

function extractJpegs(buffer) {
  const images = []
  let cursor = 0
  let current = buffer

  while (current.length) {
    const start = current.indexOf(JPEG_SOI, cursor)
    if (start < 0) {
      return {
        images,
        remainder: Buffer.alloc(0),
      }
    }

    const end = current.indexOf(JPEG_EOI, start + JPEG_SOI.length)
    if (end < 0) {
      return {
        images,
        remainder: current.slice(start),
      }
    }

    images.push(current.slice(start, end + JPEG_EOI.length))
    current = current.slice(end + JPEG_EOI.length)
    cursor = 0
  }

  return {
    images,
    remainder: Buffer.alloc(0),
  }
}

function createLivePreviewManager() {
  if (!config.livePreviewEnabled) {
    return {
      handlePacket() {},
      async close() {},
    }
  }

  const streamStates = new Map()
  const frameIntervalMs = Math.max(1, Math.round(1000 / Math.max(1, config.livePreviewFps)))
  const idleSweepTimer = setInterval(() => {
    const now = Date.now()
    for (const [key, state] of streamStates.entries()) {
      if (now - state.lastPacketAt <= config.livePreviewIdleMs) {
        continue
      }
      closeState(key, state)
    }
  }, 5000)
  idleSweepTimer.unref()

  function closeState(key, state) {
    streamStates.delete(key)
    try {
      state.ffmpeg?.stdin?.end()
    } catch {}
    try {
      state.ffmpeg?.kill('SIGTERM')
    } catch {}
  }

  function ensureState(vehicleId, channel) {
    const key = `${vehicleId}:${channel}`
    let state = streamStates.get(key)
    if (state) {
      return state
    }

    state = {
      key,
      vehicleId,
      channel,
      assembler: createFrameAssembler(),
      ffmpeg: null,
      stdoutBuffer: Buffer.alloc(0),
      lastPacketAt: 0,
      lastFrameSubmittedAt: 0,
      frameSequence: 0,
      lastFrameTimestampMs: null,
      lastError: null,
    }
    streamStates.set(key, state)
    return state
  }

  function ensureFfmpeg(state) {
    if (state.ffmpeg && !state.ffmpeg.killed && state.ffmpeg.exitCode === null) {
      return state.ffmpeg
    }

    const videoFilter = buildVideoFilter()
    const ffmpegArgs = [
      '-loglevel',
      'error',
      '-fflags',
      'nobuffer',
      '-flags',
      'low_delay',
      '-f',
      'h264',
      '-i',
      'pipe:0',
      '-an',
      '-f',
      'image2pipe',
      '-vcodec',
      'mjpeg',
      '-q:v',
      String(config.livePreviewJpegQuality),
      'pipe:1',
    ]

    if (videoFilter) {
      ffmpegArgs.splice(10, 0, '-vf', videoFilter)
    }

    const ffmpeg = spawn('ffmpeg', ffmpegArgs)

    state.ffmpeg = ffmpeg
    state.stdoutBuffer = Buffer.alloc(0)

    ffmpeg.stdout.on('data', (chunk) => {
      state.stdoutBuffer = Buffer.concat([state.stdoutBuffer, chunk])
      const { images, remainder } = extractJpegs(state.stdoutBuffer)
      state.stdoutBuffer = remainder

      for (const jpegBuffer of images) {
        state.frameSequence += 1
        writeLatestPreview({
          vehicleId: state.vehicleId,
          channel: state.channel,
          jpegBuffer,
          frameTimestampMs: state.lastFrameTimestampMs,
          sequence: state.frameSequence,
        })
      }
    })

    ffmpeg.stderr.on('data', (chunk) => {
      state.lastError = chunk.toString('utf8').trim() || state.lastError
    })

    ffmpeg.on('error', (error) => {
      state.lastError = error.message || String(error)
      closeState(state.key, state)
    })

    ffmpeg.on('close', () => {
      if (state.lastError) {
        console.error(
          `Live preview ffmpeg closed for ${state.vehicleId} ch${state.channel}: ${state.lastError}`,
        )
      }
      if (streamStates.get(state.key) === state) {
        streamStates.delete(state.key)
      }
    })

    return ffmpeg
  }

  function handlePacket(meta, payloadBuffer) {
    const vehicleId = String(meta?.vehicleId || '').trim()
    const channel = Number(meta?.channel || 0)
    if (!vehicleId || !Number.isFinite(channel) || channel <= 0 || !Buffer.isBuffer(payloadBuffer)) {
      return
    }

    const state = ensureState(vehicleId, channel)
    state.lastPacketAt = Date.now()

    const frames = state.assembler.pushPacket(payloadBuffer)
    if (!frames.length) {
      return
    }

    const ffmpeg = ensureFfmpeg(state)
    for (const frame of frames) {
      const now = Date.now()
      if (now - state.lastFrameSubmittedAt < frameIntervalMs) {
        continue
      }

      state.lastFrameSubmittedAt = now
      state.lastFrameTimestampMs = Number.isFinite(Number(frame.timestamp))
        ? Number(frame.timestamp)
        : null

      try {
        ffmpeg.stdin.write(frame.buffer)
      } catch (error) {
        state.lastError = error.message || String(error)
        closeState(state.key, state)
        break
      }
    }
  }

  async function close() {
    clearInterval(idleSweepTimer)
    for (const [key, state] of streamStates.entries()) {
      closeState(key, state)
    }
  }

  return {
    handlePacket,
    close,
  }
}

module.exports = {
  createLivePreviewManager,
}
