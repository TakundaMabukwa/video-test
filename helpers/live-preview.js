const { spawn } = require('child_process')
const config = require('./config')
const { createFrameAssembler, isPreviewKeyFrameBuffer } = require('./jt1078')
const { writeLatestPreview } = require('./live-preview-state')

function buildVideoFilter() {
  if (!Number.isFinite(config.livePreviewWidth) || config.livePreviewWidth <= 0) {
    return ''
  }
  return `scale=${config.livePreviewWidth}:-1`
}

function buildFfmpegArgs() {
  const args = [
    '-loglevel',
    'error',
    '-f',
    'h264',
    '-i',
    'pipe:0',
  ]

  const videoFilter = buildVideoFilter()
  if (videoFilter) {
    args.push('-vf', videoFilter)
  }

  args.push(
    '-frames:v',
    '1',
    '-an',
    '-f',
    'image2pipe',
    '-vcodec',
    'mjpeg',
    '-q:v',
    String(config.livePreviewJpegQuality),
    'pipe:1',
  )

  return args
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
      lastPacketAt: 0,
      lastFrameSubmittedAt: 0,
      frameSequence: 0,
      lastFrameTimestampMs: null,
      lastError: null,
      renderInFlight: false,
    }
    streamStates.set(key, state)
    return state
  }

  function renderPreviewFrame(state, frameBuffer, frameTimestampMs) {
    if (state.renderInFlight) {
      return
    }

    state.renderInFlight = true
    state.lastFrameTimestampMs = Number.isFinite(Number(frameTimestampMs))
      ? Number(frameTimestampMs)
      : null

    const ffmpeg = spawn('ffmpeg', buildFfmpegArgs())
    const stdoutChunks = []
    const stderrChunks = []

    ffmpeg.stdout.on('data', (chunk) => {
      stdoutChunks.push(chunk)
    })

    ffmpeg.stdin.on('error', (error) => {
      const detail = error?.message || String(error)
      if (String(error?.code || '') === 'EPIPE') {
        state.lastError = detail
        return
      }

      state.lastError = detail
      console.error(
        `Live preview stdin error for ${state.vehicleId} ch${state.channel}: ${detail}`,
      )
    })

    ffmpeg.stderr.on('data', (chunk) => {
      stderrChunks.push(chunk)
    })

    ffmpeg.on('error', (error) => {
      state.renderInFlight = false
      state.lastError = error.message || String(error)
      console.error(
        `Live preview render error for ${state.vehicleId} ch${state.channel}: ${state.lastError}`,
      )
    })

    ffmpeg.on('close', (code) => {
      state.renderInFlight = false
      const stderr = Buffer.concat(stderrChunks).toString('utf8').trim()
      if (stderr) {
        state.lastError = stderr
      }

      if (code !== 0) {
        console.error(
          `Live preview render failed for ${state.vehicleId} ch${state.channel}: ${stderr || `ffmpeg exited with code ${code}`}`,
        )
        return
      }

      const jpegBuffer = Buffer.concat(stdoutChunks)
      if (!jpegBuffer.length) {
        return
      }

      state.frameSequence += 1
      writeLatestPreview({
        vehicleId: state.vehicleId,
        channel: state.channel,
        jpegBuffer,
        frameTimestampMs: state.lastFrameTimestampMs,
        sequence: state.frameSequence,
      })
    })

    try {
      ffmpeg.stdin.end(frameBuffer)
    } catch (error) {
      state.renderInFlight = false
      state.lastError = error.message || String(error)
      console.error(
        `Live preview stdin close failed for ${state.vehicleId} ch${state.channel}: ${state.lastError}`,
      )
      try {
        ffmpeg.kill('SIGTERM')
      } catch {}
    }
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

    for (const frame of frames) {
      const now = Date.now()
      if (now - state.lastFrameSubmittedAt < frameIntervalMs) {
        continue
      }
      if (!isPreviewKeyFrameBuffer(frame.buffer)) {
        continue
      }

      state.lastFrameSubmittedAt = now
      renderPreviewFrame(state, frame.buffer, frame.timestamp)
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
