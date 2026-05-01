const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')
const config = require('./config')
const { createFrameAssembler } = require('./jt1078')
const {
  getStreamDir,
  getPlaylistPath,
  clearStreamFiles,
  writeLiveHlsStatus,
  hasRecentLiveHlsRequest,
} = require('./live-hls-state')

function buildFfmpegArgs(vehicleId, channel) {
  const streamDir = getStreamDir(vehicleId, channel)
  const playlistPath = getPlaylistPath(vehicleId, channel)
  const segmentPattern = path.join(streamDir, 'seg_%06d.ts')

  return [
    '-hide_banner',
    '-loglevel',
    'error',
    '-y',
    '-fflags',
    '+genpts',
    '-use_wallclock_as_timestamps',
    '1',
    '-f',
    'h264',
    '-i',
    'pipe:0',
    '-an',
    '-c:v',
    'copy',
    '-f',
    'hls',
    '-hls_time',
    String(config.liveHlsSegmentTimeSec),
    '-hls_list_size',
    String(config.liveHlsListSize),
    '-hls_delete_threshold',
    String(config.liveHlsDeleteThreshold),
    '-hls_flags',
    'delete_segments+append_list+omit_endlist+independent_segments+program_date_time',
    '-hls_segment_type',
    'mpegts',
    '-hls_segment_filename',
    segmentPattern,
    playlistPath,
  ]
}

function createLiveHlsManager({ source = 'preview' } = {}) {
  if (!config.liveHlsEnabled) {
    return {
      handlePacket() {},
      async close() {},
    }
  }

  const streamStates = new Map()

  const idleSweepTimer = setInterval(() => {
    for (const [key, state] of streamStates.entries()) {
      if (
        hasRecentLiveHlsRequest(
          state.vehicleId,
          state.channel,
          config.liveHlsRequestTtlMs,
        )
      ) {
        continue
      }
      closeState(key, state)
    }
  }, 5000)
  idleSweepTimer.unref()

  function closeState(key, state) {
    streamStates.delete(key)

    try {
      if (state.ffmpeg?.stdin && !state.ffmpeg.stdin.destroyed) {
        state.ffmpeg.stdin.end()
      }
    } catch {}

    if (state.ffmpeg && !state.ffmpeg.killed) {
      try {
        state.ffmpeg.kill('SIGTERM')
      } catch {}

      setTimeout(() => {
        try {
          if (state.ffmpeg && !state.ffmpeg.killed) {
            state.ffmpeg.kill('SIGKILL')
          }
        } catch {}
      }, 2000).unref()
    }
  }

  function flushPending(state) {
    if (!state.ffmpeg?.stdin || state.ffmpeg.stdin.destroyed) {
      state.pendingFrames.length = 0
      state.backpressure = false
      return
    }

    while (state.pendingFrames.length > 0) {
      const frame = state.pendingFrames.shift()
      if (!frame) {
        continue
      }

      const canContinue = state.ffmpeg.stdin.write(frame.buffer)
      writeLiveHlsStatus({
        vehicleId: state.vehicleId,
        channel: state.channel,
        updatedAtMs: Date.now(),
        frameTimestampMs: frame.timestamp,
        source,
        sequence: state.sequence,
      })

      if (!canContinue) {
        state.backpressure = true
        return
      }
    }

    state.backpressure = false
  }

  function ensureState(vehicleId, channel) {
    const key = `${vehicleId}:${channel}`
    let state = streamStates.get(key)
    if (state) {
      return state
    }

    clearStreamFiles(vehicleId, channel)
    const ffmpeg = spawn('ffmpeg', buildFfmpegArgs(vehicleId, channel))

    state = {
      key,
      vehicleId,
      channel,
      assembler: createFrameAssembler(),
      ffmpeg,
      pendingFrames: [],
      backpressure: false,
      sequence: 0,
      lastError: null,
    }

    ffmpeg.stdin.on('drain', () => {
      flushPending(state)
    })

    ffmpeg.stdin.on('error', (error) => {
      const detail = error?.message || String(error)
      if (String(error?.code || '') === 'EPIPE') {
        state.lastError = detail
        return
      }

      state.lastError = detail
      console.error(
        `Live HLS stdin error for ${state.vehicleId} ch${state.channel}: ${detail}`,
      )
    })

    let stderr = ''
    ffmpeg.stderr.on('data', (chunk) => {
      stderr += String(chunk || '')
    })

    ffmpeg.on('error', (error) => {
      state.lastError = error.message || String(error)
      console.error(
        `Live HLS ffmpeg error for ${state.vehicleId} ch${state.channel}: ${state.lastError}`,
      )
      closeState(key, state)
    })

    ffmpeg.on('close', (code) => {
      if (code !== 0 && !state.lastError) {
        state.lastError = stderr.trim() || `ffmpeg exited with code ${code}`
        console.error(
          `Live HLS render failed for ${state.vehicleId} ch${state.channel}: ${state.lastError}`,
        )
      }
      streamStates.delete(key)
    })

    streamStates.set(key, state)
    return state
  }

  function handlePacket(meta, payloadBuffer) {
    const vehicleId = String(meta?.vehicleId || '').trim()
    const channel = Number(meta?.channel || 0)
    if (!vehicleId || !Number.isFinite(channel) || channel <= 0 || !Buffer.isBuffer(payloadBuffer)) {
      return
    }

    if (!hasRecentLiveHlsRequest(vehicleId, channel, config.liveHlsRequestTtlMs)) {
      const existing = streamStates.get(`${vehicleId}:${channel}`)
      if (existing) {
        closeState(existing.key, existing)
      }
      return
    }

    const state = ensureState(vehicleId, channel)
    const frames = state.assembler.pushPacket(payloadBuffer)
    if (!frames.length) {
      return
    }

    for (const frame of frames) {
      state.sequence += 1
      state.pendingFrames.push({
        buffer: frame.buffer,
        timestamp: frame.timestamp,
      })

      if (!state.backpressure) {
        flushPending(state)
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
  createLiveHlsManager,
}
