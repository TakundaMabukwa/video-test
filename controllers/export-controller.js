const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')
const { readPacketsForRange } = require('../helpers/storage')
const { createFrameAssembler } = require('../helpers/jt1078')

const EXPORT_ROOT = path.join(process.cwd(), 'exports')

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function safeStamp(value) {
  return new Date(value).toISOString().replace(/[:.]/g, '-')
}

async function tryTranscodeToMp4(inputPath, outputPath) {
  return tryTranscodeToMp4WithRate(inputPath, outputPath, 12)
}

async function tryTranscodeToMp4WithRate(inputPath, outputPath, frameRate) {
  const sourceFrameRate = 25
  const setPtsMultiplier = sourceFrameRate / frameRate
  return new Promise((resolve, reject) => {
    const proc = spawn('ffmpeg', [
      '-hide_banner',
      '-loglevel', 'error',
      '-y',
      '-fflags', '+genpts',
      '-f', 'h264',
      '-i', inputPath,
      '-vf', `setpts=${setPtsMultiplier.toFixed(6)}*PTS`,
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-pix_fmt', 'yuv420p',
      '-r', String(frameRate),
      '-movflags', '+faststart',
      outputPath,
    ])

    let stderr = ''
    proc.stderr.on('data', (chunk) => {
      stderr += String(chunk || '')
    })
    proc.on('error', reject)
    proc.on('close', (code) => {
      if (code === 0 && fs.existsSync(outputPath) && fs.statSync(outputPath).size > 0) {
        resolve(outputPath)
        return
      }
      reject(new Error(stderr || `ffmpeg exited with code ${code}`))
    })
  })
}

function estimateFrameRate(frameTimestamps) {
  if (!frameTimestamps || frameTimestamps.length < 2) {
    return 12
  }

  const deltas = []
  for (let i = 1; i < frameTimestamps.length; i += 1) {
    const delta = frameTimestamps[i] - frameTimestamps[i - 1]
    if (delta > 0) {
      deltas.push(delta)
    }
  }

  if (!deltas.length) {
    return 12
  }

  deltas.sort((a, b) => a - b)
  const median = deltas[Math.floor(deltas.length / 2)]
  if (!median || median <= 0) {
    return 12
  }

  const estimated = Math.round(1000 / median)
  return Math.min(25, Math.max(1, estimated))
}

class ExportController {
  async exportVideo({ vehicleId, channel, from, to, preRollMs = 5000 }) {
    const requestedFrom = new Date(from)
    const requestedTo = new Date(to)
    if (Number.isNaN(requestedFrom.getTime()) || Number.isNaN(requestedTo.getTime())) {
      throw new Error('Invalid from/to timestamp')
    }
    if (requestedTo <= requestedFrom) {
      throw new Error('to must be greater than from')
    }

    const fromMs = requestedFrom.getTime() - preRollMs
    const toMs = requestedTo.getTime()
    const exportDir = path.join(EXPORT_ROOT, vehicleId, `ch${channel}`)
    ensureDir(exportDir)

    const stamp = `${safeStamp(fromMs)}_${safeStamp(toMs)}`
    const rawPacketsPath = path.join(exportDir, `${stamp}.packets.bin`)
    const h264Path = path.join(exportDir, `${stamp}.h264`)
    const mp4Path = path.join(exportDir, `${stamp}.mp4`)

    const rawOut = fs.createWriteStream(rawPacketsPath)
    const h264Out = fs.createWriteStream(h264Path)
    const assembler = createFrameAssembler()

    let packetCount = 0
    let byteCount = 0
    let frameCount = 0
    const frameTimestamps = []

    const handlePacket = async ({ packet }) => {
      packetCount += 1
      byteCount += packet.length
      rawOut.write(packet)

      const frames = assembler.pushPacket(packet)
      for (const frame of frames) {
        h264Out.write(frame.buffer)
        frameCount += 1
        if (Number.isFinite(frame.timestamp)) {
          frameTimestamps.push(frame.timestamp)
        }
      }
    }

    await readPacketsForRange({
      vehicleId,
      channel,
      fromMs,
      toMs,
      onPacket: handlePacket,
    })

    await new Promise((resolve) => rawOut.end(resolve))
    await new Promise((resolve) => h264Out.end(resolve))

    if (!fs.existsSync(h264Path) || fs.statSync(h264Path).size === 0) {
      throw new Error('No H264 payload found for that vehicle/timeframe')
    }

    let mp4Created = false
    let transcodeError = null
    const frameRate = estimateFrameRate(frameTimestamps)
    try {
      await tryTranscodeToMp4WithRate(h264Path, mp4Path, frameRate)
      mp4Created = true
    } catch (error) {
      transcodeError = error.message || String(error)
    }

    return {
      vehicleId,
      channel,
      requestedFrom: requestedFrom.toISOString(),
      requestedTo: requestedTo.toISOString(),
      packetCount,
      byteCount,
      frameCount,
      frameRate,
      rawPacketsPath,
      h264Path,
      mp4Path: mp4Created ? mp4Path : null,
      transcodeError,
    }
  }

  toMediaLink(filePath) {
    if (!filePath) {
      return null
    }
    const relativePath = path.relative(EXPORT_ROOT, filePath).split(path.sep).join('/')
    return `/media/exports/${relativePath}`
  }

  async exportVehicleRange({ vehicleId, from, to, preRollMs = 5000, channels = [1, 2] }) {
    const uniqueChannels = [...new Set(channels.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))]
    const results = []

    for (const channel of uniqueChannels) {
      try {
        const exportResult = await this.exportVideo({
          vehicleId,
          channel,
          from,
          to,
          preRollMs,
        })
        results.push({
          channel,
          success: true,
          packetCount: exportResult.packetCount,
          byteCount: exportResult.byteCount,
          playUrl: this.toMediaLink(exportResult.mp4Path || exportResult.h264Path),
          mp4Url: this.toMediaLink(exportResult.mp4Path),
          h264Url: this.toMediaLink(exportResult.h264Path),
          rawPacketsUrl: this.toMediaLink(exportResult.rawPacketsPath),
          transcodeError: exportResult.transcodeError,
        })
      } catch (error) {
        results.push({
          channel,
          success: false,
          message: error.message || String(error),
          playUrl: null,
          mp4Url: null,
          h264Url: null,
          rawPacketsUrl: null,
        })
      }
    }

    return {
      vehicleId,
      from,
      to,
      preRollMs,
      channels: results,
    }
  }
}

module.exports = {
  ExportController,
  EXPORT_ROOT,
}
