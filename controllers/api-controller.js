const { summarizeCoverageForRange, summarizeVehicleApproxCoverage } = require('../helpers/storage')
const { readIngestStats, readIngestRelayStats } = require('../helpers/runtime-state')

class ApiController {
  constructor({ exportController, packetController }) {
    this.exportController = exportController
    this.packetController = packetController
  }

  parseTimestampMs(value) {
    const numeric = Number(value)
    if (Number.isFinite(numeric) && String(value).trim() !== '') {
      return numeric
    }
    const ms = new Date(value).getTime()
    return Number.isFinite(ms) ? ms : null
  }

  parseChannels(rawChannels, routeChannel = null) {
    if (routeChannel !== null && routeChannel !== undefined && routeChannel !== '') {
      const channel = Number(routeChannel)
      return Number.isFinite(channel) && channel > 0 ? [channel] : []
    }

    const source = rawChannels ?? [1, 2]
    const values = Array.isArray(source)
      ? source
      : String(source)
          .split(',')
          .map((value) => Number(String(value).trim()))

    return [...new Set(values.filter((value) => Number.isFinite(value) && value > 0))]
  }

  buildExportRequest(req) {
    const vehicleId = String(
      req.params?.vehicleId || req.body?.vehicleId || req.query?.vehicleId || '',
    ).trim()
    const from = req.body?.from || req.query?.from
    const to = req.body?.to || req.query?.to
    const preRollMs = Number(req.body?.preRollMs || req.query?.preRollMs || 5000)
    const channels = this.parseChannels(
      req.body?.channels || req.query?.channels,
      req.params?.channel,
    )

    return {
      vehicleId,
      from,
      to,
      preRollMs,
      channels,
    }
  }

  buildAbsoluteMediaUrl(req, maybeRelativePath) {
    if (!maybeRelativePath) {
      return null
    }
    if (/^https?:\/\//i.test(maybeRelativePath)) {
      return maybeRelativePath
    }

    const protocolHeader = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim()
    const protocol = protocolHeader || req.protocol || 'http'
    const host = req.get('host')
    if (!host) {
      return maybeRelativePath
    }
    return `${protocol}://${host}${maybeRelativePath}`
  }

  health(req, res) {
    res.json({ success: true, status: 'ok' })
  }

  ingestStats = (req, res) => {
    const workerStats = this.packetController?.getStats?.() || readIngestStats()?.stats || null
    const relayStats = readIngestRelayStats()?.stats || null
    return res.status(200).json({
      success: true,
      stats: workerStats,
      relayStats,
    })
  }

  coverage = async (req, res) => {
    try {
      const from = req.query?.from
      const to = req.query?.to
      const vehicleId = String(req.query?.vehicleId || '').trim()
      const fromMs = this.parseTimestampMs(from)
      const toMs = this.parseTimestampMs(to)
      const source = String(req.query?.source || 'file').trim().toLowerCase()

      if (!from || !to || fromMs === null || toMs === null) {
        return res.status(400).json({
          success: false,
          message: 'valid from and to are required',
        })
      }

      let rows = []
      let usedSource = 'file'

      if (source === 'db') {
        return res.status(410).json({
          success: false,
          message: 'DB-backed packet coverage is disabled in file-first mode. Use source=file.',
        })
      }

      rows = await summarizeCoverageForRange({ fromMs, toMs, vehicleId })

      return res.status(200).json({
        success: true,
        from,
        to,
        vehicleId: vehicleId || null,
        source: usedSource,
        count: rows.length,
        rows,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  vehicleAvailability = async (req, res) => {
    try {
      const vehicleId = String(req.params?.vehicleId || '').trim()
      if (!vehicleId) {
        return res.status(400).json({
          success: false,
          message: 'vehicleId is required',
        })
      }

      const hasChannelsFilter = /(?:\?|&)channels=/.test(String(req.originalUrl || ''))
      const channels = hasChannelsFilter
        ? this.parseChannels(req.query?.channels)
        : []
      let rows = summarizeVehicleApproxCoverage(vehicleId)
      if (channels.length) {
        rows = rows.filter((row) => channels.includes(row.channel))
      }

      return res.status(200).json({
        success: true,
        vehicleId,
        channels: hasChannelsFilter ? channels : null,
        count: rows.length,
        rows,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }

  exportVehicleRange = async (req, res) => {
    try {
      const { vehicleId, from, to, preRollMs, channels } = this.buildExportRequest(req)

      if (!vehicleId || !from || !to || !channels.length) {
        return res.status(400).json({
          success: false,
          message: 'vehicleId, from, to and at least one channel are required',
        })
      }

      const result = await this.exportController.exportVehicleRange({
        vehicleId,
        from,
        to,
        preRollMs,
        channels,
      })

      const channelsWithAbsoluteUrls = (result.channels || []).map((channelResult) => ({
        ...channelResult,
        playUrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.playUrl),
        mp4UrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.mp4Url),
        h264UrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.h264Url),
        rawPacketsUrlAbsolute: this.buildAbsoluteMediaUrl(req, channelResult.rawPacketsUrl),
      }))

      return res.status(200).json({
        success: true,
        ...result,
        channels: channelsWithAbsoluteUrls,
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message || String(error),
      })
    }
  }
}

module.exports = {
  ApiController,
}
