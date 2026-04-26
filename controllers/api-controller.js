const { query } = require('../helpers/db')
const { summarizeCoverageForRange } = require('../helpers/storage')
const { readIngestStats } = require('../helpers/runtime-state')

class ApiController {
  constructor({ exportController, packetController }) {
    this.exportController = exportController
    this.packetController = packetController
  }

  parseTimestampMs(value) {
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

  health(req, res) {
    res.json({ success: true, status: 'ok' })
  }

  ingestStats = (req, res) => {
    const runtimeStats = this.packetController?.getStats?.() || readIngestStats()?.stats || null
    return res.status(200).json({
      success: true,
      stats: runtimeStats,
    })
  }

  coverage = async (req, res) => {
    try {
      const from = req.query?.from
      const to = req.query?.to
      const vehicleId = String(req.query?.vehicleId || '').trim()
      const fromMs = this.parseTimestampMs(from)
      const toMs = this.parseTimestampMs(to)
      const source = String(req.query?.source || 'auto').trim().toLowerCase()

      if (!from || !to || fromMs === null || toMs === null) {
        return res.status(400).json({
          success: false,
          message: 'valid from and to are required',
        })
      }

      let rows = []
      let usedSource = source

      if (source === 'file' || source === 'files') {
        rows = await summarizeCoverageForRange({ fromMs, toMs, vehicleId })
        usedSource = 'file'
      } else {
        const params = [fromMs, toMs]
        let vehicleClause = ''
        if (vehicleId) {
          params.push(vehicleId)
          vehicleClause = 'AND vehicle_id = $3'
        }

        const result = await query(
          `
            SELECT
              vehicle_id,
              channel,
              COUNT(*) AS packet_count,
              MIN(packet_timestamp_ms) AS first_packet_timestamp_ms,
              MAX(packet_timestamp_ms) AS last_packet_timestamp_ms,
              TO_TIMESTAMP(MIN(packet_timestamp_ms) / 1000.0) AS first_packet_time,
              TO_TIMESTAMP(MAX(packet_timestamp_ms) / 1000.0) AS last_packet_time
            FROM raw_video_packets
            WHERE packet_timestamp_ms >= $1
              AND packet_timestamp_ms <= $2
              ${vehicleClause}
            GROUP BY vehicle_id, channel
            ORDER BY packet_count DESC, last_packet_timestamp_ms DESC
          `,
          params,
        )

        rows = result.rows
        usedSource = 'db'

        if ((source === 'auto' || !source) && rows.length === 0) {
          rows = await summarizeCoverageForRange({ fromMs, toMs, vehicleId })
          usedSource = 'file'
        }
      }

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

      return res.status(200).json({
        success: true,
        ...result,
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
