const { Pool } = require('pg')
const config = require('./config')

const pool = new Pool({
  host: config.dbHost,
  port: config.dbPort,
  database: config.dbName,
  user: config.dbUser,
  password: config.dbPassword,
  max: config.dbPoolMax,
})
let poolClosed = false

async function query(text, params = []) {
  if (poolClosed) {
    throw new Error('Database pool is closed')
  }
  return pool.query(text, params)
}

async function ensureSchema() {
  const existing = await pool.query(`
    SELECT
      to_regclass('public.raw_video_packets') AS raw_video_packets,
      to_regclass('public.packet_sequence_gaps') AS packet_sequence_gaps,
      to_regclass('public.storage_index_state') AS storage_index_state
  `)

  const row = existing.rows[0] || {}
  if (!row.raw_video_packets) {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS raw_video_packets (
        id BIGSERIAL PRIMARY KEY,
        relay_type TEXT NOT NULL DEFAULT 'tcp-rtp',
        vehicle_id TEXT NOT NULL,
        channel INTEGER NOT NULL,
        packet_timestamp_ms BIGINT NOT NULL,
        journal_file TEXT,
        journal_offset_bytes BIGINT,
        sequence_number INTEGER,
        data_type INTEGER,
        fragment_type INTEGER,
        packet_size INTEGER NOT NULL,
        file_offset_bytes BIGINT NOT NULL DEFAULT 0,
        payload_hex_preview TEXT,
        payload_path TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_raw_video_packets_vehicle_time ON raw_video_packets(vehicle_id, channel, packet_timestamp_ms)`)
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_raw_video_packets_created_at ON raw_video_packets(created_at DESC)`)
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_raw_video_packets_vehicle_sequence ON raw_video_packets(vehicle_id, channel, sequence_number, packet_timestamp_ms)`)
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_raw_video_packets_path_offset ON raw_video_packets(payload_path, file_offset_bytes)`)
  }
  if (!row.packet_sequence_gaps) {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS packet_sequence_gaps (
        id BIGSERIAL PRIMARY KEY,
        vehicle_id TEXT NOT NULL,
        channel INTEGER NOT NULL,
        previous_sequence_number INTEGER,
        expected_sequence_number INTEGER NOT NULL,
        actual_sequence_number INTEGER NOT NULL,
        missing_packet_count INTEGER NOT NULL,
        packet_timestamp_ms BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_packet_sequence_gaps_vehicle_time ON packet_sequence_gaps(vehicle_id, channel, packet_timestamp_ms DESC)`)
  }

  if (!row.storage_index_state) {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS storage_index_state (
        payload_path TEXT PRIMARY KEY,
        last_indexed_offset_bytes BIGINT NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
  }
}

async function closePool() {
  if (poolClosed) {
    return
  }
  poolClosed = true
  await pool.end()
}

module.exports = {
  query,
  ensureSchema,
  closePool,
}
