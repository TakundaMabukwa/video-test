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
);

ALTER TABLE raw_video_packets
  ADD COLUMN IF NOT EXISTS journal_file TEXT;

ALTER TABLE raw_video_packets
  ADD COLUMN IF NOT EXISTS journal_offset_bytes BIGINT;

ALTER TABLE raw_video_packets
  ADD COLUMN IF NOT EXISTS sequence_number INTEGER;

ALTER TABLE raw_video_packets
  ADD COLUMN IF NOT EXISTS data_type INTEGER;

ALTER TABLE raw_video_packets
  ADD COLUMN IF NOT EXISTS fragment_type INTEGER;

ALTER TABLE raw_video_packets
  ADD COLUMN IF NOT EXISTS file_offset_bytes BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_raw_video_packets_vehicle_time
  ON raw_video_packets(vehicle_id, channel, packet_timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_raw_video_packets_created_at
  ON raw_video_packets(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_video_packets_vehicle_sequence
  ON raw_video_packets(vehicle_id, channel, sequence_number, packet_timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_raw_video_packets_path_offset
  ON raw_video_packets(payload_path, file_offset_bytes);

CREATE INDEX IF NOT EXISTS idx_raw_video_packets_journal_position
  ON raw_video_packets(journal_file, journal_offset_bytes);

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
);

CREATE INDEX IF NOT EXISTS idx_packet_sequence_gaps_vehicle_time
  ON packet_sequence_gaps(vehicle_id, channel, packet_timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS storage_index_state (
  payload_path TEXT PRIMARY KEY,
  last_indexed_offset_bytes BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
