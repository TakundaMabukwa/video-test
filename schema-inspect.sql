-- Switch to your database first:
-- \c video_storage

-- See the table definition
\d raw_video_packets

-- Latest stored packets
SELECT
  id,
  vehicle_id,
  channel,
  relay_type,
  packet_timestamp_ms,
  to_timestamp(packet_timestamp_ms / 1000.0) AS packet_time,
  packet_size,
  payload_hex_preview,
  payload_path,
  created_at
FROM raw_video_packets
ORDER BY id DESC
LIMIT 20;

-- Vehicles currently present
SELECT
  vehicle_id,
  channel,
  COUNT(*) AS packet_count,
  MIN(to_timestamp(packet_timestamp_ms / 1000.0)) AS first_packet_time,
  MAX(to_timestamp(packet_timestamp_ms / 1000.0)) AS last_packet_time
FROM raw_video_packets
GROUP BY vehicle_id, channel
ORDER BY last_packet_time DESC;

-- Example timeframe lookup
-- Replace vehicle id / channel / times as needed
SELECT
  id,
  vehicle_id,
  channel,
  to_timestamp(packet_timestamp_ms / 1000.0) AS packet_time,
  packet_size,
  payload_path
FROM raw_video_packets
WHERE vehicle_id = '221085864139'
  AND channel = 2
  AND packet_timestamp_ms BETWEEN 1777118082000 AND 1777118085000
ORDER BY packet_timestamp_ms ASC;
