# video-test

## Video Pull API

### 1) Check available time coverage for a vehicle

```bash
curl "http://127.0.0.1:3201/api/vehicles/<vehicleId>/video/availability"
curl "http://127.0.0.1:3201/api/vehicles/<vehicleId>/video/availability?channels=1"
```

Response includes approximate per-channel coverage from stored packet files.

### 2) Export playable video for vehicle + time range

```bash
curl "http://127.0.0.1:3201/api/vehicles/<vehicleId>/video?from=2026-04-26T19:49:00.000Z&to=2026-04-26T19:55:00.000Z&channels=1,2"
```

You can also pass Unix epoch milliseconds:

```bash
curl "http://127.0.0.1:3201/api/vehicles/<vehicleId>/video?from=1777232940000&to=1777233300000&channels=1"
```

Successful channel results include:

- `playUrl` / `mp4Url` / `h264Url` / `rawPacketsUrl`
- `playUrlAbsolute` / `mp4UrlAbsolute` / `h264UrlAbsolute` / `rawPacketsUrlAbsolute`

If no packets are found in the requested range, the channel result includes a clear message with approximate available range.

## Durable Packet Buffer (JetStream)

This project now uses a durable middle layer:

1. `video-feed-ingest` reads relay traffic and enqueues packets into NATS JetStream.
2. `video-feed-worker` dequeues and writes packets to storage/indexes.
3. If worker is stopped, packets remain durably buffered in JetStream until worker resumes.

### Required runtime services

- NATS with JetStream enabled.

Example local container:

```bash
docker run --name nats-js -p 4222:4222 -p 8222:8222 -d nats:2 -js
```

### Environment variables

```env
NATS_URL=nats://127.0.0.1:4222
NATS_STREAM_NAME=VIDEO_PACKET_STREAM
NATS_SUBJECT=video.packet
NATS_CONSUMER_NAME=video-packet-writer
QUEUE_WORKER_ENABLED=true
```

Optional sizing/tuning:

```env
NATS_STREAM_MAX_BYTES=53687091200
NATS_STREAM_MAX_AGE_MS=259200000
NATS_PUBLISH_TIMEOUT_MS=5000
NATS_CONSUME_BATCH_SIZE=500
NATS_CONSUMER_MAX_ACK_PENDING=20000
```

### Process startup

```bash
pnpm start:api
pnpm start:ingest
pnpm start:worker
```

Or with PM2:

```bash
pm2 start ecosystem.config.js
```

### Monitoring

Check combined worker + relay queue stats:

```bash
curl "http://127.0.0.1:3201/api/ingest/stats"
```

Key fields:

- `stats`: worker-side packet write stats.
- `relayStats.enqueuedPackets`: packets durably published to JetStream.
- `relayStats.queueDepthMessages`: buffered packets waiting for worker drain.
