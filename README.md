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

## Live Preview Bridge

This sandbox can now turn incoming live JT1078 packets into browser-friendly preview frames without the older HLS stack.

Ingress-side live flow:

1. `video-feed-ingest` receives relay packets in real time.
2. Ingest assembles raw packets into H264 frames immediately as they arrive.
3. A lightweight FFmpeg sidecar converts those frames into JPEG previews.
4. Latest frames are cached under `runtime/live-preview/...`.
5. API serves those frames as:
   - single screenshots
   - multipart MJPEG live preview

Archive/storage flow remains separate:

1. `video-feed-ingest` writes packets to archive storage immediately as they arrive.
2. `video-feed-ingest` also publishes the same packets into JetStream.
3. `video-feed-worker` can consume JetStream for secondary processing without being the primary archive writer.
4. Queue backlog no longer drives the live preview path.

### Endpoints

List active preview streams:

```bash
curl "http://127.0.0.1:3201/api/live/streams"
```

Get latest screenshot:

```bash
curl "http://127.0.0.1:3201/api/vehicles/<vehicleId>/screenshot?channel=1" --output latest.jpg
```

Open MJPEG live preview:

```bash
curl "http://127.0.0.1:3201/api/vehicles/<vehicleId>/live.mjpeg?channel=1"
```

In a browser, the MJPEG endpoint can be used directly in an `<img>` tag:

```html
<img src="http://127.0.0.1:3201/api/vehicles/221085886967/live.mjpeg?channel=1" />
```

### Tuning

```env
LIVE_PREVIEW_ENABLED=true
LIVE_PREVIEW_SOURCE=ingest
LIVE_PREVIEW_FPS=4
LIVE_PREVIEW_WIDTH=960
LIVE_PREVIEW_JPEG_QUALITY=6
LIVE_PREVIEW_IDLE_MS=15000
LIVE_PREVIEW_WAIT_MS=10000
LIVE_PREVIEW_MAX_AGE_MS=15000
```

`LIVE_PREVIEW_SOURCE` options:

- `ingest` (default): live preview follows incoming relay packets directly
- `worker`: live preview follows queued worker processing
- `both`: both processes may write preview frames
- `none`: disable preview generation

This path is meant for reliable live dashboard previews and screenshot capture. It does not replace full archive export.

### Listener websocket ingest mode

You can ingest directly from listener `/ws/raw` instead of only relying on relay TCP:

```env
INGEST_SOURCE_MODE=ws
SOURCE_WS_URL=ws://127.0.0.1:3000/ws/raw
SOURCE_WS_RECONNECT_MS=3000
SOURCE_WS_PING_INTERVAL_MS=30000
SOURCE_WS_PONG_TIMEOUT_MS=10000
SOURCE_WS_MAX_BODY_LENGTH=1048576
```

Modes:

- `relay`: relay TCP only (`RELAY_HOST` / `RELAY_PORT`)
- `ws`: websocket only (`SOURCE_WS_URL`)
- `both`: ingest from both sources in parallel

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
ARCHIVE_WRITE_SOURCE=ingest
RETENTION_DAYS=0
```

Optional sizing/tuning:

```env
NATS_STREAM_MAX_BYTES=0
NATS_STREAM_MAX_AGE_MS=0
NATS_ACK_WAIT_MS=300000
NATS_PUBLISH_TIMEOUT_MS=5000
NATS_CONSUME_BATCH_SIZE=500
NATS_CONSUMER_MAX_ACK_PENDING=20000
```

Recommended for mandatory full archive retention:

- `ARCHIVE_WRITE_SOURCE=ingest`
- `RETENTION_DAYS=0`
- `NATS_STREAM_MAX_BYTES=0`
- `NATS_STREAM_MAX_AGE_MS=0`

With those settings, archive packet files are written immediately on ingest and local retention cleanup is disabled.

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

## Ensure Alert MP4 + Links

Use this when you need unresolved alerts to have evidence collection triggered and video-link status tracked.

```bash
npm run alerts:ensure-media
```

Optional flags:

```bash
node scripts/ensure-alert-media.js \
  --base http://46.101.219.78:3100 \
  --limit 100 \
  --concurrency 4 \
  --pollRounds 2 \
  --pollDelayMs 15000 \
  --requestReport true
```

What it does per unresolved alert:

1. Triggers `POST /api/alerts/:id/collect-evidence`
2. Polls `GET /api/alerts/:id/videos?ensureMedia=true`
3. If no stored video links yet, triggers `POST /api/alerts/:id/request-report-video`
4. Writes a JSON run report under `runtime/alert-captures/ensure-alert-media-*.json`
