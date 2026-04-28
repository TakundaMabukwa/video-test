module.exports = {
  apps: [
    {
      name: 'video-feed-api',
      script: 'api-server.js',
      cwd: __dirname,
    },
    {
      name: 'video-feed-ingest',
      script: 'ingest-server.js',
      cwd: __dirname,
    },
    {
      name: 'video-feed-worker',
      script: 'queue-worker.js',
      cwd: __dirname,
    },
  ],
}
