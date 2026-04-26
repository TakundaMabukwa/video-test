const fs = require('fs')
const path = require('path')

const RUNTIME_DIR = path.join(process.cwd(), 'runtime')
const INGEST_STATS_PATH = path.join(RUNTIME_DIR, 'ingest-stats.json')

function ensureDir() {
  fs.mkdirSync(RUNTIME_DIR, { recursive: true })
}

function writeIngestStats(stats) {
  ensureDir()
  const payload = {
    updatedAt: new Date().toISOString(),
    stats,
  }
  fs.writeFileSync(INGEST_STATS_PATH, JSON.stringify(payload, null, 2))
}

function readIngestStats() {
  if (!fs.existsSync(INGEST_STATS_PATH)) {
    return null
  }

  try {
    return JSON.parse(fs.readFileSync(INGEST_STATS_PATH, 'utf8'))
  } catch {
    return null
  }
}

module.exports = {
  INGEST_STATS_PATH,
  writeIngestStats,
  readIngestStats,
}
