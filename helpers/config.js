const fs = require('fs')
const path = require('path')

function loadEnvFile() {
  const envPath = path.join(process.cwd(), '.env')
  if (!fs.existsSync(envPath)) {
    return
  }

  const content = fs.readFileSync(envPath, 'utf8')
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) {
      continue
    }

    const separatorIndex = line.indexOf('=')
    if (separatorIndex <= 0) {
      continue
    }

    const key = line.slice(0, separatorIndex).trim()
    const value = line.slice(separatorIndex + 1).trim()
    if (process.env[key] === undefined) {
      process.env[key] = value
    }
  }
}

loadEnvFile()

function getString(name, fallback = '') {
  const value = process.env[name]
  if (value === undefined || String(value).trim() === '') {
    return fallback
  }
  return String(value).trim()
}

function getNumber(name, fallback) {
  const parsed = Number(getString(name, String(fallback)))
  return Number.isFinite(parsed) ? parsed : fallback
}

module.exports = {
  relayHost: getString('RELAY_HOST', '209.38.206.44'),
  relayPort: getNumber('RELAY_PORT', 7081),
  apiPort: getNumber('API_PORT', 3201),
  dbHost: getString('DB_HOST', '127.0.0.1'),
  dbPort: getNumber('DB_PORT', 5432),
  dbName: getString('DB_NAME', 'video_storage'),
  dbUser: getString('DB_USER', 'postgres'),
  dbPassword: getString('DB_PASSWORD', ''),
  dbPoolMax: getNumber('DB_POOL_MAX', 10),
  packetBatchSize: 500,
  packetFlushMs: 250,
  statsLogMs: 5000,
  mirrorRawOutput: false,
  storeHexPreview: false,
  rawDebugLogs: false,
}
