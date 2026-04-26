function getArg(name, fallback = null) {
  const prefix = `--${name}=`
  const match = process.argv.find((arg) => arg.startsWith(prefix))
  return match ? match.slice(prefix.length) : fallback
}
const { ExportController } = require('./controllers/export-controller')

async function main() {
  const vehicleId = String(getArg('vehicleId', '')).trim()
  const channel = Number(getArg('channel', '0'))
  const from = getArg('from')
  const to = getArg('to')
  const preRollMs = Number(getArg('preRollMs', '5000'))

  if (!vehicleId || !Number.isFinite(channel) || channel <= 0 || !from || !to) {
    throw new Error(
      'Usage: npm run export:video -- --vehicleId=<id> --channel=<n> --from=<iso> --to=<iso> [--preRollMs=5000]',
    )
  }

  const exportController = new ExportController()
  const result = await exportController.exportVideo({ vehicleId, channel, from, to, preRollMs })
  console.log(JSON.stringify(result, null, 2))
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message || String(error))
    process.exit(1)
  })
}

module.exports = {
  ExportController,
}
