const { spawnSync } = require('child_process');

const result = spawnSync('bash', ['-euo', 'pipefail', '-c', process.env.INPUT_RUN], {
  stdio: 'inherit',
});
if (result.error) console.error(result.error);
process.exit(result.status ?? 1);
