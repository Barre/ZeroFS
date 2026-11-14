const { spawnSync } = require('child_process');
const path = require('path');

const env = {
  ...process.env,
  MOUNT_PATH: process.env.STATE_mount_path ?? '',
  ZEROFS_STATE_DIR: process.env.STATE_state_dir ?? '',
};

const result = spawnSync('bash', [path.join(__dirname, '..', 'cleanup-zerofs.sh')], {
  env,
  stdio: 'inherit',
});
if (result.error) console.error(result.error);
process.exit(result.status ?? 1);
