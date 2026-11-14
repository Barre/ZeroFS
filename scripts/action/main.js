// Inputs arrive as INPUT_<NAME> with dashes kept, which bash cannot expand.
const { spawnSync } = require('child_process');
const path = require('path');

const input = (name) => process.env[`INPUT_${name.toUpperCase()}`] ?? '';

const env = {
  ...process.env,
  ZEROFS_VERSION: input('zerofs-version'),
  OBJECT_STORE_URL: input('object-store-url'),
  MOUNT_PATH: input('mount-path'),
  SLATEDB_CACHE_DIR: input('cache-dir'),
  SLATEDB_CACHE_SIZE_GB: input('cache-size-gb'),
  ZEROFS_MEMORY_CACHE_SIZE_GB: input('memory-cache-size-gb'),
  ZEROFS_ENCRYPTION_PASSWORD: input('encryption-password'),
  AWS_ACCESS_KEY_ID: input('aws-access-key-id'),
  AWS_SECRET_ACCESS_KEY: input('aws-secret-access-key'),
  AWS_DEFAULT_REGION: input('aws-region'),
  AWS_ENDPOINT: input('aws-endpoint'),
  AWS_ALLOW_HTTP: input('aws-allow-http'),
  AZURE_STORAGE_ACCOUNT_NAME: input('azure-storage-account-name'),
  AZURE_STORAGE_ACCOUNT_KEY: input('azure-storage-account-key'),
  ZEROFS_NFS_HOST: input('nfs-host'),
  ZEROFS_NFS_HOST_PORT: input('nfs-port'),
};

const result = spawnSync('bash', [path.join(__dirname, '..', 'setup-zerofs.sh')], {
  env,
  stdio: 'inherit',
});
if (result.error) console.error(result.error);
process.exit(result.status ?? 1);
