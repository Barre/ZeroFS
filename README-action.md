# ZeroFS GitHub Action

This GitHub Action provides persistent volumes backed by S3-compatible storage for your workflows.

## Usage

```yaml
- uses: Barre/ZeroFS@main
  with:
    object-store-url: 's3://bucket/path'
    encryption-password: ${{ secrets.ZEROFS_PASSWORD }}
    aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
    aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

## Inputs

| Name | Description | Required | Default |
|------|-------------|----------|---------|
| `object-store-url` | Object store URL (e.g. s3://bucket/path) | Yes | - |
| `encryption-password` | Password for filesystem encryption | Yes | - |
| `mount-path` | Path where the volume should be mounted | No | `/mnt/zerofs` |
| `cache-dir` | Directory for SlateDB disk cache | No | `/tmp/zerofs-cache` |
| `cache-size-gb` | SlateDB disk cache size in GB | No | `1` |
| `memory-cache-size-gb` | ZeroFS in-memory cache size in GB | No | `0.25` |
| `aws-access-key-id` | AWS access key ID (for S3) | No | - |
| `aws-secret-access-key` | AWS secret access key (for S3) | No | - |
| `aws-region` | AWS region | No | `us-east-1` |
| `aws-endpoint` | S3-compatible endpoint URL | No | - |
| `aws-allow-http` | Allow HTTP connections (for MinIO, etc.) | No | `false` |
| `azure-storage-account-name` | Azure storage account name | No | - |
| `azure-storage-account-key` | Azure storage account key | No | - |
| `nfs-host` | NFS server host address | No | `127.0.0.1` |
| `nfs-port` | NFS server port | No | `2049` |
| `zerofs-version` | ZeroFS version to use | No | `latest` |

## Outputs

| Name | Description |
|------|-------------|
| `mount-path` | Path where the volume is mounted |
| `nfs-endpoint` | NFS endpoint (host:port) |

## Examples

See the [examples](examples/) directory for complete workflow examples.
