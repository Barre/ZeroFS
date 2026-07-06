#!/usr/bin/env bash
# Assemble the Cloudflare Pages deploy directory.
#
# The docs are a Next.js static export with basePath /docs, so their built
# assets already reference /docs/*. Dropping the export into website/docs lets a
# single Pages project serve the marketing site and the docs from one origin,
# with no reverse proxy. Deploy directory = website/.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==> Building documentation (next static export)"
( cd documentation && npm ci && npm run build )   # output: 'export' -> documentation/out

echo "==> Assembling website/docs"
rm -rf website/docs
mkdir -p website/docs
cp -R documentation/out/. website/docs/

echo "==> Done. Deploy directory: website/"
