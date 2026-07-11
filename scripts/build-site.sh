#!/usr/bin/env bash
# Build and assemble the Cloudflare Pages deploy directory.
#
# Zola owns the marketing site and blog. The docs are a Next.js static export
# with basePath /docs, so their built assets already reference /docs/*. Both
# outputs are combined under dist/site for one static Pages deployment.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

site_dir="$repo_root/dist/site"
rm -rf "$site_dir"
mkdir -p "$(dirname "$site_dir")"

echo "==> Building marketing site (Zola)"
zola --root website build --output-dir "$site_dir"

echo "==> Building documentation (next static export)"
( cd documentation && npm ci && npm run build )   # output: 'export' -> documentation/out

echo "==> Assembling documentation under /docs"
mkdir -p "$site_dir/docs"
cp -R documentation/out/. "$site_dir/docs/"

echo "==> Generating unified sitemap"
./scripts/generate-sitemap.sh "$site_dir"

echo "==> Done. Deploy directory: dist/site/"
