#!/usr/bin/env bash
# Generate one sitemap after the Zola marketing site and Next.js docs are assembled.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <assembled-site-directory>" >&2
  exit 2
fi

site_dir="$(cd "$1" && pwd)"
site_url="https://www.zerofs.net"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

{
  printf '%s\n' '<?xml version="1.0" encoding="UTF-8"?>'
  printf '%s\n' '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'

  while IFS= read -r file; do
    relative="${file#"$site_dir"/}"

    case "$relative" in
      404.html|*/404.html|social/*|_next/*|docs/_next/*)
        continue
        ;;
      index.html)
        path="/"
        ;;
      */index.html)
        path="/${relative%index.html}"
        ;;
      *.html)
        path="/${relative%.html}"
        ;;
      *)
        continue
        ;;
    esac

    printf '  <url><loc>%s%s</loc></url>\n' "$site_url" "$path"
  done < <(find "$site_dir" -type f -name '*.html' -print | LC_ALL=C sort)

  printf '%s\n' '</urlset>'
} > "$tmp"

mv "$tmp" "$site_dir/sitemap.xml"
trap - EXIT
