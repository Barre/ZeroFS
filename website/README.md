# ZeroFS marketing site

The landing page and blog are generated with [Zola](https://www.getzola.org/).
The documentation remains a separate Next.js static export under
`documentation/` and is mounted at `/docs` during the combined build.

Use Zola 0.22.1 for site development:

```sh
zola --root website serve
```

Run the same combined build used by GitHub Actions with:

```sh
./scripts/build-site.sh
```

The deployable output is written to `dist/site/`. Blog posts live in
`website/content/blog/`; shared static files live in `website/static/`.
