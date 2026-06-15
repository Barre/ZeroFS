// JSON-safe package.json stamper for the assemble.sh CI flow. Using Node (which
// the npm publish job already has) sidesteps the escaping hazards of building
// JSON with sed/awk: every value is injected via the object model and the
// result is re-serialized, so it is always valid JSON.
//
// Usage:
//   node stamp.mjs main    <tmpl> <out> <version>
//   node stamp.mjs platform <tmpl> <out> <version> <pkgname> <target> <libfile> <os> <cpu> [libc]
import { readFileSync, writeFileSync } from "node:fs";

const [mode, tmplPath, outPath, version, ...rest] = process.argv.slice(2);
if (!mode || !tmplPath || !outPath || !version) {
  console.error("stamp.mjs: usage: <main|platform> <tmpl> <out> <version> [...]");
  process.exit(2);
}

// Templates are valid JSON except for the placeholder tokens, which only ever
// appear inside string values, save __OS__/__CPU__/__LIBC_FIELD__ in the
// platform template, which we strip before parsing and re-add structurally.
let text = readFileSync(tmplPath, "utf8");

if (mode === "main") {
  // Only __VERSION__ to substitute; it sits in string values, so a plain
  // global replace keeps the JSON well-formed. Parse to validate, re-emit.
  text = text.replaceAll("__VERSION__", version);
  const pkg = JSON.parse(text);
  writeFileSync(outPath, JSON.stringify(pkg, null, 2) + "\n");
} else if (mode === "platform") {
  const [pkgname, target, libfile, os, cpu, libc] = rest;
  if (!pkgname || !target || !libfile || !os || !cpu) {
    console.error("stamp.mjs platform: need pkgname target libfile os cpu [libc]");
    process.exit(2);
  }
  // Drop the structural placeholders, parse the rest, then set them as real
  // arrays so quoting/commas are never our problem.
  text = text
    .replaceAll("__VERSION__", version)
    .replaceAll("__PKGNAME__", pkgname)
    .replaceAll("__TARGET__", target)
    .replaceAll("__LIBFILE__", libfile)
    .replace(/\[__OS__\]/g, "[]")
    .replace(/\[__CPU__\]/g, "[]");
  const pkg = JSON.parse(text);
  pkg.os = [os];
  pkg.cpu = [cpu];
  if (libc && libc !== "-") pkg.libc = [libc];
  writeFileSync(outPath, JSON.stringify(pkg, null, 2) + "\n");
} else {
  console.error(`stamp.mjs: unknown mode '${mode}'`);
  process.exit(2);
}
