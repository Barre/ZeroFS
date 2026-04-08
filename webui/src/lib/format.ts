export function joinPath(dir: string, name: string): string {
  return dir === "/" ? `/${name}` : `${dir}/${name}`;
}

export function formatSize(bytes: bigint | number): string {
  const n = typeof bytes === "bigint" ? Number(bytes) : bytes;
  if (n === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
  const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
  const val = n / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function formatSizeFixed(bytes: number): string {
  const units = [" B", "KB", "MB", "GB", "TB"];
  if (bytes === 0) return `${"0.0".padStart(5)} ${units[1]}`;
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(1).padStart(5)} ${units[Math.max(i, 1)]}`;
}

export function formatOps(ops: number): string {
  if (ops < 1000) return `${ops.toString().padStart(3)}  `;
  if (ops < 10000) return `${(ops / 1000).toFixed(1)} K`;
  return `${Math.round(ops / 1000)
    .toString()
    .padStart(3)} K`;
}

export function formatMode(mode: number): string {
  const types: Record<number, string> = {
    0o140000: "s", // socket
    0o120000: "l", // symlink
    0o100000: "-", // regular
    0o060000: "b", // block
    0o040000: "d", // directory
    0o020000: "c", // char
    0o010000: "p", // fifo
  };
  const typeChar = types[mode & 0o170000] ?? "?";
  const perms = [
    mode & 0o400 ? "r" : "-",
    mode & 0o200 ? "w" : "-",
    mode & 0o100 ? "x" : "-",
    mode & 0o040 ? "r" : "-",
    mode & 0o020 ? "w" : "-",
    mode & 0o010 ? "x" : "-",
    mode & 0o004 ? "r" : "-",
    mode & 0o002 ? "w" : "-",
    mode & 0o001 ? "x" : "-",
  ].join("");
  return typeChar + perms;
}

export function formatTimestamp(sec: bigint, nsec: bigint): string {
  const ms = Number(sec) * 1000 + Number(nsec) / 1_000_000;
  return new Date(ms).toLocaleString();
}
