import { P9Error } from "./ninep/client";

const friendlyMessages: Record<number, string> = {
  1: "Permission denied",
  2: "File or directory not found",
  5: "I/O error",
  9: "Bad file descriptor",
  11: "Resource temporarily unavailable",
  12: "Out of memory",
  13: "Access denied",
  14: "Bad address",
  16: "Device or resource busy",
  17: "Already exists",
  18: "Invalid cross-device link",
  19: "No such device",
  20: "Not a directory",
  21: "Is a directory",
  22: "Invalid argument",
  23: "Too many open files in system",
  24: "Too many open files",
  26: "Text file busy",
  27: "File too large",
  28: "No space left on device",
  30: "Read-only file system",
  31: "Too many links",
  36: "File name too long",
  38: "Operation not supported",
  39: "Directory not empty",
  40: "Too many levels of symbolic links",
  61: "No data available",
  62: "Timer expired",
  75: "Value too large for data type",
  95: "Operation not supported",
  110: "Connection timed out",
  116: "Stale file handle",
  122: "Disk quota exceeded",
};

export function formatError(err: unknown): string {
  if (err instanceof P9Error) {
    return friendlyMessages[err.ecode] ?? `Unknown filesystem error (code ${err.ecode})`;
  }
  if (err instanceof Error) return err.message;
  return String(err);
}
