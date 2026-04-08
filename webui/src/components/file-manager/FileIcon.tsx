import { Folder, File, FileText, FileImage, FileVideo, FileAudio, FileCode, FileArchive, Link } from "lucide-react";

const EXT_MAP: Record<string, typeof File> = {
  txt: FileText,
  md: FileText,
  log: FileText,
  csv: FileText,
  json: FileText,
  yaml: FileText,
  yml: FileText,
  toml: FileText,
  xml: FileText,
  png: FileImage,
  jpg: FileImage,
  jpeg: FileImage,
  gif: FileImage,
  svg: FileImage,
  webp: FileImage,
  bmp: FileImage,
  ico: FileImage,
  mp4: FileVideo,
  mkv: FileVideo,
  avi: FileVideo,
  mov: FileVideo,
  webm: FileVideo,
  mp3: FileAudio,
  wav: FileAudio,
  ogg: FileAudio,
  flac: FileAudio,
  aac: FileAudio,
  ts: FileCode,
  tsx: FileCode,
  js: FileCode,
  jsx: FileCode,
  py: FileCode,
  rs: FileCode,
  go: FileCode,
  c: FileCode,
  h: FileCode,
  cpp: FileCode,
  java: FileCode,
  rb: FileCode,
  sh: FileCode,
  css: FileCode,
  html: FileCode,
  zip: FileArchive,
  tar: FileArchive,
  gz: FileArchive,
  bz2: FileArchive,
  xz: FileArchive,
  "7z": FileArchive,
  rar: FileArchive,
};

interface FileIconProps {
  name: string;
  isDir: boolean;
  isSymlink: boolean;
  size?: number;
}

export function FileIcon({ name, isDir, isSymlink, size = 16 }: FileIconProps) {
  if (isSymlink) return <Link size={size} strokeWidth={1.5} className="text-violet-400 shrink-0" />;
  if (isDir) return <Folder size={size} strokeWidth={1.5} className="text-blue-400 shrink-0" />;

  const ext = name.split(".").pop()?.toLowerCase() ?? "";
  const Icon = EXT_MAP[ext] ?? File;
  return <Icon size={size} strokeWidth={1.5} className="text-muted shrink-0" />;
}
