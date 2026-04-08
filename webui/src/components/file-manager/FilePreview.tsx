import { lazy, Suspense, useEffect, useState, useRef, useCallback } from "react";
import {
  X,
  Download,
  Maximize2,
  Minimize2,
  Loader2,
  FileText,
  Binary,
  Eye,
  ZoomIn,
  ZoomOut,
  Maximize,
  Save,
} from "lucide-react";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import { TransformWrapper, TransformComponent } from "react-zoom-pan-pinch";
import * as Tooltip from "@radix-ui/react-tooltip";
import { HexViewer } from "./HexViewer";
import { ToolbarButton } from "./Toolbar";
import { filetypemime } from "magic-bytes.js";
import { p9client } from "../../lib/ninep/client";
import { formatError } from "../../lib/errors";
import "../../lib/monaco";

const MonacoEditor = lazy(() => import("@monaco-editor/react").then((m) => ({ default: m.default })));
const PDFViewer = lazy(() => import("./PDFViewer").then((m) => ({ default: m.PDFViewer })));

interface FilePreviewProps {
  path: string;
  name: string;
  size: bigint;
  fullscreen: boolean;
  onToggleFullscreen: () => void;
  onClose: () => void;
  onDownload: () => void;
  onDirtyChange?: (dirty: boolean) => void;
}

const MONACO_LANG_MAP: Record<string, string> = {
  js: "javascript",
  jsx: "javascript",
  ts: "typescript",
  tsx: "typescript",
  py: "python",
  rb: "ruby",
  rs: "rust",
  go: "go",
  java: "java",
  c: "c",
  h: "c",
  cpp: "cpp",
  hpp: "cpp",
  cc: "cpp",
  cs: "csharp",
  swift: "swift",
  kt: "kotlin",
  scala: "scala",
  sh: "shell",
  bash: "shell",
  zsh: "shell",
  json: "json",
  jsonc: "json",
  yaml: "yaml",
  yml: "yaml",
  toml: "ini",
  ini: "ini",
  cfg: "ini",
  xml: "xml",
  html: "html",
  css: "css",
  sql: "sql",
  md: "markdown",
  dockerfile: "dockerfile",
  graphql: "graphql",
  proto: "protobuf",
  lua: "lua",
  php: "php",
  r: "r",
  dart: "dart",
};

function getMonacoLanguage(name: string): string {
  const ext = name.split(".").pop()?.toLowerCase() ?? "";
  return MONACO_LANG_MAP[ext] ?? MONACO_LANG_MAP[name.toLowerCase()] ?? "plaintext";
}

type DetectedType = { kind: "image"; mime: string } | { kind: "pdf" } | { kind: "text" } | { kind: "binary" };

function detectType(data: Uint8Array, name: string): DetectedType {
  const mimes = filetypemime(data);
  if (mimes.length > 0) {
    const mime = mimes[0];
    if (mime.startsWith("image/")) return { kind: "image", mime };
    if (mime.startsWith("video/")) return { kind: "binary" };
    if (mime.startsWith("audio/")) return { kind: "binary" };
    if (mime === "application/pdf") return { kind: "pdf" };
    return { kind: "binary" };
  }

  // Fallback for formats magic-bytes.js misses
  const ext = name.split(".").pop()?.toLowerCase() ?? "";
  if (ext === "svg") return { kind: "image", mime: "image/svg+xml" };
  const MEDIA_EXTS = new Set([
    "mov",
    "mp4",
    "mkv",
    "avi",
    "wmv",
    "flv",
    "webm",
    "m4v",
    "3gp",
    "mp3",
    "wav",
    "flac",
    "aac",
    "ogg",
    "wma",
    "m4a",
    "opus",
  ]);
  if (MEDIA_EXTS.has(ext)) return { kind: "binary" };

  // Check if content looks like text: no null bytes in first 8KB
  const sample = data.slice(0, 8192);
  for (let i = 0; i < sample.length; i++) {
    if (sample[i] === 0) return { kind: "binary" };
  }
  return { kind: "text" };
}

const MAX_TEXT_SIZE = 5n * 1024n * 1024n; // 5MB for Monaco
const MAX_MEDIA_SIZE = 50n * 1024n * 1024n; // 50MB for images/PDF loaded into memory

export function FilePreview({
  path,
  name,
  size,
  fullscreen,
  onToggleFullscreen,
  onClose,
  onDownload,
  onDirtyChange,
}: FilePreviewProps) {
  const qc = useQueryClient();
  const [content, setContent] = useState<string | null>(null);
  const [mediaUrl, setMediaUrl] = useState<string | null>(null);
  const [detectedType, setDetectedType] = useState<DetectedType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showHex, setShowHex] = useState(false);
  const [dirty, setDirtyState] = useState(false);
  const setDirty = useCallback(
    (d: boolean) => {
      setDirtyState(d);
      onDirtyChange?.(d);
    },
    [onDirtyChange],
  );
  const [saving, setSaving] = useState(false);
  const editedContentRef = useRef<string | null>(null);
  const savedContentRef = useRef<string | null>(null);
  const saveRef = useRef<() => void>(() => {});

  const handleSave = useCallback(async () => {
    if (!dirty || editedContentRef.current === null) return;
    setSaving(true);
    try {
      const data = new TextEncoder().encode(editedContentRef.current);
      await p9client.saveFile(path, data);
      savedContentRef.current = editedContentRef.current;
      setDirty(false);
      toast.success("File saved");
      qc.invalidateQueries({ queryKey: ["9p", "ls"] });
    } catch (err) {
      toast.error("Failed to save", { description: formatError(err) });
    } finally {
      setSaving(false);
    }
  }, [dirty, path, qc, setDirty]);
  saveRef.current = handleSave;

  useEffect(() => {
    const handler = (e: BeforeUnloadEvent) => {
      if (dirty) {
        e.preventDefault();
      }
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [dirty]);

  useEffect(() => {
    let cancelled = false;
    setContent(null);
    setMediaUrl(null);
    setDetectedType(null);
    setLoading(true);
    setError(null);
    setShowHex(false);
    setDirty(false);
    editedContentRef.current = null;
    savedContentRef.current = null;

    (async () => {
      try {
        // 16KB head to catch formats with headers beyond 4KB
        const head = await p9client.readFileHead(path, 16384);
        if (cancelled) return;

        const type = detectType(head, name);
        if (cancelled) return;
        setDetectedType(type);

        if (type.kind === "binary") {
          setLoading(false);
          return;
        }

        if (type.kind === "text" && size > MAX_TEXT_SIZE) {
          setDetectedType({ kind: "binary" }); // fall back to hex viewer
          setLoading(false);
          return;
        }

        if ((type.kind === "image" || type.kind === "pdf") && size > MAX_MEDIA_SIZE) {
          setDetectedType({ kind: "binary" });
          setLoading(false);
          return;
        }

        if (type.kind === "image" || type.kind === "pdf") {
          const mime = type.kind === "pdf" ? "application/pdf" : type.mime;
          const data = await p9client.readFile(path);
          if (cancelled) return;
          const blob = new Blob([data.slice().buffer as ArrayBuffer], { type: mime });
          setMediaUrl(URL.createObjectURL(blob));
        } else {
          const data = await p9client.readFile(path);
          if (cancelled) return;
          setContent(new TextDecoder().decode(data));
        }
      } catch (e) {
        if (!cancelled) setError(formatError(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, name, size]);

  useEffect(() => {
    return () => {
      if (mediaUrl) URL.revokeObjectURL(mediaUrl);
    };
  }, [mediaUrl]);

  return (
    <div
      className={`flex flex-col h-full bg-card select-text ${fullscreen ? "" : "border-l border-border animate-[slideIn_0.15s_ease-out]"}`}
    >
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-border shrink-0">
        <span className="text-sm font-mono truncate mr-3">
          {name}
          {dirty && (
            <span className="text-primary ml-1.5" title="Unsaved changes">
              &bull;
            </span>
          )}
        </span>
        <Tooltip.Provider delayDuration={400} skipDelayDuration={100}>
          <div className="flex items-center gap-0.5 shrink-0">
            {detectedType?.kind === "text" && (
              <ToolbarButton
                onClick={handleSave}
                title="Save (Ctrl+S)"
                className={saving ? "animate-pulse" : dirty ? "text-primary" : "opacity-30 pointer-events-none"}
              >
                <Save size={14} strokeWidth={1.5} />
              </ToolbarButton>
            )}
            {detectedType && detectedType.kind !== "binary" && (
              <ToolbarButton
                onClick={() => setShowHex((h) => !h)}
                title={showHex ? "Preview" : "Hex"}
                className={showHex ? "text-foreground bg-accent" : ""}
              >
                {showHex ? <Eye size={14} strokeWidth={1.5} /> : <Binary size={14} strokeWidth={1.5} />}
              </ToolbarButton>
            )}
            <ToolbarButton onClick={onDownload} title="Download">
              <Download size={14} strokeWidth={1.5} />
            </ToolbarButton>
            <ToolbarButton onClick={onToggleFullscreen} title={fullscreen ? "Exit fullscreen" : "Fullscreen"}>
              {fullscreen ? <Minimize2 size={14} strokeWidth={1.5} /> : <Maximize2 size={14} strokeWidth={1.5} />}
            </ToolbarButton>
            <ToolbarButton onClick={onClose} title="Close">
              <X size={14} strokeWidth={1.5} />
            </ToolbarButton>
          </div>
        </Tooltip.Provider>
      </div>

      <div className="relative flex-1 min-h-0">
        {showHex ? (
          <HexViewer path={path} fileSize={size} />
        ) : loading ? (
          <div className="flex flex-col items-center justify-center h-full gap-3 text-muted">
            <Loader2 size={18} className="animate-spin" />
            <span className="text-xs">Loading...</span>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-full text-destructive text-sm px-4">{error}</div>
        ) : detectedType?.kind === "image" && mediaUrl ? (
          <TransformWrapper initialScale={1} minScale={0.1} maxScale={20} centerOnInit>
            {({ zoomIn, zoomOut, resetTransform }) => (
              <>
                <Tooltip.Provider delayDuration={400} skipDelayDuration={100}>
                  <div className="absolute top-2 left-1/2 -translate-x-1/2 z-10 flex items-center gap-1 bg-card/90 backdrop-blur-sm rounded-lg border border-border px-1 py-0.5">
                    <ToolbarButton onClick={() => zoomOut()} title="Zoom out">
                      <ZoomOut size={14} strokeWidth={1.5} />
                    </ToolbarButton>
                    <ToolbarButton onClick={() => resetTransform()} title="Reset zoom">
                      <Maximize size={14} strokeWidth={1.5} />
                    </ToolbarButton>
                    <ToolbarButton onClick={() => zoomIn()} title="Zoom in">
                      <ZoomIn size={14} strokeWidth={1.5} />
                    </ToolbarButton>
                  </div>
                </Tooltip.Provider>
                <TransformComponent
                  wrapperClass="checkerboard"
                  wrapperStyle={{ width: "100%", height: "100%" }}
                  contentStyle={{
                    width: "100%",
                    height: "100%",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  <div className="w-full h-full flex items-center justify-center p-6">
                    <img src={mediaUrl} alt={name} className="max-w-full max-h-full object-contain" />
                  </div>
                </TransformComponent>
              </>
            )}
          </TransformWrapper>
        ) : detectedType?.kind === "pdf" && mediaUrl ? (
          <Suspense
            fallback={
              <div className="flex items-center justify-center h-full text-muted">
                <Loader2 size={18} className="animate-spin" />
              </div>
            }
          >
            <PDFViewer url={mediaUrl} />
          </Suspense>
        ) : detectedType?.kind === "text" && content !== null ? (
          <div className="absolute inset-0">
            <Suspense
              fallback={
                <div className="flex items-center justify-center h-full gap-3 text-muted">
                  <Loader2 size={18} className="animate-spin" />
                  <span className="text-xs">Loading editor...</span>
                </div>
              }
            >
              <MonacoEditor
                width="100%"
                height="100%"
                language={getMonacoLanguage(name)}
                value={content}
                theme="github-dark"
                onChange={(value) => {
                  editedContentRef.current = value ?? null;
                  setDirty(value !== (savedContentRef.current ?? content));
                }}
                onMount={(editor) => {
                  editor.addCommand(
                    // Monaco.KeyMod.CtrlCmd | Monaco.KeyCode.KeyS
                    2048 | 49,
                    () => saveRef.current(),
                  );
                }}
                options={{
                  minimap: { enabled: false },
                  fontSize: 12,
                  fontFamily: "'Geist Mono', 'SF Mono', monospace",
                  lineNumbers: "on",
                  scrollBeyondLastLine: false,
                  renderLineHighlight: "gutter",
                  overviewRulerLanes: 0,
                  hideCursorInOverviewRuler: true,
                  padding: { top: 8, bottom: 8 },
                  scrollbar: {
                    vertical: "auto",
                    horizontal: "auto",
                    verticalScrollbarSize: 10,
                    horizontalScrollbarSize: 10,
                  },
                }}
              />
            </Suspense>
          </div>
        ) : detectedType?.kind === "binary" ? (
          <HexViewer path={path} fileSize={size} />
        ) : (
          <div className="flex flex-col items-center justify-center h-full gap-3 text-muted px-4">
            <FileText size={32} strokeWidth={1} />
            <p className="text-xs">Loading...</p>
          </div>
        )}
      </div>
    </div>
  );
}
