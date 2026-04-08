import { useCallback, useEffect, useRef, useState } from "react";
import { usePresence } from "../../hooks/use-presence";
import {
  ChevronDown,
  ChevronUp,
  Download,
  Upload,
  Trash2,
  Check,
  X,
  RotateCcw,
  Loader2,
  Square,
  Clock,
} from "lucide-react";
import { formatSize } from "../../lib/format";
import { TransferContext, type TransferHandle } from "./TransferContext";

export type { TransferHandle } from "./TransferContext";

type TransferStatus = "active" | "done" | "error" | "cancelled";

interface TransferEntry {
  id: number;
  type: "download" | "upload" | "delete";
  name: string;
  status: TransferStatus;
  received: number;
  total: number;
  startedAt: number;
  error?: string;
  detail?: string;
  retry?: () => void;
  abort?: AbortController;
}

export function TransferPanel({ children }: { children: React.ReactNode }) {
  const [transfers, setTransfers] = useState<TransferEntry[]>([]);
  const [collapsed, setCollapsed] = useState(false);
  const nextId = useRef(0);

  const createHandle = useCallback(
    (id: number, abort: AbortController): TransferHandle => ({
      id,
      signal: abort.signal,
      setName: (name: string) => {
        setTransfers((prev) => prev.map((t) => (t.id === id ? { ...t, name } : t)));
      },
      update: (received: number, total: number, detail?: string) => {
        setTransfers((prev) => prev.map((t) => (t.id === id ? { ...t, received, total, detail } : t)));
      },
      finish: () => {
        setTransfers((prev) =>
          prev.map((t) => (t.id === id ? { ...t, status: "done" as const, received: t.total, detail: undefined } : t)),
        );
      },
      fail: (error: string) => {
        setTransfers((prev) =>
          prev.map((t) => (t.id === id && t.status === "active" ? { ...t, status: "error" as const, error } : t)),
        );
      },
    }),
    [],
  );

  const startTransfer = useCallback(
    (type: TransferEntry["type"], name: string, total: number, retry?: () => void): TransferHandle => {
      const id = nextId.current++;
      const abort = new AbortController();
      setTransfers((prev) => [
        ...prev,
        { id, type, name, status: "active", received: 0, total, startedAt: performance.now(), retry, abort },
      ]);
      setCollapsed(false);
      return createHandle(id, abort);
    },
    [createHandle],
  );

  const startDownload = useCallback(
    (name: string, retry?: () => void) => startTransfer("download", name, 0, retry),
    [startTransfer],
  );

  const startUpload = useCallback(
    (name: string, total: number, retry?: () => void) => startTransfer("upload", name, total, retry),
    [startTransfer],
  );

  const startDelete = useCallback(
    (name: string, retry?: () => void) => startTransfer("delete", name, 0, retry),
    [startTransfer],
  );

  const cancelTransfer = useCallback((id: number) => {
    setTransfers((prev) =>
      prev.map((t) => {
        if (t.id === id && t.status === "active") {
          t.abort?.abort();
          return { ...t, status: "cancelled" as const };
        }
        return t;
      }),
    );
  }, []);

  const clearCompleted = useCallback(() => {
    setTransfers((prev) => prev.filter((t) => t.status === "active"));
  }, []);

  const dismiss = useCallback((id: number) => {
    setTransfers((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const activeCount = transfers.filter((t) => t.status === "active").length;
  const hasTransfers = transfers.length > 0;
  const { mounted: panelMounted, open: panelOpen } = usePresence(hasTransfers, 200);

  return (
    <TransferContext value={{ startDownload, startUpload, startDelete }}>
      {children}
      {panelMounted && (
        <div
          className={`fixed bottom-4 right-4 z-50 w-[320px] card-surface-raised rounded-lg overflow-hidden ${panelOpen ? "animate-[slideUp_0.2s_ease-out]" : "animate-[slideDown_0.2s_ease-out_forwards]"}`}
        >
          <button
            onClick={() => setCollapsed((c) => !c)}
            className="flex items-center justify-between w-full px-4 py-2.5 text-sm hover:bg-accent/40 transition-colors"
          >
            <span className="flex items-center gap-2">
              {activeCount > 0 && <Loader2 size={13} className="animate-spin text-primary" />}
              <span className="font-medium">
                {activeCount > 0
                  ? `${activeCount} transfer${activeCount > 1 ? "s" : ""} in progress`
                  : "Transfers complete"}
              </span>
            </span>
            <span className="flex items-center gap-1">
              {transfers.some((t) => t.status !== "active") && (
                <span
                  onClick={(e) => {
                    e.stopPropagation();
                    clearCompleted();
                  }}
                  className="text-xs text-muted hover:text-foreground px-1.5 py-0.5 rounded hover:bg-accent transition-colors"
                >
                  Clear
                </span>
              )}
              {collapsed ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
            </span>
          </button>

          {!collapsed && (
            <div className="max-h-[300px] overflow-auto border-t border-border">
              {transfers.map((t) => (
                <TransferRow key={t.id} transfer={t} onDismiss={dismiss} onCancel={cancelTransfer} />
              ))}
            </div>
          )}
        </div>
      )}
    </TransferContext>
  );
}

function formatEta(seconds: number): string {
  if (!isFinite(seconds) || seconds <= 0) return "";
  if (seconds < 60) return `${Math.ceil(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.ceil(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function TransferRow({
  transfer: t,
  onDismiss,
  onCancel,
}: {
  transfer: TransferEntry;
  onDismiss: (id: number) => void;
  onCancel: (id: number) => void;
}) {
  const [now, setNow] = useState(() => performance.now());
  useEffect(() => {
    if (t.status !== "active") return;
    const id = setInterval(() => setNow(performance.now()), 500);
    return () => clearInterval(id);
  }, [t.status]);
  const elapsed = (now - t.startedAt) / 1000;
  const speed = t.status === "active" && t.type !== "delete" && elapsed > 0 ? t.received / elapsed : 0;
  const pct = t.total > 0 ? Math.round((t.received / t.total) * 100) : null;
  const eta = speed > 0 && t.total > 0 ? (t.total - t.received) / speed : 0;

  const iconColor = t.type === "delete" ? "text-destructive" : "text-primary";
  const barColor = "bg-primary";
  const TypeIcon = t.type === "download" ? Download : t.type === "upload" ? Upload : Trash2;

  return (
    <div className="px-4 py-2.5 border-b border-border/40 last:border-0 group">
      <div className="flex items-center gap-2.5 mb-1.5">
        <span className="shrink-0">
          {t.status === "active" ? (
            <TypeIcon size={13} strokeWidth={1.5} className={iconColor} />
          ) : t.status === "done" ? (
            <Check size={13} strokeWidth={2} className="text-success" />
          ) : t.status === "cancelled" ? (
            <Square size={13} strokeWidth={2} className="text-muted" />
          ) : (
            <X size={13} strokeWidth={2} className="text-destructive" />
          )}
        </span>
        <span className="text-sm font-mono truncate flex-1">{t.name}</span>
        {t.status === "active" && (
          <button
            onClick={() => onCancel(t.id)}
            className="shrink-0 px-1.5 py-0.5 rounded opacity-0 group-hover:opacity-100 text-destructive hover:bg-destructive/10 transition-all"
            title="Cancel"
          >
            <X size={12} strokeWidth={2.5} />
          </button>
        )}
        {t.status === "error" && t.retry && (
          <button
            onClick={() => {
              onDismiss(t.id);
              t.retry!();
            }}
            className="shrink-0 p-0.5 rounded opacity-0 group-hover:opacity-100 hover:bg-accent text-muted hover:text-foreground transition-all"
            title="Retry"
          >
            <RotateCcw size={12} strokeWidth={2} />
          </button>
        )}
        {t.status !== "active" && (
          <button
            onClick={() => onDismiss(t.id)}
            className="shrink-0 p-0.5 rounded opacity-0 group-hover:opacity-100 hover:bg-accent text-muted hover:text-foreground transition-all"
          >
            <X size={12} strokeWidth={2} />
          </button>
        )}
      </div>

      {t.status === "active" && (
        <>
          {t.detail && <p className="text-xs text-muted-foreground font-mono truncate mb-1">{t.detail}</p>}
          {t.type === "delete" ? (
            <p className="text-xs text-muted font-mono">{t.received} items deleted</p>
          ) : (
            <>
              <div className="relative h-1.5 bg-white/10 rounded-full overflow-hidden mb-1.5">
                <div
                  className={`absolute inset-y-0 left-0 rounded-full transition-all duration-300 overflow-hidden ${barColor}`}
                  style={{ width: pct != null ? `${pct}%` : "100%" }}
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent animate-[shimmer_1.5s_infinite]" />
                </div>
              </div>
              <div className="flex justify-between text-[11px] text-muted font-mono">
                <span>
                  {formatSize(t.received)}
                  {pct != null ? ` / ${formatSize(t.total)} (${pct}%)` : ""}
                </span>
                <span className="flex items-center gap-1">
                  {formatSize(speed)}/s
                  {eta > 0 && (
                    <>
                      <Clock size={9} strokeWidth={2} />
                      {formatEta(eta)}
                    </>
                  )}
                </span>
              </div>
            </>
          )}
        </>
      )}

      {t.status === "done" && (
        <p className="text-xs text-muted">
          {t.type === "delete" ? `${t.received} items deleted` : formatSize(t.total)}
        </p>
      )}

      {t.status === "cancelled" && <p className="text-xs text-muted">Cancelled</p>}

      {t.status === "error" && <p className="text-xs text-destructive">{t.error}</p>}
    </div>
  );
}
