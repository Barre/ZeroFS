import { useState, useEffect, useRef } from "react";
import { Eraser, Pause, Play } from "lucide-react";
import * as Tooltip from "@radix-ui/react-tooltip";
import { adminClient } from "../../lib/grpc/client";
import { formatSize } from "../../lib/format";
import { Button } from "../ui/Button";
import { Tip } from "../ui/Tip";
import type { FileAccessEvent, OperationParams } from "../../lib/grpc/gen/admin_pb";

const OP_COLORS: Record<string, string> = {
  READ: "text-blue-400",
  WRITE: "text-green-400",
  CREATE: "text-emerald-400",
  REMOVE: "text-red-400",
  RENAME: "text-amber-400",
  MKDIR: "text-emerald-400",
  READDIR: "text-blue-300",
  LOOKUP: "text-muted-foreground",
  SETATTR: "text-amber-300",
  LINK: "text-violet-400",
  SYMLINK: "text-violet-400",
  MKNOD: "text-orange-400",
  TRIM: "text-yellow-400",
  FSYNC: "text-cyan-400",
};

function formatTime(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${d.getMilliseconds().toString().padStart(3, "0")}`;
}

function formatParams(params: OperationParams | undefined): string {
  if (!params) return "";
  const parts: string[] = [];
  if (params.offset != null && params.length != null) {
    parts.push(`${formatSize(params.offset)}+${formatSize(params.length)}`);
  } else if (params.length != null) {
    parts.push(formatSize(params.length));
  }
  if (params.mode != null) parts.push((params.mode & 0o7777).toString(8));
  if (params.newPath) parts.push(`\u2192 ${params.newPath}`);
  if (params.linkTarget) parts.push(`\u2192 ${params.linkTarget}`);
  if (params.filename) parts.push(params.filename);
  return parts.join(" ");
}

interface TimedEvent {
  event: FileAccessEvent;
  receivedAt: Date;
}

export function FileAccessTracer() {
  const [events, setEvents] = useState<TimedEvent[]>([]);
  const [error, setError] = useState<Error | null>(null);
  const [paused, setPaused] = useState(false);
  const pausedRef = useRef(false);
  useEffect(() => { pausedRef.current = paused; }, [paused]);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const abort = new AbortController();
    (async () => {
      try {
        for await (const event of adminClient.watchFileAccess({}, { signal: abort.signal })) {
          if (pausedRef.current) continue;
          setEvents((prev) => {
            const next = [...prev, { event, receivedAt: new Date() }];
            if (next.length > 1000) return next.slice(-1000);
            return next;
          });
        }
      } catch (e) {
        if (!abort.signal.aborted) setError(e instanceof Error ? e : new Error(String(e)));
      }
    })();
    return () => abort.abort();
  }, []);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [events]);

  const opNames = [
    "READ",
    "WRITE",
    "CREATE",
    "REMOVE",
    "RENAME",
    "MKDIR",
    "READDIR",
    "LOOKUP",
    "SETATTR",
    "LINK",
    "SYMLINK",
    "MKNOD",
    "TRIM",
    "FSYNC",
  ];

  return (
    <div className="card-surface rounded-lg p-5">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">File Access Trace</p>
        <Tooltip.Provider delayDuration={400} skipDelayDuration={100}>
          <div className="flex items-center gap-0.5">
            <Tip label={paused ? "Resume" : "Pause"}>
              <Button variant="icon" onClick={() => setPaused((p) => !p)} className={paused ? "text-amber-400" : ""}>
                {paused ? <Play size={14} strokeWidth={1.5} /> : <Pause size={14} strokeWidth={1.5} />}
              </Button>
            </Tip>
            <Tip label="Clear">
              <Button variant="icon" onClick={() => setEvents([])}>
                <Eraser size={14} strokeWidth={1.5} />
              </Button>
            </Tip>
          </div>
        </Tooltip.Provider>
      </div>
      {error && <p className="text-xs text-destructive mb-2 font-mono">{error.message}</p>}
      <div ref={scrollRef} className="h-64 overflow-y-auto overflow-x-hidden font-mono text-[13px] leading-relaxed">
        {events.length === 0 ? (
          <p className="text-muted-foreground">Waiting for events...</p>
        ) : (
          events.map(({ event, receivedAt }, i) => {
            const opName = opNames[event.operation] ?? "?";
            return (
              <div key={i} className="flex gap-2 hover:bg-accent/40 px-1 -mx-1 rounded min-w-0">
                <span className="text-muted-foreground/50 shrink-0 tabular-nums">{formatTime(receivedAt)}</span>
                <span className={`w-[60px] shrink-0 text-right ${OP_COLORS[opName] ?? "text-muted"}`}>{opName}</span>
                <span className="text-foreground/80 truncate min-w-0">{event.path}</span>
                {event.params && (
                  <span className="text-muted-foreground shrink-0 whitespace-nowrap">{formatParams(event.params)}</span>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
