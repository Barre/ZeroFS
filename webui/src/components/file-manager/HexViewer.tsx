import React, { useRef, useState, useEffect, useCallback } from "react";
import { useResizeObserver } from "../../hooks/use-resize-observer";
import { p9client } from "../../lib/ninep/client";

interface HexViewerProps {
  path: string;
  fileSize: bigint;
}

const BYTES_PER_ROW = 16;
const CHUNK_SIZE = 16 * 1024;
const ROW_HEIGHT = 22;
// Browsers cap element height at ~33M pixels
const MAX_SCROLL_HEIGHT = 5_000_000;

export function HexViewer({ path, fileSize }: HexViewerProps) {
  const totalRows = Math.ceil(Number(fileSize) / BYTES_PER_ROW);
  const parentRef = useRef<HTMLDivElement>(null);
  const [roRef, { height: viewportHeight }] = useResizeObserver();
  const [topRow, setTopRow] = useState(0);
  const [cache, setCache] = useState<Map<number, Uint8Array>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const loadingRef = useRef<Set<number>>(new Set());
  const cacheRef = useRef(cache);
  cacheRef.current = cache;
  const pathRef = useRef(path);
  pathRef.current = path;

  useEffect(() => {
    setCache(new Map());
    setError(null);
    setTopRow(0);
    loadingRef.current = new Set();
    if (parentRef.current) parentRef.current.scrollTop = 0;
  }, [path, fileSize]);

  const visibleCount = Math.ceil(viewportHeight / ROW_HEIGHT) + 2;
  const maxTopRow = Math.max(0, totalRows - visibleCount + 2);

  const naturalHeight = totalRows * ROW_HEIGHT;
  const scrollHeight = Math.min(naturalHeight, MAX_SCROLL_HEIGHT);
  const needsMapping = naturalHeight > MAX_SCROLL_HEIGHT;

  const suppressScrollRef = useRef(false);
  const syncScrollbar = useCallback(
    (row: number) => {
      const el = parentRef.current;
      if (!el || !needsMapping) return;
      const maxScroll = scrollHeight - el.clientHeight;
      if (maxScroll <= 0) return;
      suppressScrollRef.current = true;
      el.scrollTop = maxTopRow > 0 ? (row / maxTopRow) * maxScroll : 0;
    },
    [needsMapping, scrollHeight, maxTopRow],
  );

  const handleScroll = useCallback(() => {
    if (suppressScrollRef.current) {
      suppressScrollRef.current = false;
      return;
    }
    const el = parentRef.current;
    if (!el) return;
    if (!needsMapping) {
      setTopRow(Math.floor(el.scrollTop / ROW_HEIGHT));
    } else {
      const maxScroll = scrollHeight - el.clientHeight;
      if (maxScroll <= 0) return;
      const ratio = el.scrollTop / maxScroll;
      setTopRow(Math.round(ratio * maxTopRow));
    }
  }, [needsMapping, scrollHeight, maxTopRow]);

  // Override wheel to control row-level scrolling on large files
  useEffect(() => {
    const el = parentRef.current;
    if (!el || !needsMapping) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      let rowDelta: number;
      if (e.deltaMode === 1)
        rowDelta = e.deltaY * 5; // lines
      else if (e.deltaMode === 2)
        rowDelta = e.deltaY * visibleCount; // pages
      else rowDelta = (e.deltaY / ROW_HEIGHT) * 3; // pixels to rows, 3x multiplier
      setTopRow((prev) => {
        const next = Math.max(0, Math.min(maxTopRow, Math.round(prev + rowDelta)));
        syncScrollbar(next);
        return next;
      });
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [needsMapping, maxTopRow, visibleCount, syncScrollbar]);

  const loadChunk = useCallback(async (chunkIndex: number) => {
    if (loadingRef.current.has(chunkIndex) || cacheRef.current.has(chunkIndex)) return;
    const currentPath = pathRef.current;
    loadingRef.current.add(chunkIndex);
    try {
      const offset = chunkIndex * CHUNK_SIZE;
      const data = await p9client.readFileChunk(currentPath, offset, CHUNK_SIZE);
      if (pathRef.current === currentPath) {
        setCache((prev) => new Map(prev).set(chunkIndex, data));
      }
    } catch (err) {
      if (pathRef.current === currentPath) {
        setError(err instanceof Error ? err.message : "Failed to read file");
      }
    } finally {
      loadingRef.current.delete(chunkIndex);
    }
  }, []);

  useEffect(() => {
    if (visibleCount <= 0) return;
    const timer = setTimeout(() => {
      const firstByte = topRow * BYTES_PER_ROW;
      const lastByte = Math.min(topRow + visibleCount, totalRows) * BYTES_PER_ROW;
      const firstChunk = Math.floor(firstByte / CHUNK_SIZE);
      const lastChunk = Math.floor(lastByte / CHUNK_SIZE);
      for (let c = firstChunk; c <= lastChunk; c++) {
        loadChunk(c);
      }
    }, 150);
    return () => clearTimeout(timer);
  }, [topRow, visibleCount, totalRows, loadChunk]);

  const getRowBytes = (rowIndex: number): Uint8Array | null => {
    const byteOffset = rowIndex * BYTES_PER_ROW;
    const chunkIndex = Math.floor(byteOffset / CHUNK_SIZE);
    const chunk = cache.get(chunkIndex);
    if (!chunk) return null;
    const start = byteOffset - chunkIndex * CHUNK_SIZE;
    const end = Math.min(start + BYTES_PER_ROW, chunk.length);
    if (start >= chunk.length) return null;
    return chunk.subarray(start, end);
  };

  if (error) {
    return <div className="flex items-center justify-center h-full text-destructive text-sm px-4">{error}</div>;
  }

  const rows: React.ReactNode[] = [];
  const end = Math.min(topRow + visibleCount, totalRows);
  for (let r = topRow; r < end; r++) {
    const bytes = getRowBytes(r);
    const offset = r * BYTES_PER_ROW;
    rows.push(
      <div key={r} className="flex items-center px-4 hover:bg-accent/40" style={{ height: ROW_HEIGHT }}>
        <span className="text-muted-foreground w-[72px] shrink-0 tabular-nums select-none">
          {offset.toString(16).padStart(8, "0")}
        </span>
        <span className="flex-1 tabular-nums whitespace-pre">
          {bytes ? (
            formatHex(bytes)
          ) : (
            <span className="text-muted-foreground/15 animate-pulse">{"·· ".repeat(15)}··</span>
          )}
        </span>
        <span className="w-[140px] shrink-0 ml-4 select-text">{bytes ? formatAscii(bytes) : ""}</span>
      </div>,
    );
  }

  return (
    <div ref={(el) => { parentRef.current = el; roRef(el); }} className="h-full overflow-auto font-mono text-[11px] leading-none" onScroll={handleScroll}>
      {/* Spacer for scrollbar -capped height */}
      <div style={{ height: scrollHeight, position: "relative" }}>
        {/* Sticky content stays in viewport */}
        <div style={{ position: "sticky", top: 0 }}>{rows}</div>
      </div>
    </div>
  );
}

function formatHex(bytes: Uint8Array): React.ReactNode[] {
  const elements: React.ReactNode[] = [];
  for (let i = 0; i < 16; i++) {
    if (i < bytes.length) {
      const b = bytes[i];
      elements.push(
        <span key={i} className={b === 0 ? "text-muted-foreground/30" : "text-foreground"}>
          {b.toString(16).padStart(2, "0")}
        </span>,
      );
    } else {
      elements.push(
        <span key={i} className="text-muted-foreground/20">
          {"  "}
        </span>,
      );
    }
    if (i < 15) elements.push(<span key={`s${i}`}>{i === 7 ? "  " : " "}</span>);
  }
  return elements;
}

function formatAscii(bytes: Uint8Array): React.ReactNode[] {
  return Array.from(bytes).map((b, i) => (
    <span key={i} className={b >= 32 && b < 127 ? "text-foreground" : "text-muted-foreground/30"}>
      {b >= 32 && b < 127 ? String.fromCharCode(b) : "·"}
    </span>
  ));
}
