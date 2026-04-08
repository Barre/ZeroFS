import { useState, useMemo, useRef, useCallback, useEffect, useLayoutEffect } from "react";
import { useResizeObserver } from "../../hooks/use-resize-observer";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowUp, ArrowDown, FolderPlus, FilePlus } from "lucide-react";
import { type FileEntry } from "../../lib/ninep/client";
import { FileIcon } from "./FileIcon";
import { formatSize, formatMode } from "../../lib/format";
import { FileContextMenuWrapper, type ContextMenuAction } from "./FileContextMenu";
import { Checkbox } from "./Checkbox";
import { InlineInput } from "./InlineInput";
import { useDragAndDrop } from "./hooks/use-drag-drop";
import { useKeyboardNav } from "./hooks/use-keyboard-nav";

const DRAG_MIME = "application/x-zerofs-entry";

interface FileBrowserProps {
  entries: FileEntry[];
  currentPath: string;
  view: "grid" | "list";
  selectedNames: Set<string>;
  onOpen: (entry: FileEntry) => void;
  onSelect: (names: string[], mode: "replace" | "toggle" | "add") => void;
  onSelectAll: () => void;
  onClearSelection: () => void;
  getContextActions: (entry: FileEntry) => ContextMenuAction[];
  onMoveToDir: (entryName: string, targetDir: string) => void;
  renameTarget: string | null;
  onRename: (newName: string) => void;
  onRenameCancel: () => void;
  inlineCreate: "file" | "directory" | null;
  onCreate: (name: string) => void;
  onCreateCancel: () => void;
}

type SortKey = "name" | "size" | "modified" | "permissions" | "owner";
type SortDir = "asc" | "desc";

function formatDateTime(epochSec: bigint): string {
  if (epochSec === 0n) return "--";
  const d = new Date(Number(epochSec) * 1000);
  const now = new Date();
  const pad = (n: number) => n.toString().padStart(2, "0");
  const time = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  const month = d.toLocaleString("default", { month: "short" });
  if (d.getFullYear() === now.getFullYear()) return `${month} ${d.getDate()}, ${time}`;
  return `${month} ${d.getDate()}, ${d.getFullYear()}`;
}

function compareEntries(a: FileEntry, b: FileEntry, key: SortKey, dir: SortDir): number {
  if (a.resolvedIsDir !== b.resolvedIsDir) return a.resolvedIsDir ? -1 : 1;
  let cmp = 0;
  switch (key) {
    case "name":
      cmp = a.name.localeCompare(b.name);
      break;
    case "size":
      cmp = Number(a.size - b.size);
      break;
    case "modified":
      cmp = Number(a.mtimeSec - b.mtimeSec);
      break;
    case "permissions":
      cmp = a.mode - b.mode;
      break;
    case "owner":
      cmp = a.uid - b.uid || a.gid - b.gid;
      break;
  }
  if (cmp === 0 && key !== "name") cmp = a.name.localeCompare(b.name);
  return dir === "asc" ? cmp : -cmp;
}

function SortIndicator({ column, sortKey, sortDir }: { column: SortKey; sortKey: SortKey; sortDir: SortDir }) {
  const Icon = sortDir === "asc" ? ArrowUp : ArrowDown;
  return (
    <span className="inline-flex w-4 justify-center ml-0.5">
      {column === sortKey && <Icon size={11} className="text-primary" />}
    </span>
  );
}

export { DRAG_MIME };

export function FileBrowser({
  entries,
  currentPath,
  view,
  selectedNames,
  onOpen,
  onSelect,
  onSelectAll,
  onClearSelection,
  getContextActions,
  onMoveToDir,
  renameTarget,
  onRename,
  onRenameCancel,
  inlineCreate,
  onCreate,
  onCreateCancel,
}: FileBrowserProps) {
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const lastClickedRef = useRef<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "name" ? "asc" : "desc");
    }
  };

  const sorted = useMemo(
    () => [...entries].sort((a, b) => compareEntries(a, b, sortKey, sortDir)),
    [entries, sortKey, sortDir],
  );

  const { dropTarget, onDragStart, onDirDragOver, onDirDragLeave, onDirDrop } = useDragAndDrop({
    currentPath,
    selectedNames,
    onMoveToDir,
  });

  const scrollToIndexRef = useRef<((index: number) => void) | null>(null);
  const scrollToIndexFn = useCallback((index: number) => scrollToIndexRef.current?.(index), []);
  const { focusIndex, setFocusIndex, sortedRef, viewRef, handleKeyDown } = useKeyboardNav({
    containerRef,
    lastClickedRef,
    onSelect,
    onOpen,
    onSelectAll,
    scrollToIndex: scrollToIndexFn,
  });

  useLayoutEffect(() => {
    sortedRef.current = sorted;
    viewRef.current = view;
  });

  useEffect(() => {
    if (selectedNames.size === 1) {
      const name = [...selectedNames][0];
      const idx = sorted.findIndex((s) => s.name === name);
      if (idx !== -1) setFocusIndex(idx);
    } else if (selectedNames.size === 0) {
      setFocusIndex(-1);
    }
  }, [selectedNames, sorted, setFocusIndex]);

  const handleClick = useCallback(
    (e: React.MouseEvent, name: string) => {
      if (e.shiftKey && lastClickedRef.current) {
        const names = sorted.map((s) => s.name);
        const from = names.indexOf(lastClickedRef.current);
        const to = names.indexOf(name);
        if (from !== -1 && to !== -1) {
          const start = Math.min(from, to);
          const end = Math.max(from, to);
          onSelect(names.slice(start, end + 1), "add");
          lastClickedRef.current = name;
          return;
        }
      }
      lastClickedRef.current = name;
      if (e.ctrlKey || e.metaKey) {
        onSelect([name], "toggle");
      } else {
        onSelect([name], "replace");
      }
    },
    [sorted, onSelect],
  );


  // All hooks must be before the grid early-return
  const [tableRef, { width: tableWidth }] = useResizeObserver();
  const [gridRef, { width: gridWidth }] = useResizeObserver();

  const GRID_ITEM_MIN = 120;
  const GRID_GAP = 8;
  const GRID_PAD = 16;
  const GRID_ROW_H = 128;
  const LIST_ROW_H = 41;
  const gridCols = Math.max(1, Math.floor((gridWidth - 2 * GRID_PAD + GRID_GAP) / (GRID_ITEM_MIN + GRID_GAP)));

  const virtualizer = useVirtualizer({
    count: view === "grid" ? Math.ceil(sorted.length / gridCols) : sorted.length,
    getScrollElement: () => containerRef.current,
    estimateSize: () => (view === "grid" ? GRID_ROW_H : LIST_ROW_H),
    overscan: view === "grid" ? 3 : 10,
  });
  scrollToIndexRef.current = (index: number) => {
    const virIdx = view === "grid" ? Math.floor(index / gridCols) : index;
    virtualizer.scrollToIndex(virIdx, { align: "auto" });
  };

  if (view === "grid") {
    const virtualItems = virtualizer.getVirtualItems();
    return (
      <div
        ref={(el) => { containerRef.current = el; gridRef(el); }}
        tabIndex={0}
        onKeyDown={(e) => handleKeyDown(e, renameTarget, inlineCreate)}
        className="overflow-auto h-full select-none !outline-none"
      >
        {inlineCreate && (
          <div className="px-4 pt-4">
            <div className="flex flex-col items-center gap-2 p-4 rounded-lg text-center bg-primary/5 ring-1 ring-primary/30 max-w-[152px]">
              {inlineCreate === "directory" ? (
                <FolderPlus size={32} strokeWidth={1.5} className="text-primary" />
              ) : (
                <FilePlus size={32} strokeWidth={1.5} className="text-primary" />
              )}
              <InlineInput
                defaultValue={inlineCreate === "directory" ? "New Folder" : "new-file.txt"}
                onConfirm={onCreate}
                onCancel={onCreateCancel}
              />
            </div>
          </div>
        )}
        <div className="relative" style={{ height: virtualizer.getTotalSize() + GRID_PAD * 2 }}>
          {virtualItems.map((virtualRow) => {
            const startIdx = virtualRow.index * gridCols;
            const rowEntries = sorted.slice(startIdx, startIdx + gridCols);
            return (
              <div
                key={virtualRow.key}
                className="absolute grid gap-2"
                style={{
                  top: virtualRow.start + GRID_PAD,
                  left: GRID_PAD,
                  right: GRID_PAD,
                  gridTemplateColumns: `repeat(${gridCols}, minmax(0, 1fr))`,
                }}
              >
                {rowEntries.map((entry, colIdx) => {
                  const i = startIdx + colIdx;
                  return (
                    <FileContextMenuWrapper key={entry.name} actions={getContextActions(entry)}>
                      <div
                        data-index={i}
                        draggable={renameTarget !== entry.name}
                        onDragStart={(e) => onDragStart(e, entry)}
                        onDragOver={(e) => onDirDragOver(e, entry)}
                        onDragLeave={(e) => onDirDragLeave(e, entry)}
                        onDrop={(e) => onDirDrop(e, entry)}
                        style={{ height: GRID_ROW_H - GRID_GAP }}
                        className={`flex flex-col items-center gap-2 p-4 rounded-lg text-center cursor-pointer transition-all ${
                          dropTarget === entry.name
                            ? "bg-[#316dca33] ring-1 ring-[#316dca66]"
                            : selectedNames.has(entry.name)
                              ? "bg-[#316dca26]"
                              : "bg-background hover:bg-accent"
                        } ${i === focusIndex ? "ring-1 ring-primary/50" : ""}`}
                        onClick={(e) => handleClick(e, entry.name)}
                        onDoubleClick={() => onOpen(entry)}
                      >
                        <FileIcon name={entry.name} isDir={entry.isDir} isSymlink={entry.isSymlink} size={32} />
                        {renameTarget === entry.name ? (
                          <InlineInput
                            defaultValue={entry.name}
                            onConfirm={onRename}
                            onCancel={onRenameCancel}
                            selectBase
                          />
                        ) : (
                          <span className="text-[13px] truncate w-full leading-tight">{entry.name}</span>
                        )}
                        {!entry.resolvedIsDir && renameTarget !== entry.name && (
                          <span className="text-xs font-mono text-muted-foreground">{formatSize(entry.size)}</span>
                        )}
                      </div>
                    </FileContextMenuWrapper>
                  );
                })}
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  const thBase =
    "px-4 py-3 text-xs font-medium uppercase tracking-wider cursor-pointer select-none hover:text-foreground transition-colors";

  const showModified = tableWidth >= 600;
  const showOwner = tableWidth >= 750;
  const showMode = tableWidth >= 900;

  const virtualItems = virtualizer.getVirtualItems();
  const paddingTop = virtualItems.length > 0 ? virtualItems[0].start : 0;
  const paddingBottom =
    virtualItems.length > 0 ? virtualizer.getTotalSize() - virtualItems[virtualItems.length - 1].end : 0;

  const colgroup = (
    <colgroup>
      <col className="w-[40px]" />
      <col />
      {showMode && <col className="w-[120px]" />}
      {showOwner && <col className="w-[80px]" />}
      <col className="w-[100px]" />
      {showModified && <col className="w-[140px]" />}
    </colgroup>
  );

  return (
    <div
      tabIndex={0}
      onKeyDown={(e) => handleKeyDown(e, renameTarget, inlineCreate)}
      className="!outline-none flex flex-col h-full"
    >
      <table ref={(el) => tableRef(el)} className="w-full text-sm select-none table-fixed shrink-0">
        {colgroup}
        <thead>
          <tr className="border-b border-border text-left text-muted">
            <th className="px-3 py-3">
              <Checkbox
                checked={selectedNames.size > 0 && selectedNames.size === sorted.length}
                indeterminate={selectedNames.size > 0 && selectedNames.size < sorted.length}
                onChange={() => (selectedNames.size === sorted.length ? onClearSelection() : onSelectAll())}
              />
            </th>
            <th className={thBase} onClick={() => toggleSort("name")}>
              <span className="flex items-center">
                Name
                <SortIndicator column="name" sortKey={sortKey} sortDir={sortDir} />
              </span>
            </th>
            {showMode && (
              <th className={thBase} onClick={() => toggleSort("permissions")}>
                <span className="flex items-center">
                  Mode
                  <SortIndicator column="permissions" sortKey={sortKey} sortDir={sortDir} />
                </span>
              </th>
            )}
            {showOwner && (
              <th className={thBase} onClick={() => toggleSort("owner")}>
                <span className="flex items-center justify-end">
                  Owner
                  <SortIndicator column="owner" sortKey={sortKey} sortDir={sortDir} />
                </span>
              </th>
            )}
            <th className={thBase} onClick={() => toggleSort("size")}>
              <span className="flex items-center justify-end">
                Size
                <SortIndicator column="size" sortKey={sortKey} sortDir={sortDir} />
              </span>
            </th>
            {showModified && (
              <th className={thBase} onClick={() => toggleSort("modified")}>
                <span className="flex items-center justify-end">
                  Modified
                  <SortIndicator column="modified" sortKey={sortKey} sortDir={sortDir} />
                </span>
              </th>
            )}
          </tr>
        </thead>
      </table>
      <div ref={containerRef} className="overflow-auto flex-1 min-h-0">
        <table className="w-full text-sm select-none table-fixed">
          {colgroup}
          <tbody>
            {inlineCreate && (
              <tr className="border-b border-border/40 bg-primary/5">
                <td className="px-3 py-2.5" />
                <td className="px-4 py-2.5">
                  <div className="flex items-center gap-3 min-w-0">
                    {inlineCreate === "directory" ? (
                      <FolderPlus size={18} strokeWidth={1.5} className="text-primary shrink-0" />
                    ) : (
                      <FilePlus size={18} strokeWidth={1.5} className="text-primary shrink-0" />
                    )}
                    <InlineInput
                      defaultValue={inlineCreate === "directory" ? "New Folder" : "new-file.txt"}
                      onConfirm={onCreate}
                      onCancel={onCreateCancel}
                    />
                  </div>
                </td>
                <td colSpan={10} />
              </tr>
            )}
            {paddingTop > 0 && (
              <tr>
                <td style={{ height: paddingTop }} />
              </tr>
            )}
            {virtualItems.map((virtualRow) => {
              const i = virtualRow.index;
              const entry = sorted[i];
              return (
                <FileContextMenuWrapper key={entry.name} actions={getContextActions(entry)}>
                  <tr
                    data-index={i}
                    draggable={renameTarget !== entry.name}
                    onDragStart={(e) => onDragStart(e, entry)}
                    onDragOver={(e) => onDirDragOver(e, entry)}
                    onDragLeave={(e) => onDirDragLeave(e, entry)}
                    onDrop={(e) => onDirDrop(e, entry)}
                    className={`border-b border-border/40 border-l-2 cursor-pointer transition-colors group ${
                      dropTarget === entry.name
                        ? "bg-[#316dca33]"
                        : selectedNames.has(entry.name)
                          ? "bg-[#316dca26]"
                          : "hover:bg-accent"
                    } ${i === focusIndex ? "border-l-primary" : "border-l-transparent"}`}
                    onClick={(e) => handleClick(e, entry.name)}
                    onDoubleClick={() => onOpen(entry)}
                  >
                    <td className="px-3 py-2.5">
                      <Checkbox
                        checked={selectedNames.has(entry.name)}
                        onChange={() => onSelect([entry.name], "toggle")}
                      />
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-3 min-w-0">
                        <FileIcon name={entry.name} isDir={entry.isDir} isSymlink={entry.isSymlink} size={18} />
                        {renameTarget === entry.name ? (
                          <InlineInput
                            defaultValue={entry.name}
                            onConfirm={onRename}
                            onCancel={onRenameCancel}
                            selectBase
                          />
                        ) : (
                          <span className="group-hover:text-foreground transition-colors truncate">{entry.name}</span>
                        )}
                      </div>
                    </td>
                    {showMode && (
                      <td className="px-4 py-2.5 font-mono text-[13px] text-muted">
                        {entry.mode ? formatMode(entry.mode) : ""}
                      </td>
                    )}
                    {showOwner && (
                      <td className="px-4 py-2.5 font-mono text-[13px] text-muted text-right">
                        {entry.uid}:{entry.gid}
                      </td>
                    )}
                    <td className="px-4 py-2.5 font-mono text-[13px] text-muted text-right tabular-nums">
                      {entry.resolvedIsDir ? "--" : formatSize(entry.size)}
                    </td>
                    {showModified && (
                      <td className="px-4 py-2.5 text-[13px] text-muted text-right tabular-nums">
                        {formatDateTime(entry.mtimeSec)}
                      </td>
                    )}
                  </tr>
                </FileContextMenuWrapper>
              );
            })}
            {paddingBottom > 0 && (
              <tr>
                <td style={{ height: paddingBottom }} />
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
