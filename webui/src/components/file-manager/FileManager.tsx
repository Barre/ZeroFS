import { useState, useCallback, useRef, useEffect, lazy, Suspense } from "react";
import { useResizeObserver } from "../../hooks/use-resize-observer";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Loader2, Upload } from "lucide-react";
import { useDirectory, useMkdir, useRemove, useRename, useTrafficStats } from "../../hooks/use-ninep";
import { p9client, type FileEntry } from "../../lib/ninep/client";
import { formatError } from "../../lib/errors";
import { joinPath } from "../../lib/format";
import { Breadcrumb } from "./Breadcrumb";
import { FileBrowser } from "./FileBrowser";
import { UploadZone } from "./UploadZone";
import { FileProperties } from "./FileProperties";
import { buildContextActions } from "./context-actions";
import { useUploads } from "./hooks/use-uploads";
import { usePreviewPanel } from "./hooks/use-preview-panel";
import { ConfirmDialogs } from "./ConfirmDialogs";
import { Toolbar } from "./Toolbar";
const FilePreview = lazy(() => import("./FilePreview").then((m) => ({ default: m.FilePreview })));

interface FileManagerProps {
  path: string;
  onNavigate: (path: string) => void;
}

export function FileManager({ path, onNavigate }: FileManagerProps) {
  const [view, setViewState] = useState<"grid" | "list">(() => {
    const saved = localStorage.getItem("zerofs-view");
    return saved === "grid" || saved === "list" ? saved : "list";
  });
  const setView = (v: "grid" | "list") => {
    setViewState(v);
    localStorage.setItem("zerofs-view", v);
  };
  const preview = usePreviewPanel();
  const {
    previewEnabled,
    previewFileRef,
    previewDirtyRef,
    previewWidth,
    previewFullscreen,
    setPreviewFullscreen,
    previewOpen,
    renderPreview,
    discardConfirm,
    setPreviewFile,
    setPreviewFileRaw,
    handleResizeStart,
  } = preview;
  const togglePreview = () =>
    preview.togglePreview(() => {
      if (selected.size === 1) {
        const name = [...selected][0];
        const entry = entries?.find((e) => e.name === name);
        if (entry && !entry.resolvedIsDir) {
          return { path: joinPath(path, entry.name), name: entry.name, size: entry.size };
        }
      }
      return null;
    });

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const selectedRef = useRef(selected);
  selectedRef.current = selected;
  const [createDialog, setCreateDialog] = useState<"file" | "directory" | null>(null);
  const [renameTarget, setRenameTarget] = useState<string | null>(null);
  const [propertiesPath, setPropertiesPath] = useState<string | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<{
    names: string[];
    resolve: (ok: boolean) => void;
  } | null>(null);
  const [splitRef, { width: splitWidth }] = useResizeObserver();
  const isNarrow = splitWidth > 0 && splitWidth < 700;

  const qc = useQueryClient();
  const { data: entries, isLoading, isFetching, error } = useDirectory(path);
  const [spinKey, setSpinKey] = useState(0);
  const wasFetching = useRef(false);
  useEffect(() => {
    if (isFetching && !wasFetching.current) setSpinKey((k) => k + 1);
    wasFetching.current = isFetching;
  }, [isFetching]);
  useEffect(() => {
    if (!entries) return;
    const entryNames = new Set(entries.map((e) => e.name));
    setSelected((prev) => {
      const pruned = new Set([...prev].filter((n) => entryNames.has(n)));
      return pruned.size === prev.size ? prev : pruned;
    });
  }, [entries]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (document.querySelector("[role='dialog']")) return; // let Radix handle it
      setSelected(new Set());
      setPreviewFile(null);
    };
    // Capture phase: runs before Radix removes the dialog overlay
    document.addEventListener("keydown", onKeyDown, true);
    return () => document.removeEventListener("keydown", onKeyDown, true);
  }, [setPreviewFile]);

  const traffic = useTrafficStats();
  const mkdirMut = useMkdir();
  const removeMut = useRemove();
  const renameMut = useRename();

  const {
    startDownload,
    deleteEntry,
    uploadFiles,
    handleFileUpload,
    handleFolderUpload,
    fileInputRef,
    overwritePrompt,
  } = useUploads(path, entries);

  const fullPath = useCallback((name: string) => joinPath(path, name), [path]);

  const handleOpen = useCallback(
    async (entry: FileEntry) => {
      if (!entry.resolvedIsDir) return;
      const ok = await setPreviewFile(null);
      if (!ok) return;
      onNavigate(fullPath(entry.name));
      setSelected(new Set());
    },
    [fullPath, onNavigate, setPreviewFile],
  );

  const handleSelect = useCallback(
    async (names: string[], mode: "replace" | "toggle" | "add") => {
      // Gate on unsaved changes before switching preview
      if (mode === "replace" && names.length === 1 && previewEnabled && !isNarrow && previewDirtyRef.current) {
        const entry = entries?.find((e) => e.name === names[0]);
        if (entry && !entry.resolvedIsDir) {
          const newPath = fullPath(entry.name);
          if (newPath !== previewFileRef.current?.path) {
            const ok = await setPreviewFile({ path: newPath, name: entry.name, size: entry.size });
            if (!ok) return; // User cancelled so don't change selection
          }
        }
      }

      setSelected((prev) => {
        if (mode === "replace") return new Set(names);
        if (mode === "add") {
          const next = new Set(prev);
          for (const n of names) next.add(n);
          return next;
        }
        const next = new Set(prev);
        for (const n of names) {
          if (next.has(n)) next.delete(n);
          else next.add(n);
        }
        return next;
      });
      if (mode === "replace" && names.length === 1 && !isNarrow && previewEnabled && !previewDirtyRef.current) {
        const entry = entries?.find((e) => e.name === names[0]);
        if (entry && !entry.resolvedIsDir) {
          setPreviewFile({ path: fullPath(entry.name), name: entry.name, size: entry.size });
        } else {
          setPreviewFile(null);
        }
      }
    },
    [entries, fullPath, isNarrow, previewEnabled, previewDirtyRef, previewFileRef, setPreviewFile],
  );

  const confirmAndDelete = useCallback(
    async (targets: { name: string; isDir: boolean }[]) => {
      const confirmed = await new Promise<boolean>((resolve) =>
        setDeleteConfirm({ names: targets.map((t) => t.name), resolve }),
      );
      setDeleteConfirm(null);
      if (!confirmed) return;
      for (const t of targets) await deleteEntry(t.name, t.isDir, removeMut);
      const deleted = new Set(targets.map((t) => t.name));
      setSelected((prev) => {
        const next = new Set([...prev].filter((n) => !deleted.has(n)));
        return next.size === prev.size ? prev : next;
      });
      qc.invalidateQueries({ queryKey: ["9p", "ls"] });
    },
    [deleteEntry, removeMut, qc],
  );

  const getContextActions = useCallback(
    (entry: FileEntry) => {
      const entryPath = fullPath(entry.name);
      const targets =
        selected.has(entry.name) && selected.size > 1
          ? (entries?.filter((e) => selected.has(e.name)) ?? [entry])
          : [entry];
      const isMulti = targets.length > 1;

      return buildContextActions({
        isDir: entry.resolvedIsDir && !isMulti,
        onOpen: () => handleOpen(entry),
        onPreview:
          isNarrow && !isMulti
            ? () => setPreviewFile({ path: entryPath, name: entry.name, size: entry.size })
            : undefined,
        onDownload: () => {
          for (const t of targets) startDownload(fullPath(t.name), t.resolvedIsDir);
        },
        onRename: isMulti ? undefined : () => setRenameTarget(entry.name),
        onDelete: () => confirmAndDelete(targets.map((t) => ({ name: t.name, isDir: t.resolvedIsDir }))),
        onProperties: isMulti ? undefined : () => setPropertiesPath(entryPath),
      });
    },
    [fullPath, handleOpen, startDownload, confirmAndDelete, entries, selected, isNarrow, setPreviewFile],
  );

  const handleCreate = useCallback(
    async (name: string) => {
      try {
        if (createDialog === "directory") {
          await mkdirMut.mutateAsync({ path: fullPath(name) });
          toast.success(`Created folder ${name}`);
        } else {
          await p9client.writeFile(fullPath(name), new Uint8Array(0));
          qc.invalidateQueries({ queryKey: ["9p", "ls", path] });
          toast.success(`Created file ${name}`);
        }
      } catch (err) {
        toast.error(`Failed to create ${name}`, { description: formatError(err) });
      }
      setCreateDialog(null);
    },
    [createDialog, fullPath, path, mkdirMut, qc],
  );

  const [renameConfirm, setRenameConfirm] = useState<{
    newName: string;
    resolve: (ok: boolean) => void;
  } | null>(null);

  const handleRename = useCallback(
    async (newName: string) => {
      if (!renameTarget) return;
      if (newName === renameTarget) {
        setRenameTarget(null);
        return;
      }
      if (entries?.some((e) => e.name === newName)) {
        const confirmed = await new Promise<boolean>((resolve) => setRenameConfirm({ newName, resolve }));
        setRenameConfirm(null);
        if (!confirmed) return;
      }
      try {
        await renameMut.mutateAsync({
          oldPath: fullPath(renameTarget),
          newPath: fullPath(newName),
        });
        toast.success(`Renamed to ${newName}`);
        if (selectedRef.current.has(renameTarget)) {
          setSelected((prev) => {
            const next = new Set(prev);
            next.delete(renameTarget);
            next.add(newName);
            return next;
          });
          if (previewFileRef.current?.path === fullPath(renameTarget)) {
            setPreviewFileRaw({ path: fullPath(newName), name: newName, size: previewFileRef.current.size });
          }
        }
      } catch (err) {
        toast.error(`Failed to rename ${renameTarget}`, { description: formatError(err) });
      }
      setRenameTarget(null);
    },
    [renameTarget, fullPath, renameMut, entries, previewFileRef, setPreviewFileRaw],
  );

  const handleMoveToDir = useCallback(
    async (entryName: string, targetDir: string) => {
      if (targetDir === path) return;
      const oldPath = fullPath(entryName);
      const newPath = joinPath(targetDir, entryName);
      if (oldPath === newPath) return;
      try {
        await renameMut.mutateAsync({ oldPath, newPath });
        const targetName = targetDir === "/" ? "/" : targetDir.split("/").pop();
        toast.success(`Moved ${entryName}`, { description: `to ${targetName}` });
        setSelected(new Set());
      } catch (err) {
        toast.error(`Failed to move ${entryName}`, { description: formatError(err) });
      }
    },
    [path, fullPath, renameMut],
  );

  return (
    <div ref={splitRef} className="flex flex-col h-full select-none">
      <div className="flex items-center px-4 h-10 border-b border-border shrink-0">
        <Breadcrumb path={path} onNavigate={onNavigate} onMoveToDir={handleMoveToDir} />
      </div>

      <Toolbar
        path={path}
        onNavigate={onNavigate}
        spinKey={spinKey}
        onRefresh={() => qc.invalidateQueries({ queryKey: ["9p", "ls", path] })}
        onNewFolder={() => setCreateDialog("directory")}
        onNewFile={() => setCreateDialog("file")}
        fileInputRef={fileInputRef}
        onFileInputClick={() => fileInputRef.current?.click()}
        onFileInputChange={handleFileUpload}
        onSelectFile={(filePath, name) => {
          setPreviewFile({ path: filePath, name, size: 0n });
          setSelected(new Set([name]));
        }}
        view={view}
        onViewChange={setView}
        previewEnabled={previewEnabled}
        onTogglePreview={togglePreview}
        splitWidth={splitWidth}
        traffic={traffic}
        selected={selected}
        entries={entries}
        onOpenSelected={() => {
          if (selected.size !== 1 || !entries) return;
          const name = [...selected][0];
          const entry = entries.find((e) => e.name === name);
          if (entry) handleOpen(entry);
        }}
        onRenameSelected={() => {
          if (selected.size !== 1) return;
          setRenameTarget([...selected][0]);
        }}
        onDownloadSelected={() => {
          for (const name of selected) {
            const entry = entries?.find((e) => e.name === name);
            startDownload(fullPath(name), entry?.resolvedIsDir ?? false);
          }
          setSelected(new Set());
        }}
        onDeleteSelected={() => {
          if (!entries) return;
          const targets = [...selected]
            .map((n) => entries.find((e) => e.name === n))
            .filter(Boolean)
            .map((e) => ({ name: e!.name, isDir: e!.resolvedIsDir }));
          confirmAndDelete(targets);
        }}
        onClearSelection={() => setSelected(new Set())}
      />

      <div className="flex flex-1 min-h-0">
        {/* File list (hidden when preview is fullscreen or narrow+preview open) */}
        {!(previewFullscreen || (isNarrow && renderPreview)) && (
          <UploadZone
            dirPath={path}
            disabled={isLoading || isFetching}
            onUpload={uploadFiles}
            onUploadItems={handleFolderUpload}
          >
            {isLoading ? (
              <div className="flex flex-col items-center justify-center h-64 gap-3 text-muted">
                <Loader2 size={20} className="animate-spin" />
                <span className="text-sm">Loading directory...</span>
              </div>
            ) : error ? (
              <div className="flex items-center justify-center h-64 text-destructive text-sm">{formatError(error)}</div>
            ) : entries && entries.length === 0 && !isFetching ? (
              <div className="flex flex-col items-center justify-center h-64 gap-3 text-muted">
                <Upload size={32} strokeWidth={1} className="text-muted-foreground/40" />
                <span className="text-base">This folder is empty</span>
                <span className="text-sm text-muted-foreground/60">
                  Drop files here or use the toolbar to create files and folders
                </span>
              </div>
            ) : entries ? (
              <div className="relative h-full overflow-hidden">
                <FileBrowser
                  entries={entries}
                  currentPath={path}
                  view={view}
                  selectedNames={selected}
                  onOpen={handleOpen}
                  onSelect={handleSelect}
                  onSelectAll={() => setSelected(new Set(entries?.map((e) => e.name) ?? []))}
                  onClearSelection={() => setSelected(new Set())}
                  getContextActions={getContextActions}
                  onMoveToDir={handleMoveToDir}
                  renameTarget={renameTarget}
                  onRename={handleRename}
                  onRenameCancel={() => setRenameTarget(null)}
                  inlineCreate={createDialog}
                  onCreate={handleCreate}
                  onCreateCancel={() => setCreateDialog(null)}
                />
                {isFetching && (
                  <div className="absolute inset-0 bg-background/60 flex items-center justify-center z-20">
                    <Loader2 size={20} className="animate-spin text-muted-foreground" />
                  </div>
                )}
              </div>
            ) : null}
          </UploadZone>
        )}

        {renderPreview && (
          <>
            {/* Resize handle (hidden in fullscreen/narrow) */}
            {!previewFullscreen && !isNarrow && (
              <div
                className="w-1 shrink-0 cursor-col-resize hover:bg-primary/30 active:bg-primary/50 transition-colors"
                onMouseDown={handleResizeStart}
              />
            )}
            <div
              style={previewFullscreen || isNarrow ? undefined : { width: previewWidth }}
              className={`${previewFullscreen || isNarrow ? "flex-1" : "shrink-0"} ${previewOpen ? "animate-[fadeIn_0.15s_ease-out]" : "animate-[fadeOut_0.15s_ease-out_forwards]"}`}
            >
              <Suspense
                fallback={
                  <div className="flex items-center justify-center h-full text-muted">
                    <Loader2 size={18} className="animate-spin" />
                  </div>
                }
              >
                <FilePreview
                  path={renderPreview.path}
                  name={renderPreview.name}
                  size={renderPreview.size}
                  fullscreen={previewFullscreen || isNarrow}
                  onToggleFullscreen={() => (isNarrow ? setPreviewFile(null) : setPreviewFullscreen((f) => !f))}
                  onClose={async () => {
                    await setPreviewFile(null);
                    setPreviewFullscreen(false);
                    setSelected(new Set());
                  }}
                  onDownload={() => startDownload(renderPreview.path)}
                  onDirtyChange={(d) => {
                    previewDirtyRef.current = d;
                  }}
                />
              </Suspense>
            </div>
          </>
        )}
      </div>

      <FileProperties path={propertiesPath} onClose={() => setPropertiesPath(null)} />
      <ConfirmDialogs
        discardConfirm={discardConfirm}
        deleteConfirm={deleteConfirm}
        renameConfirm={renameConfirm}
        overwritePrompt={overwritePrompt}
      />
    </div>
  );
}
