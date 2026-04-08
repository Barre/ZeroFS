import { useRef, useState, useEffect, useCallback } from "react";
import { useQueryClient } from "@tanstack/react-query";
import * as Dialog from "@radix-ui/react-dialog";
import { X, Info, FolderOpen, Loader2, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { useStat } from "../../hooks/use-ninep";
import { useFrozen } from "../../hooks/use-frozen";
import { p9client } from "../../lib/ninep/client";
import { formatSize, formatMode, formatTimestamp } from "../../lib/format";
import { formatError } from "../../lib/errors";
import { Button } from "../ui/Button";
import { Checkbox } from "./Checkbox";
import { overlayClass, contentClass } from "./dialog-classes";

interface FilePropertiesProps {
  path: string | null;
  onClose: () => void;
}

const propsContentClass = `${contentClass} w-[440px]`;

function parseOctal(s: string): number | null {
  if (!/^[0-7]{3,4}$/.test(s)) return null;
  return parseInt(s, 8);
}

export function FileProperties({ path, onClose }: FilePropertiesProps) {
  const frozenPath = useFrozen(path) ?? "/";
  const qc = useQueryClient();
  const { data, isLoading, refetch } = useStat(frozenPath);

  const [modeInput, setModeInput] = useState("");
  const [uidInput, setUidInput] = useState("");
  const [gidInput, setGidInput] = useState("");
  const [recursive, setRecursive] = useState(false);
  const [applying, setApplying] = useState(false);
  const [dirStats, setDirStats] = useState<{ size: bigint; files: number; dirs: number } | null>(null);
  const [computingSize, setComputingSize] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  // Sync inputs when data loads
  useEffect(() => {
    if (!data) return;
    setModeInput((data.stat.mode & 0o7777).toString(8).padStart(3, "0"));
    setUidInput(String(data.stat.uid));
    setGidInput(String(data.stat.gid));
  }, [data]);

  // Reset when path changes (new file or close)
  useEffect(() => {
    abortRef.current?.abort();
    setComputingSize(false);
    setDirStats(null);
    setRecursive(false);
  }, [path]);

  const computeDirSize = useCallback(async () => {
    abortRef.current?.abort();
    const abort = new AbortController();
    abortRef.current = abort;
    setComputingSize(true);
    setDirStats(null);
    try {
      const stats = await p9client.dirSize(
        frozenPath,
        (progress) => {
          if (!abort.signal.aborted) setDirStats(progress);
        },
        abort.signal,
      );
      if (!abort.signal.aborted) setDirStats(stats);
    } catch {
      if (!abort.signal.aborted) toast.error("Failed to compute folder size");
    } finally {
      if (!abort.signal.aborted) setComputingSize(false);
    }
  }, [frozenPath]);

  const hasChanges = data
    ? modeInput !== (data.stat.mode & 0o7777).toString(8).padStart(3, "0") ||
    uidInput !== String(data.stat.uid) ||
    gidInput !== String(data.stat.gid)
    : false;

  const canApply = hasChanges || recursive;

  const handleApply = async () => {
    if (!data) return;

    // Validate all inputs upfront
    const parsedMode = parseOctal(modeInput);
    if (parsedMode === null) {
      toast.error("Invalid mode — use octal (e.g. 755)");
      return;
    }
    const parsedUid = parseInt(uidInput, 10);
    if (isNaN(parsedUid) || parsedUid < 0) {
      toast.error("Invalid UID");
      return;
    }
    const parsedGid = parseInt(gidInput, 10);
    if (isNaN(parsedGid) || parsedGid < 0) {
      toast.error("Invalid GID");
      return;
    }

    // When recursive, apply all current values (children may differ from root).
    // When not recursive, only apply what changed on the root.
    const opts: { mode?: number; uid?: number; gid?: number } = {};
    if (recursive) {
      opts.mode = parsedMode;
      opts.uid = parsedUid;
      opts.gid = parsedGid;
    } else {
      if (parsedMode !== (data.stat.mode & 0o7777)) opts.mode = parsedMode;
      if (parsedUid !== data.stat.uid) opts.uid = parsedUid;
      if (parsedGid !== data.stat.gid) opts.gid = parsedGid;
    }

    if (Object.keys(opts).length === 0) return;

    setApplying(true);
    try {
      if (recursive && data.isDir) {
        const { applied, failed } = await p9client.setattrRecursive(frozenPath, opts);
        if (failed > 0) {
          toast.warning(`Applied to ${applied} items (${failed} failed)`);
        } else {
          toast.success(`Applied to ${applied} items`);
        }
      } else {
        await p9client.setattr(frozenPath, opts);
        toast.success("Properties updated");
      }
      refetch();
      qc.invalidateQueries({ queryKey: ["9p", "ls"] });
    } catch (err) {
      toast.error("Failed to update properties", { description: formatError(err) });
    } finally {
      setApplying(false);
    }
  };

  return (
    <Dialog.Root
      open={!!path}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className={overlayClass} />
        <Dialog.Content className={propsContentClass}>
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <div className="flex items-center gap-2.5">
              <Info size={16} strokeWidth={1.5} className="text-primary" />
              <Dialog.Title className="text-sm font-semibold">Properties</Dialog.Title>
            </div>
            <Dialog.Close asChild>
              <Button variant="icon">
                <X size={16} strokeWidth={1.5} />
              </Button>
            </Dialog.Close>
          </div>
          <div className="px-5 py-4 max-h-[60vh] overflow-auto select-none">
            {isLoading ? (
              <p className="text-muted text-sm">Loading...</p>
            ) : data ? (
              <dl className="space-y-2.5">
                <PropRow label="Name" value={data.name} mono />
                <PropRow label="Path" value={data.path} mono />
                <div className="border-t border-border pt-2.5" />

                {/* Size  with compute button for dirs */}
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-0.5">Size</dt>
                  <dd className="text-sm text-foreground font-mono flex items-baseline gap-2 flex-wrap select-text">
                    <span>{formatSize(data.stat.size)}</span>
                    {data.isDir && dirStats !== null && (
                      <span className="flex items-center gap-1 text-xs text-muted">
                        {computingSize ? (
                          <Loader2 size={12} className="animate-spin" />
                        ) : (
                          <Button variant="icon-sm" onClick={computeDirSize} title="Refresh">
                            <RefreshCw size={11} />
                          </Button>
                        )}
                        {formatSize(dirStats.size)} in {dirStats.files.toLocaleString()} file
                        {dirStats.files !== 1 ? "s" : ""}, {dirStats.dirs.toLocaleString()} folder
                        {dirStats.dirs !== 1 ? "s" : ""}
                      </span>
                    )}
                    {data.isDir && dirStats === null && (
                      <Button
                        variant="ghost"
                        onClick={computeDirSize}
                        disabled={computingSize}
                        className="inline-flex items-center gap-1.5 text-xs h-6 px-1.5"
                      >
                        {computingSize ? <Loader2 size={12} className="animate-spin" /> : <FolderOpen size={12} />}
                        Compute total
                      </Button>
                    )}
                  </dd>
                </div>

                {/* Editable mode */}
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-0.5">Mode</dt>
                  <dd className="flex items-center gap-2">
                    <input
                      type="text"
                      value={modeInput}
                      onChange={(e) => setModeInput(e.target.value)}
                      className="bg-transparent border border-border rounded px-2 py-0.5 text-sm font-mono w-16 focus:outline-none focus:border-primary"
                    />
                    <span className="text-xs text-muted font-mono">
                      {(() => {
                        const parsed = parseOctal(modeInput);
                        if (parsed === null) return "----------";
                        return formatMode((data.stat.mode & ~0o7777) | parsed);
                      })()}
                    </span>
                  </dd>
                </div>

                {/* Editable UID/GID */}
                <div className="flex gap-4">
                  <div className="flex-1">
                    <dt className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-0.5">UID</dt>
                    <dd>
                      <input
                        type="text"
                        value={uidInput}
                        onChange={(e) => setUidInput(e.target.value)}
                        className="bg-transparent border border-border rounded px-2 py-0.5 text-sm font-mono w-full focus:outline-none focus:border-primary"
                      />
                    </dd>
                  </div>
                  <div className="flex-1">
                    <dt className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-0.5">GID</dt>
                    <dd>
                      <input
                        type="text"
                        value={gidInput}
                        onChange={(e) => setGidInput(e.target.value)}
                        className="bg-transparent border border-border rounded px-2 py-0.5 text-sm font-mono w-full focus:outline-none focus:border-primary"
                      />
                    </dd>
                  </div>
                  <div className="flex-1">
                    <PropRow label="Links" value={String(data.stat.nlink)} mono />
                  </div>
                </div>

                {/* Recursive toggle for directories */}
                {data.isDir && (
                  <div
                    className="flex items-center gap-2 text-sm cursor-pointer select-none"
                    onClick={() => setRecursive((r) => !r)}
                  >
                    <Checkbox checked={recursive} onChange={() => setRecursive((r) => !r)} />
                    <span>Apply recursively</span>
                  </div>
                )}

                <div className="border-t border-border pt-2.5" />
                <PropRow label="Modified" value={formatTimestamp(data.stat.mtimeSec, data.stat.mtimeNsec)} />
                <PropRow label="Accessed" value={formatTimestamp(data.stat.atimeSec, data.stat.atimeNsec)} />
                <PropRow label="Created" value={formatTimestamp(data.stat.ctimeSec, data.stat.ctimeNsec)} />
              </dl>
            ) : null}
          </div>
          <div className="flex justify-end gap-2 px-5 py-3 border-t border-border">
            <Dialog.Close asChild>
              <Button variant="ghost">Close</Button>
            </Dialog.Close>
            {canApply && (
              <Button variant="primary" onClick={handleApply} disabled={applying}>
                {applying ? (
                  <span className="flex items-center gap-1.5">
                    <Loader2 size={13} className="animate-spin" />
                    Applying...
                  </span>
                ) : (
                  "Apply"
                )}
              </Button>
            )}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function PropRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div>
      <dt className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-0.5">{label}</dt>
      <dd className={`text-sm text-foreground truncate select-text ${mono ? "font-mono" : ""}`}>{value}</dd>
    </div>
  );
}
