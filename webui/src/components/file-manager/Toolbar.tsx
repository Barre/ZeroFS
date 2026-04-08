import {
  Grid3X3,
  List,
  FolderPlus,
  FilePlus,
  Upload,
  Download,
  RefreshCw,
  Trash2,
  X,
  ArrowUp,
  ArrowDown,
  Activity,
  PanelRight,
  PanelRightClose,
  FolderOpen,
  Pencil,
} from "lucide-react";
import * as Tooltip from "@radix-ui/react-tooltip";
import { Button } from "../ui/Button";
import { Tip } from "../ui/Tip";
import { SearchBar } from "./SearchBar";
import { usePresence } from "../../hooks/use-presence";
import { formatSizeFixed, formatOps } from "../../lib/format";
import type { FileEntry } from "../../lib/ninep/client";

export function ToolbarButton({
  onClick,
  title,
  active,
  className,
  children,
}: {
  onClick: () => void;
  title: string;
  active?: boolean;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <Tip label={title}>
      <Button variant="icon" onClick={onClick} className={`${active ? "bg-accent text-foreground" : ""} ${className ?? ""}`}>
        {children}
      </Button>
    </Tip>
  );
}

interface TrafficStats {
  bytesSent: number;
  bytesReceived: number;
  ops: number;
}

interface ToolbarProps {
  path: string;
  onNavigate: (path: string) => void;
  spinKey: number;
  onRefresh: () => void;
  onNewFolder: () => void;
  onNewFile: () => void;
  fileInputRef: React.RefObject<HTMLInputElement | null>;
  onFileInputClick: () => void;
  onFileInputChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onSelectFile: (filePath: string, name: string) => void;
  view: "grid" | "list";
  onViewChange: (v: "grid" | "list") => void;
  previewEnabled: boolean;
  onTogglePreview: () => void;
  splitWidth: number;
  traffic: TrafficStats;
  selected: Set<string>;
  entries: FileEntry[] | undefined;
  onOpenSelected: () => void;
  onRenameSelected: () => void;
  onDownloadSelected: () => void;
  onDeleteSelected: () => void;
  onClearSelection: () => void;
}

export function Toolbar({
  path,
  onNavigate,
  spinKey,
  onRefresh,
  onNewFolder,
  onNewFile,
  fileInputRef,
  onFileInputClick,
  onFileInputChange,
  onSelectFile,
  view,
  onViewChange,
  previewEnabled,
  onTogglePreview,
  splitWidth,
  traffic,
  selected,
  entries,
  onOpenSelected,
  onRenameSelected,
  onDownloadSelected,
  onDeleteSelected,
  onClearSelection,
}: ToolbarProps) {
  const hasSelection = selected.size > 0 && !!entries;
  const { mounted: selMounted, open: selOpen } = usePresence(hasSelection);
  const singleSelected = selected.size === 1 && entries ? entries.find((e) => e.name === [...selected][0]) : null;

  return (
    <Tooltip.Provider delayDuration={400} skipDelayDuration={100}>
      <div className="flex items-center justify-between px-4 h-10 border-b border-border shrink-0 min-w-0">
        <div className="flex items-center gap-1 min-w-0">
          <ToolbarButton onClick={onRefresh} title="Refresh">
            <RefreshCw
              key={spinKey}
              size={16}
              strokeWidth={1.5}
              style={spinKey > 0 ? { animation: "spinOnce 0.5s ease-in-out" } : undefined}
            />
          </ToolbarButton>
          <ToolbarButton onClick={onNewFolder} title="New folder">
            <FolderPlus size={16} strokeWidth={1.5} />
          </ToolbarButton>
          <ToolbarButton onClick={onNewFile} title="New file">
            <FilePlus size={16} strokeWidth={1.5} />
          </ToolbarButton>
          <ToolbarButton onClick={onFileInputClick} title="Upload">
            <Upload size={16} strokeWidth={1.5} />
          </ToolbarButton>
          <input ref={fileInputRef} type="file" multiple className="hidden" onChange={onFileInputChange} />
          <SearchBar
            currentPath={path}
            onNavigate={onNavigate}
            onSelectFile={(filePath, name) => {
              const parentDir = filePath.substring(0, filePath.lastIndexOf("/")) || "/";
              if (parentDir !== path) onNavigate(parentDir);
              onSelectFile(filePath, name);
            }}
          />
          <div className="w-px h-5 bg-border mx-1.5" />
          <ToolbarButton onClick={() => onViewChange("grid")} title="Grid view" active={view === "grid"}>
            <Grid3X3 size={16} strokeWidth={1.5} />
          </ToolbarButton>
          <ToolbarButton onClick={() => onViewChange("list")} title="List view" active={view === "list"}>
            <List size={16} strokeWidth={1.5} />
          </ToolbarButton>
          <ToolbarButton
            onClick={onTogglePreview}
            title={previewEnabled ? "Hide preview panel" : "Show preview panel"}
            active={previewEnabled}
          >
            {previewEnabled ? (
              <PanelRightClose size={16} strokeWidth={1.5} />
            ) : (
              <PanelRight size={16} strokeWidth={1.5} />
            )}
          </ToolbarButton>
          {splitWidth >= 1000 && (
            <>
              <div className="w-px h-5 bg-border mx-1.5 shrink-0" />
              <div className="flex items-center gap-2.5 text-[13px] font-mono font-medium text-muted tabular-nums">
                <span className="flex items-center gap-1 whitespace-pre" title="Upload">
                  <ArrowUp size={11} strokeWidth={2} className={traffic.bytesSent > 0 ? "text-emerald-400" : ""} />
                  {formatSizeFixed(traffic.bytesSent)}/s
                </span>
                <span className="flex items-center gap-1 whitespace-pre" title="Download">
                  <ArrowDown size={11} strokeWidth={2} className={traffic.bytesReceived > 0 ? "text-blue-400" : ""} />
                  {formatSizeFixed(traffic.bytesReceived)}/s
                </span>
                <span className="flex items-center gap-1 whitespace-pre" title="Operations per second">
                  <Activity size={11} strokeWidth={2} />
                  {formatOps(traffic.ops).padStart(4)} op/s
                </span>
              </div>
            </>
          )}
        </div>

        <div className="flex items-center gap-1 shrink-0">
          {selMounted && (
            <span
              className={`flex items-center gap-1 ${selOpen ? "animate-[fadeIn_0.15s_ease-out]" : "animate-[fadeOut_0.15s_ease-out_forwards]"}`}
            >
              <span className="text-sm mr-1">
                <span className="font-medium">{selected.size}</span>
                <span className="text-muted ml-1">selected</span>
              </span>
              {singleSelected?.resolvedIsDir && (
                <Tip label="Open folder">
                  <Button variant="icon" onClick={onOpenSelected}>
                    <FolderOpen size={14} strokeWidth={1.5} />
                  </Button>
                </Tip>
              )}
              {singleSelected && (
                <Tip label="Rename">
                  <Button variant="icon" onClick={onRenameSelected}>
                    <Pencil size={14} strokeWidth={1.5} />
                  </Button>
                </Tip>
              )}
              <Tip label="Download">
                <Button variant="icon" onClick={onDownloadSelected}>
                  <Download size={14} strokeWidth={1.5} />
                </Button>
              </Tip>
              <Tip label="Delete">
                <Button
                  variant="icon"
                  onClick={onDeleteSelected}
                  className="text-destructive hover:text-destructive hover:bg-destructive/10"
                >
                  <Trash2 size={14} strokeWidth={1.5} />
                </Button>
              </Tip>
              <Tip label="Clear selection">
                <Button variant="icon" onClick={onClearSelection}>
                  <X size={14} strokeWidth={1.5} />
                </Button>
              </Tip>
            </span>
          )}
        </div>
      </div>
    </Tooltip.Provider>
  );
}
