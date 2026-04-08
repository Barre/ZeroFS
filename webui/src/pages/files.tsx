import { useState, useCallback, useEffect, useRef } from "react";
import { useTitle } from "../hooks/use-title";
import { useLocation, useNavigate } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { Plus, X, FolderOpen } from "lucide-react";
import * as Tooltip from "@radix-ui/react-tooltip";
import { Tip } from "../components/ui/Tip";
import { DndContext, closestCenter, PointerSensor, useSensor, useSensors, type DragEndEvent } from "@dnd-kit/core";
import { SortableContext, horizontalListSortingStrategy, useSortable, arrayMove } from "@dnd-kit/sortable";
import { FileManager } from "../components/file-manager/FileManager";
import { DRAG_MIME } from "../components/file-manager/FileBrowser";
import { p9client } from "../lib/ninep/client";
import { formatError } from "../lib/errors";
import { joinPath } from "../lib/format";
import { toast } from "sonner";

function pathToUrl(p: string): string {
  return p.split("/").map(encodeURIComponent).join("/");
}

interface Tab {
  id: number;
  path: string;
  label: string;
}

function labelFromPath(path: string): string {
  if (path === "/") return "/";
  return path.split("/").filter(Boolean).pop() ?? "/";
}

function SortableTab({
  tab,
  isActive,
  isFileDrop,
  onSwitch,
  onClose,
  onFileDragOver,
  onFileDragLeave,
  onFileDrop,
}: {
  tab: Tab;
  isActive: boolean;
  isFileDrop: boolean;
  onSwitch: () => void;
  onClose: (e: React.MouseEvent) => void;
  onFileDragOver: (e: React.DragEvent) => void;
  onFileDragLeave: (e: React.DragEvent) => void;
  onFileDrop: (e: React.DragEvent) => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: tab.id });

  const style: React.CSSProperties = {
    transform: transform ? `translate3d(${Math.round(transform.x)}px, 0px, 0)` : undefined,
    transition: !isDragging && transition ? transition : undefined,
  };

  return (
    <button
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      onClick={onSwitch}
      onDragOver={onFileDragOver}
      onDragLeave={onFileDragLeave}
      onDrop={onFileDrop}
      className={`group relative flex items-center gap-2 pl-3.5 pr-2 py-2 text-[13px] shrink-0 max-w-[200px] rounded-t-lg transition-colors border border-border -mb-px ${
        isDragging
          ? "opacity-50 z-20 bg-background border-b-background"
          : isFileDrop
            ? "bg-primary/15 text-foreground ring-1 ring-primary/50 border-primary/50 z-10"
            : isActive
              ? "bg-background text-foreground border-b-background z-10"
              : "text-muted hover:text-foreground bg-card/70 hover:bg-card"
      }`}
    >
      <FolderOpen size={14} strokeWidth={1.5} className="shrink-0" />
      <span className="truncate font-mono">{tab.label}</span>
      <Tooltip.Provider delayDuration={400}>
        <Tip label="Close tab">
          <button
            onPointerDown={(e) => e.stopPropagation()}
            onClick={(e) => {
              e.stopPropagation();
              onClose(e);
            }}
            className={`shrink-0 ml-1 p-0.5 rounded transition-all duration-150 active:scale-[0.85] active:duration-75 ${
              isActive
                ? "text-muted-foreground hover:text-foreground hover:bg-accent"
                : "text-[#656c76] hover:text-foreground hover:bg-accent"
            }`}
          >
            <X size={12} strokeWidth={2} />
          </button>
        </Tip>
      </Tooltip.Provider>
    </button>
  );
}

export function FilesPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const initialPath = decodeURIComponent(location.pathname.replace(/^\/files/, "") || "/");

  const [tabs, setTabs] = useState<Tab[]>([{ id: 0, path: initialPath, label: labelFromPath(initialPath) }]);
  const [activeTabId, setActiveTabId] = useState(0);
  const [dropTargetTabId, setDropTargetTabId] = useState<number | null>(null);
  const switchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const nextTabIdRef = useRef(1);

  const activeTab = tabs.find((t) => t.id === activeTabId) ?? tabs[0];

  useTitle(activeTab.path === "/" ? "Files" : `Files - ${activeTab.label}`);

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 8 } }));

  const urlPath = decodeURIComponent(location.pathname.replace(/^\/files/, "") || "/");
  const isTabSwitch = useRef(false);
  useEffect(() => {
    if (isTabSwitch.current) {
      isTabSwitch.current = false;
      return;
    }
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setTabs((prev) => {
      const active = prev.find((t) => t.id === activeTabId);
      if (!active || active.path === urlPath) return prev;
      return prev.map((t) => (t.id === activeTabId ? { ...t, path: urlPath, label: labelFromPath(urlPath) } : t));
    });
  }, [urlPath, activeTabId]);

  const handleNavigate = useCallback(
    (newPath: string) => {
      setTabs((prev) =>
        prev.map((t) => (t.id === activeTabId ? { ...t, path: newPath, label: labelFromPath(newPath) } : t)),
      );
      const url = newPath === "/" ? "/files" : `/files${pathToUrl(newPath)}`;
      navigate(url);
    },
    [activeTabId, navigate],
  );

  const switchTab = useCallback(
    (id: number) => {
      isTabSwitch.current = true;
      setActiveTabId(id);
      const tab = tabs.find((t) => t.id === id);
      if (tab) {
        const url = tab.path === "/" ? "/files" : `/files${pathToUrl(tab.path)}`;
        navigate(url, { replace: true });
      }
    },
    [tabs, navigate],
  );

  const addTab = useCallback(() => {
    const id = nextTabIdRef.current++;
    isTabSwitch.current = true;
    setTabs((prev) => [...prev, { id, path: "/", label: "/" }]);
    setActiveTabId(id);
    navigate("/files", { replace: true });
  }, [navigate]);

  const closeTab = useCallback(
    (id: number, e: React.MouseEvent) => {
      e.stopPropagation();
      isTabSwitch.current = true;
      setTabs((prev) => {
        const next = prev.filter((t) => t.id !== id);
        if (next.length === 0) {
          const newTab = { id: nextTabIdRef.current++, path: "/", label: "/" };
          next.push(newTab);
        }
        if (id === activeTabId) {
          const idx = prev.findIndex((t) => t.id === id);
          const newActive = next[Math.min(idx, next.length - 1)];
          setActiveTabId(newActive.id);
          const url = newActive.path === "/" ? "/files" : `/files${pathToUrl(newActive.path)}`;
          navigate(url, { replace: true });
        }
        return next;
      });
    },
    [activeTabId, navigate],
  );

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      setTabs((prev) => {
        const oldIndex = prev.findIndex((t) => t.id === active.id);
        const newIndex = prev.findIndex((t) => t.id === over.id);
        return arrayMove(prev, oldIndex, newIndex);
      });
    }
  }, []);

  const handleTabDragOver = useCallback(
    (e: React.DragEvent, tabId: number) => {
      if (!e.dataTransfer.types.includes(DRAG_MIME)) return;
      if (tabId === activeTabId) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = "move";
      setDropTargetTabId(tabId);

      if (switchTimerRef.current) clearTimeout(switchTimerRef.current);
      switchTimerRef.current = setTimeout(() => {
        switchTab(tabId);
      }, 600);
    },
    [activeTabId, switchTab],
  );

  const handleTabDragLeave = useCallback((_e: React.DragEvent, tabId: number) => {
    setDropTargetTabId((prev) => (prev === tabId ? null : prev));
    if (switchTimerRef.current) {
      clearTimeout(switchTimerRef.current);
      switchTimerRef.current = null;
    }
  }, []);

  const handleTabDrop = useCallback(
    async (e: React.DragEvent, tabId: number) => {
      e.preventDefault();
      setDropTargetTabId(null);
      if (switchTimerRef.current) {
        clearTimeout(switchTimerRef.current);
        switchTimerRef.current = null;
      }

      const raw = e.dataTransfer.getData(DRAG_MIME);
      if (!raw) return;
      const names: string[] = JSON.parse(raw);

      const targetTab = tabs.find((t) => t.id === tabId);
      if (!targetTab) return;

      for (const entryName of names) {
        const sourcePath = joinPath(activeTab.path, entryName);
        const targetPath = joinPath(targetTab.path, entryName);
        if (sourcePath === targetPath) continue;
        try {
          await p9client.rename(sourcePath, targetPath);
          toast.success(`Moved ${entryName}`, { description: `to ${targetTab.label}` });
        } catch (err) {
          toast.error(`Failed to move ${entryName}`, { description: formatError(err) });
        }
      }
      qc.invalidateQueries({ queryKey: ["9p", "ls", activeTab.path] });
      qc.invalidateQueries({ queryKey: ["9p", "ls", targetTab?.path] });
    },
    [tabs, activeTab, qc],
  );

  return (
    <div className="h-full flex flex-col">
      <div className="flex items-end bg-card shrink-0 pl-2 pt-2 pb-px gap-1 border-b border-border overflow-x-auto">
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
          <SortableContext items={tabs.map((t) => t.id)} strategy={horizontalListSortingStrategy}>
            {tabs.map((tab) => (
              <SortableTab
                key={tab.id}
                tab={tab}
                isActive={tab.id === activeTabId}
                isFileDrop={tab.id === dropTargetTabId}
                onSwitch={() => switchTab(tab.id)}
                onClose={(e) => closeTab(tab.id, e)}
                onFileDragOver={(e) => handleTabDragOver(e, tab.id)}
                onFileDragLeave={(e) => handleTabDragLeave(e, tab.id)}
                onFileDrop={(e) => handleTabDrop(e, tab.id)}
              />
            ))}
          </SortableContext>
        </DndContext>
        <Tooltip.Provider delayDuration={400}>
          <Tip label="New tab">
            <button
              onClick={addTab}
              className="p-1.5 mb-0.5 rounded text-muted-foreground hover:text-foreground hover:bg-accent transition-all duration-150 active:scale-[0.9] active:duration-75 shrink-0"
            >
              <Plus size={15} strokeWidth={1.5} />
            </button>
          </Tip>
        </Tooltip.Provider>
      </div>

      <div className="flex-1 min-h-0">
        <FileManager key={activeTab.id} path={activeTab.path} onNavigate={handleNavigate} />
      </div>
    </div>
  );
}
