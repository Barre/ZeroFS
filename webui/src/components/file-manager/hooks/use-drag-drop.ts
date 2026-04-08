import { useState } from "react";
import { type FileEntry } from "../../../lib/ninep/client";
import { joinPath } from "../../../lib/format";
import { DRAG_MIME } from "../FileBrowser";

interface UseDragAndDropOptions {
  currentPath: string;
  selectedNames: Set<string>;
  onMoveToDir: (entryName: string, targetDir: string) => void;
}

export function useDragAndDrop({ currentPath, selectedNames, onMoveToDir }: UseDragAndDropOptions) {
  const [dropTarget, setDropTarget] = useState<string | null>(null);

  const onDragStart = (e: React.DragEvent, entry: FileEntry) => {
    const names = selectedNames.has(entry.name) && selectedNames.size > 1 ? Array.from(selectedNames) : [entry.name];
    e.dataTransfer.setData(DRAG_MIME, JSON.stringify(names));
    e.dataTransfer.effectAllowed = "move";

    const badge = document.createElement("div");
    badge.textContent = names.length > 1 ? `${names.length} items` : entry.name;
    Object.assign(badge.style, {
      position: "absolute",
      top: "-9999px",
      padding: "6px 12px",
      borderRadius: "6px",
      background: "var(--color-card)",
      color: "var(--color-foreground)",
      fontSize: "13px",
      fontWeight: "500",
      whiteSpace: "nowrap",
      maxWidth: "300px",
      overflow: "hidden",
      textOverflow: "ellipsis",
    });
    document.body.appendChild(badge);
    e.dataTransfer.setDragImage(badge, badge.offsetWidth / 2, badge.offsetHeight / 2);
    requestAnimationFrame(() => badge.remove());
  };

  const onDirDragOver = (e: React.DragEvent, entry: FileEntry) => {
    if (!entry.resolvedIsDir || !e.dataTransfer.types.includes(DRAG_MIME)) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    setDropTarget(entry.name);
  };

  const onDirDragLeave = (_e: React.DragEvent, entry: FileEntry) => {
    setDropTarget((prev) => (prev === entry.name ? null : prev));
  };

  const onDirDrop = (e: React.DragEvent, entry: FileEntry) => {
    e.preventDefault();
    setDropTarget(null);
    if (!entry.resolvedIsDir) return;
    const raw = e.dataTransfer.getData(DRAG_MIME);
    if (!raw) return;
    const names: string[] = JSON.parse(raw);
    const targetDir = joinPath(currentPath, entry.name);
    for (const name of names) {
      if (name === entry.name) continue;
      onMoveToDir(name, targetDir);
    }
  };

  return { dropTarget, onDragStart, onDirDragOver, onDirDragLeave, onDirDrop };
}
