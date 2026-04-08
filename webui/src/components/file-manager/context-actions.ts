import { FolderOpen, Download, Pencil, Trash2, Info, Eye } from "lucide-react";
import { type ContextMenuAction } from "./FileContextMenu";

export function buildContextActions({
  isDir,
  onOpen,
  onPreview,
  onDownload,
  onRename,
  onDelete,
  onProperties,
}: {
  isDir: boolean;
  onOpen: () => void;
  onPreview?: () => void;
  onDownload: () => void;
  onRename?: () => void;
  onDelete: () => void;
  onProperties?: () => void;
}): ContextMenuAction[] {
  const actions: ContextMenuAction[] = [{ label: "Open", icon: FolderOpen, onClick: onOpen }];
  if (onPreview && !isDir) {
    actions.push({ label: "Preview", icon: Eye, onClick: onPreview });
  }
  actions.push({ label: isDir ? "Download as zip" : "Download", icon: Download, onClick: onDownload });
  if (onRename) actions.push({ label: "Rename", icon: Pencil, onClick: onRename, preventCloseAutoFocus: true });
  if (onProperties) actions.push({ label: "Properties", icon: Info, onClick: onProperties });
  actions.push({ label: "Delete", icon: Trash2, onClick: onDelete, destructive: true });
  return actions;
}
