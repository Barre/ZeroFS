import { useState, useRef } from "react";
import { Upload } from "lucide-react";
import { DRAG_MIME } from "./FileBrowser";

interface UploadZoneProps {
  dirPath: string;
  disabled?: boolean;
  onUpload: (files: FileList) => void;
  onUploadItems: (items: DataTransferItemList) => void;
  children: React.ReactNode;
}

function isInternalDrag(e: React.DragEvent): boolean {
  return e.dataTransfer.types.includes(DRAG_MIME);
}

function hasDirectories(items: DataTransferItemList): boolean {
  for (let i = 0; i < items.length; i++) {
    const entry = items[i].webkitGetAsEntry?.();
    if (entry?.isDirectory) return true;
  }
  return false;
}

export function UploadZone({ children, disabled, onUpload, onUploadItems }: UploadZoneProps) {
  const [isDragging, setIsDragging] = useState(false);
  const dragCounter = useRef(0);

  return (
    <div
      className="relative flex-1 overflow-hidden"
      onDragEnter={(e) => {
        e.preventDefault();
        if (disabled || isInternalDrag(e)) return;
        dragCounter.current++;
        setIsDragging(true);
      }}
      onDragOver={(e) => e.preventDefault()}
      onDragLeave={(e) => {
        e.preventDefault();
        if (disabled || isInternalDrag(e)) return;
        dragCounter.current--;
        if (dragCounter.current === 0) setIsDragging(false);
      }}
      onDrop={(e) => {
        e.preventDefault();
        dragCounter.current = 0;
        setIsDragging(false);
        if (disabled || isInternalDrag(e)) return;
        if (e.dataTransfer.items && hasDirectories(e.dataTransfer.items)) {
          onUploadItems(e.dataTransfer.items);
        } else if (e.dataTransfer.files.length > 0) {
          onUpload(e.dataTransfer.files);
        }
      }}
    >
      {children}
      {isDragging && (
        <div className="absolute inset-0 bg-primary/5 border-2 border-dashed border-primary/30 rounded flex items-center justify-center z-40 backdrop-blur-[2px]">
          <div className="flex flex-col items-center gap-2 text-primary">
            <Upload size={32} strokeWidth={1} />
            <span className="text-sm font-medium">Drop files or folders to upload</span>
          </div>
        </div>
      )}
    </div>
  );
}
