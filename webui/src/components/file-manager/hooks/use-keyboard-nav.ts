import { useState, useRef, useCallback } from "react";
import { type FileEntry } from "../../../lib/ninep/client";

interface UseKeyboardNavOptions {
  containerRef: React.RefObject<HTMLDivElement | null>;
  lastClickedRef: React.RefObject<string | null>;
  onSelect: (names: string[], mode: "replace" | "toggle" | "add") => void;
  onOpen: (entry: FileEntry) => void;
  onSelectAll: () => void;
  scrollToIndex?: (index: number) => void;
}

export function useKeyboardNav({
  containerRef,
  lastClickedRef,
  onSelect,
  onOpen,
  onSelectAll,
  scrollToIndex,
}: UseKeyboardNavOptions) {
  const [focusIndex, setFocusIndexState] = useState(-1);
  const focusIndexRef = useRef(-1);
  const setFocusIndex = (i: number) => {
    focusIndexRef.current = i;
    setFocusIndexState(i);
  };

  const sortedRef = useRef<FileEntry[]>([]);
  const viewRef = useRef<"grid" | "list">("list");

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent, renameTarget: string | null, inlineCreate: "file" | "directory" | null) => {
      if (renameTarget || inlineCreate) return;
      if ((e.target as HTMLElement).closest?.("[data-radix-menu-content]")) return;

      const s = sortedRef.current;
      if (!s.length) return;

      const move = (delta: number, shift: boolean) => {
        e.preventDefault();
        e.stopPropagation();
        const cur = focusIndexRef.current;
        const start = cur < 0 ? (delta > 0 ? -1 : s.length) : cur;
        const next = Math.max(0, Math.min(s.length - 1, start + delta));
        if (next < 0 || next >= s.length) return;
        setFocusIndex(next);
        lastClickedRef.current = s[next].name;
        if (shift) {
          onSelect([s[next].name], "add");
        } else {
          onSelect([s[next].name], "replace");
        }
        if (scrollToIndex) {
          scrollToIndex(next);
        } else {
          const row = containerRef.current?.querySelector(`[data-index="${next}"]`);
          row?.scrollIntoView({ block: "nearest" });
        }
      };

      const getGridCols = () => {
        const w = containerRef.current?.clientWidth ?? 0;
        return Math.max(1, Math.floor((w - 32) / 136)); // 120px min + 16px gap approx
      };
      const cols = viewRef.current === "grid" ? getGridCols() : 1;

      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          move(cols, e.shiftKey);
          break;
        case "ArrowUp":
          e.preventDefault();
          move(-cols, e.shiftKey);
          break;
        case "ArrowRight":
          if (viewRef.current === "grid") {
            e.preventDefault();
            move(1, e.shiftKey);
          }
          break;
        case "ArrowLeft":
          if (viewRef.current === "grid") {
            e.preventDefault();
            move(-1, e.shiftKey);
          }
          break;
        case "Enter": {
          const cur = focusIndexRef.current;
          if (cur >= 0 && cur < s.length) {
            e.preventDefault();
            onOpen(s[cur]);
          }
          break;
        }
        case "a":
          if (e.ctrlKey || e.metaKey) {
            e.preventDefault();
            onSelectAll();
          }
          break;
      }
    },
    [onSelect, onOpen, onSelectAll, lastClickedRef, containerRef, scrollToIndex],
  );

  return { focusIndex, setFocusIndex, focusIndexRef, sortedRef, viewRef, handleKeyDown };
}
