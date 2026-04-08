import { useState, useRef, useCallback, useEffect, useLayoutEffect } from "react";
import { Search, X, Loader2 } from "lucide-react";
import * as Dialog from "@radix-ui/react-dialog";
import { p9client, type SearchResult } from "../../lib/ninep/client";
import { Button } from "../ui/Button";
import { ToolbarButton } from "./Toolbar";
import { FileIcon } from "./FileIcon";
import { overlayClass } from "./dialog-classes";

interface SearchBarProps {
  currentPath: string;
  onNavigate: (path: string) => void;
  onSelectFile: (path: string, name: string) => void;
}

export function SearchBar({ currentPath, onNavigate, onSelectFile }: SearchBarProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [searching, setSearching] = useState(false);
  const [focusIdx, setFocusIdx] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  const startSearch = useCallback(
    (q: string) => {
      abortRef.current?.abort();
      setResults([]);

      if (q.length < 2) {
        setSearching(false);
        return;
      }

      setSearching(true);
      setFocusIdx(-1);
      const abort = new AbortController();
      abortRef.current = abort;

      p9client
        .search(
          currentPath,
          q,
          (result) => {
            if (!abort.signal.aborted) {
              setResults((prev) => (prev.length < 100 ? [...prev, result] : prev));
            }
          },
          abort.signal,
        )
        .catch(() => {})
        .finally(() => {
          if (!abort.signal.aborted) setSearching(false);
        });
    },
    [currentPath],
  );

  useEffect(() => {
    const timer = setTimeout(() => startSearch(query), 200);
    return () => clearTimeout(timer);
  }, [query, startSearch]);

  useLayoutEffect(() => {
    if (focusIdx < 0) return;
    const el = listRef.current?.children[focusIdx] as HTMLElement | undefined;
    el?.scrollIntoView({ block: "nearest" });
  }, [focusIdx]);

  const close = useCallback(() => {
    abortRef.current?.abort();
    setOpen(false);
    setQuery("");
    setResults([]);
    setSearching(false);
  }, []);

  const handleSelect = useCallback(
    (result: SearchResult) => {
      if (result.isDir) {
        onNavigate(result.path);
      } else {
        onSelectFile(result.path, result.name);
      }
      close();
    },
    [onNavigate, onSelectFile, close],
  );

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        e.preventDefault();
        setOpen(true);
        setTimeout(() => inputRef.current?.focus(), 0);
      }
      if (e.key === "Escape" && open) {
        close();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open, close]);

  return (
    <>
      <ToolbarButton
        onClick={() => {
          setOpen(true);
          setTimeout(() => inputRef.current?.focus(), 0);
        }}
        title="Search (Ctrl+F)"
      >
        <Search size={16} strokeWidth={1.5} />
      </ToolbarButton>
      <Dialog.Root
        open={open}
        onOpenChange={(o) => {
          if (!o) close();
        }}
      >
        <Dialog.Portal>
          <Dialog.Overlay className={overlayClass} />
          <Dialog.Content className="fixed left-1/2 top-[15vh] -translate-x-1/2 z-50 bg-background border border-border-bright rounded-lg w-[500px] max-h-[60vh] flex flex-col shadow-[0_8px_32px_rgba(0,0,0,0.5)] data-[state=open]:animate-[dialogIn_0.15s_ease-out] data-[state=closed]:animate-[dialogOut_0.15s_ease-out]">
            <Dialog.Title className="sr-only">Search files</Dialog.Title>
            <div className="flex items-center gap-2 px-4 py-1.5 border-b border-border text-xs text-muted-foreground">
              <span>Searching in</span>
              <span className="font-mono text-foreground bg-accent px-1.5 py-0.5 rounded">{currentPath}</span>
            </div>
            <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
              {searching ? (
                <Loader2 size={16} strokeWidth={1.5} className="animate-spin text-muted shrink-0" />
              ) : (
                <Search size={16} strokeWidth={1.5} className="text-muted shrink-0" />
              )}
              <input
                ref={inputRef}
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "ArrowDown") {
                    e.preventDefault();
                    setFocusIdx((i) => Math.min(i + 1, results.length - 1));
                  } else if (e.key === "ArrowUp") {
                    e.preventDefault();
                    setFocusIdx((i) => Math.max(i - 1, -1));
                  } else if (e.key === "Enter" && focusIdx >= 0 && focusIdx < results.length) {
                    e.preventDefault();
                    handleSelect(results[focusIdx]);
                  }
                }}
                placeholder={`Search in ${currentPath === "/" ? "/" : currentPath.split("/").pop()}... (* ? /regex/)`}
                className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
                autoFocus
              />
              <Button variant="icon-sm" onClick={close}>
                <X size={14} strokeWidth={1.5} />
              </Button>
            </div>

            <div ref={listRef} className="overflow-auto flex-1">
              {query.length < 2 ? (
                <p className="px-4 py-6 text-sm text-muted text-center">Type at least 2 characters to search</p>
              ) : results.length === 0 ? (
                <div className="px-4 py-6 text-sm text-muted text-center">
                  {searching ? (
                    <span className="flex items-center justify-center gap-2">
                      <Loader2 size={14} className="animate-spin" />
                      Searching...
                    </span>
                  ) : (
                    "No results found"
                  )}
                </div>
              ) : (
                <>
                  {results.map((result, i) => (
                    <button
                      key={result.path}
                      onClick={() => handleSelect(result)}
                      onMouseEnter={() => setFocusIdx(i)}
                      className={`flex items-center gap-3 w-full px-4 py-2.5 text-left transition-colors ${i === focusIdx ? "bg-accent" : "hover:bg-accent"}`}
                    >
                      <FileIcon name={result.name} isDir={result.isDir} isSymlink={false} size={16} />
                      <div className="min-w-0 flex-1">
                        <p className="text-sm truncate">
                          {result.name.slice(0, result.matchStart)}
                          <span className="text-primary font-semibold">
                            {result.name.slice(result.matchStart, result.matchEnd)}
                          </span>
                          {result.name.slice(result.matchEnd)}
                        </p>
                        <p className="text-xs text-muted-foreground font-mono truncate">{result.path}</p>
                      </div>
                    </button>
                  ))}
                  {searching && (
                    <p className="px-4 py-2 text-xs text-muted text-center flex items-center justify-center gap-1.5">
                      <Loader2 size={10} className="animate-spin" />
                      Searching...
                    </p>
                  )}
                </>
              )}
              {results.length >= 100 && (
                <p className="px-4 py-2 text-xs text-muted text-center">Showing first 100 results</p>
              )}
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  );
}
