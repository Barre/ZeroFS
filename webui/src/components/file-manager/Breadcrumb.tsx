import { useState, useRef, useEffect, useCallback } from "react";
import { ChevronRight, HardDrive, Ellipsis } from "lucide-react";
import { DRAG_MIME } from "./FileBrowser";

interface BreadcrumbProps {
  path: string;
  onNavigate: (path: string) => void;
  onMoveToDir: (entryName: string, targetDir: string) => void;
}

function BreadcrumbInner({ path, onNavigate, onMoveToDir }: BreadcrumbProps) {
  const parts = path.split("/").filter((s) => s.length > 0);
  const [dropTarget, setDropTarget] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [collapseCount, setCollapseCount] = useState(0);
  const navRef = useRef<HTMLElement>(null);
  const measuringRef = useRef(false);

  useEffect(() => {
    if (expanded || !navRef.current || measuringRef.current) return;
    const nav = navRef.current;
    requestAnimationFrame(() => {
      if (nav.scrollWidth > nav.clientWidth + 2) {
        const maxCollapsible = Math.max(0, parts.length - 2);
        if (collapseCount < maxCollapsible) {
          setCollapseCount((c) => c + 1);
        }
      }
    });
  }, [collapseCount, parts.length, expanded]);

  useEffect(() => {
    if (expanded && navRef.current) {
      navRef.current.scrollLeft = navRef.current.scrollWidth;
    }
  }, [expanded]);

  useEffect(() => {
    const handle = () => {
      if (expanded) return;
      setCollapseCount(0); // reset and let the effect re-measure
    };
    window.addEventListener("resize", handle);
    return () => window.removeEventListener("resize", handle);
  }, [expanded]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    if (!e.dataTransfer.types.includes(DRAG_MIME)) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
  }, []);

  const handleDragEnter = useCallback((e: React.DragEvent, targetPath: string) => {
    if (!e.dataTransfer.types.includes(DRAG_MIME)) return;
    e.preventDefault();
    setDropTarget(targetPath);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent, targetPath: string) => {
    const btn = e.currentTarget as HTMLElement;
    if (btn.contains(e.relatedTarget as Node)) return;
    setDropTarget((prev) => (prev === targetPath ? null : prev));
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent, targetPath: string) => {
      e.preventDefault();
      setDropTarget(null);
      const raw = e.dataTransfer.getData(DRAG_MIME);
      if (!raw) return;
      const names: string[] = JSON.parse(raw);
      for (const name of names) {
        onMoveToDir(name, targetPath);
      }
    },
    [onMoveToDir],
  );

  const shouldTruncate = !expanded && collapseCount > 0 && parts.length > 2;
  let visibleParts: { part: string; index: number }[];

  if (shouldTruncate) {
    const keepFront = 1;
    const keepBack = Math.max(1, parts.length - keepFront - collapseCount);
    const startBack = parts.length - keepBack;

    visibleParts = [
      ...parts.slice(0, keepFront).map((part, i) => ({ part, index: i })),
      ...parts.slice(startBack).map((part, i) => ({ part, index: startBack + i })),
    ];
  } else {
    visibleParts = parts.map((part, index) => ({ part, index }));
  }

  const renderSegment = (part: string, index: number) => {
    const partPath = "/" + parts.slice(0, index + 1).join("/");
    const isLast = index === parts.length - 1;
    return (
      <button
        key={partPath}
        onClick={() => onNavigate(partPath)}
        onDragOver={handleDragOver}
        onDragEnter={(e) => handleDragEnter(e, partPath)}
        onDragLeave={(e) => handleDragLeave(e, partPath)}
        onDrop={(e) => handleDrop(e, partPath)}
        className={`px-2 py-1.5 rounded-md transition-all duration-150 active:scale-[0.95] active:duration-75 font-mono truncate max-w-[200px] shrink-0 ${
          dropTarget === partPath
            ? "bg-primary/15 text-foreground shadow-[inset_0_0_0_1px_var(--color-primary)]"
            : isLast
              ? "text-foreground"
              : "text-muted hover:text-foreground hover:bg-accent"
        }`}
      >
        {part}
      </button>
    );
  };

  return (
    <nav
      ref={navRef}
      className={`flex items-center gap-0.5 text-sm min-w-0 ${expanded ? "overflow-x-auto" : "overflow-hidden"}`}
    >
      <button
        onClick={() => onNavigate("/")}
        onDragOver={handleDragOver}
        onDragEnter={(e) => handleDragEnter(e, "/")}
        onDragLeave={(e) => handleDragLeave(e, "/")}
        onDrop={(e) => handleDrop(e, "/")}
        className={`flex items-center gap-2 px-2.5 py-1.5 rounded-md transition-all duration-150 active:scale-[0.95] active:duration-75 shrink-0 ${
          dropTarget === "/"
            ? "bg-primary/15 text-foreground shadow-[inset_0_0_0_1px_var(--color-primary)]"
            : "text-muted hover:text-foreground hover:bg-accent"
        }`}
      >
        <HardDrive size={15} strokeWidth={1.5} />
        <span className="font-mono">/</span>
      </button>

      {shouldTruncate && (
        <>
          <span className="flex items-center gap-0.5 shrink-0">
            <ChevronRight size={14} className="text-muted-foreground shrink-0" />
            {renderSegment(visibleParts[0].part, visibleParts[0].index)}
          </span>

          <span className="flex items-center gap-0.5 shrink-0">
            <ChevronRight size={14} className="text-muted-foreground shrink-0" />
            <button
              onClick={() => {
                setExpanded(true);
                setCollapseCount(0);
              }}
              className="px-1.5 py-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-accent transition-all duration-150 active:scale-[0.95] active:duration-75"
              title={`${collapseCount} hidden, click to expand`}
            >
              <Ellipsis size={14} />
            </button>
          </span>

          {visibleParts.slice(1).map(({ part, index }) => (
            <span key={index} className="flex items-center gap-0.5 shrink-0">
              <ChevronRight size={14} className="text-muted-foreground shrink-0" />
              {renderSegment(part, index)}
            </span>
          ))}
        </>
      )}

      {!shouldTruncate &&
        parts.map((part, i) => (
          <span key={i} className="flex items-center gap-0.5 shrink-0">
            <ChevronRight size={14} className="text-muted-foreground shrink-0" />
            {renderSegment(part, i)}
          </span>
        ))}
    </nav>
  );
}

export function Breadcrumb(props: BreadcrumbProps) {
  return <BreadcrumbInner key={props.path} {...props} />;
}
