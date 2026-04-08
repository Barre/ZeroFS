import { useState, useRef, useEffect, useCallback } from "react";
import { useResizeObserver } from "../../hooks/use-resize-observer";
import { Document, Page, pdfjs } from "react-pdf";
import { ZoomIn, ZoomOut, Maximize } from "lucide-react";
import { Button } from "../ui/Button";
import "react-pdf/dist/Page/AnnotationLayer.css";
import "react-pdf/dist/Page/TextLayer.css";

pdfjs.GlobalWorkerOptions.workerSrc = `https://cdn.jsdelivr.net/npm/pdfjs-dist@${pdfjs.version}/build/pdf.worker.min.mjs`;

interface PDFViewerProps {
  url: string;
}

export function PDFViewer({ url }: PDFViewerProps) {
  const [numPages, setNumPages] = useState(0);
  const [scale, setScale] = useState<number | null>(null); // null = fit-to-width
  const [currentPage, setCurrentPage] = useState(1);
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerRoRef, { width: containerWidth }] = useResizeObserver();
  const pageRefs = useRef<Map<number, HTMLDivElement>>(new Map());
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const onScroll = () => {
      const scrollMid = el.scrollTop + el.clientHeight / 3;
      let closest = 1;
      let closestDist = Infinity;
      pageRefs.current.forEach((div, pageNum) => {
        const dist = Math.abs(div.offsetTop - scrollMid);
        if (dist < closestDist) {
          closestDist = dist;
          closest = pageNum;
        }
      });
      setCurrentPage(closest);
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  const scrollToPage = useCallback((page: number) => {
    const div = pageRefs.current.get(page);
    if (div) div.scrollIntoView({ behavior: "smooth", block: "start" });
  }, []);

  const handlePageInput = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === "Enter") {
        const val = parseInt(e.currentTarget.value, 10);
        if (val >= 1 && val <= numPages) {
          scrollToPage(val);
          e.currentTarget.blur();
        }
      }
    },
    [numPages, scrollToPage],
  );

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement) return;
      if (e.key === "ArrowLeft" || e.key === "PageUp") {
        e.preventDefault();
        setCurrentPage((p) => {
          const next = Math.max(1, p - 1);
          scrollToPage(next);
          return next;
        });
      } else if (e.key === "ArrowRight" || e.key === "PageDown") {
        e.preventDefault();
        setCurrentPage((p) => {
          const next = Math.min(numPages, p + 1);
          scrollToPage(next);
          return next;
        });
      } else if (e.key === "Home") {
        e.preventDefault();
        scrollToPage(1);
      } else if (e.key === "End") {
        e.preventDefault();
        scrollToPage(numPages);
      }
    };
    el.addEventListener("keydown", onKeyDown);
    return () => el.removeEventListener("keydown", onKeyDown);
  }, [numPages, scrollToPage]);

  const fitWidth = containerWidth - 48; // 24px padding each side
  const effectiveScale = scale ?? (fitWidth > 0 ? fitWidth / 612 : 1); // 612 = default PDF point width

  const zoomIn = () => setScale(Math.min(5, effectiveScale + 0.25));
  const zoomOut = () => setScale(Math.max(0.25, effectiveScale - 0.25));
  const fitToWidth = () => setScale(null);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-center gap-2 px-3 py-1.5 border-b border-border shrink-0">
        <div className="flex items-center gap-1">
          <input
            ref={inputRef}
            type="text"
            defaultValue={currentPage}
            key={currentPage}
            onKeyDown={handlePageInput}
            onFocus={(e) => e.currentTarget.select()}
            className="w-10 text-center text-xs font-mono tabular-nums bg-transparent border border-border rounded px-1 py-0.5 focus:outline-none focus:border-primary"
          />
          <span className="text-xs text-muted font-mono tabular-nums">/ {numPages}</span>
        </div>
        <div className="w-px h-4 bg-border mx-1" />
        <Button variant="icon" onClick={zoomOut}>
          <ZoomOut size={14} />
        </Button>
        <span className="text-xs font-mono tabular-nums w-10 text-center">{Math.round(effectiveScale * 100)}%</span>
        <Button variant="icon" onClick={zoomIn}>
          <ZoomIn size={14} />
        </Button>
        <Button
          variant="icon"
          onClick={fitToWidth}
          title="Fit to width"
          className={scale === null ? "text-foreground bg-accent" : ""}
        >
          <Maximize size={14} />
        </Button>
      </div>

      <div
        ref={(el) => { containerRef.current = el; containerRoRef(el); }}
        className="flex-1 overflow-auto focus:outline-none"
        tabIndex={0}
        style={{ background: "#0c0c0e" }}
      >
        <Document
          file={url}
          onLoadSuccess={({ numPages: n }) => setNumPages(n)}
          loading=""
          error={<p className="text-destructive text-sm p-4">Failed to load PDF</p>}
        >
          {Array.from({ length: numPages }, (_, i) => (
            <div
              key={i + 1}
              ref={(el) => {
                if (el) pageRefs.current.set(i + 1, el);
              }}
              className="flex justify-center py-2"
            >
              <Page pageNumber={i + 1} scale={effectiveScale} loading="" renderAnnotationLayer renderTextLayer />
            </div>
          ))}
        </Document>
      </div>
    </div>
  );
}
