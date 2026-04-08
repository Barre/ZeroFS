import { useState, useEffect, useCallback, useRef } from "react";

/** Track an element's content-box width (and optionally height) via ResizeObserver. */
export function useResizeObserver(): [
  (el: HTMLElement | null) => void,
  { width: number; height: number },
] {
  const [size, setSize] = useState({ width: 0, height: 0 });
  const roRef = useRef<ResizeObserver | null>(null);
  const elRef = useRef<HTMLElement | null>(null);

  const ref = useCallback((el: HTMLElement | null) => {
    if (el === elRef.current) return;
    elRef.current = el;
    if (roRef.current) {
      roRef.current.disconnect();
      roRef.current = null;
    }
    if (!el) return;
    const ro = new ResizeObserver(([e]) =>
      setSize({ width: e.contentRect.width, height: e.contentRect.height }),
    );
    ro.observe(el);
    roRef.current = ro;
  }, []);

  useEffect(() => {
    return () => roRef.current?.disconnect();
  }, []);

  return [ref, size];
}
