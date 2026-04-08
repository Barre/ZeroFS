import { useState, useRef, useCallback, useEffect } from "react";
import { usePresence } from "../../../hooks/use-presence";
import { useFrozen } from "../../../hooks/use-frozen";

export interface PreviewTarget {
  path: string;
  name: string;
  size: bigint;
}

export interface DiscardConfirm {
  resolve: (ok: boolean) => void;
}

export function usePreviewPanel() {
  const [previewEnabled, setPreviewEnabled] = useState(
    () => localStorage.getItem("zerofs-preview") !== "false",
  );
  const [previewFile, setPreviewFileRaw] = useState<PreviewTarget | null>(null);
  const previewFileRef = useRef(previewFile);
  useEffect(() => { previewFileRef.current = previewFile; }, [previewFile]);
  const previewDirtyRef = useRef(false);
  const [discardConfirm, setDiscardConfirm] = useState<DiscardConfirm | null>(null);
  const [previewWidth, setPreviewWidth] = useState(() => {
    const saved = localStorage.getItem("zerofs-preview-width");
    return saved ? parseInt(saved, 10) : 420;
  });
  const [previewFullscreen, setPreviewFullscreen] = useState(false);
  const resizing = useRef(false);

  const { mounted: previewMounted, open: previewOpen } = usePresence(!!previewFile, 150);
  const frozenPreview = useFrozen(previewFile);
  const renderPreview = previewMounted ? (previewFile ?? frozenPreview) : null;

  const setPreviewFile = useCallback(async (file: PreviewTarget | null): Promise<boolean> => {
    if (file?.path === previewFileRef.current?.path) return true;
    if (previewDirtyRef.current) {
      const confirmed = await new Promise<boolean>((resolve) => setDiscardConfirm({ resolve }));
      setDiscardConfirm(null);
      if (!confirmed) return false;
    }
    previewDirtyRef.current = false;
    setPreviewFileRaw(file);
    return true;
  }, []);

  const togglePreview = useCallback((getTarget: () => PreviewTarget | null) => {
    setPreviewEnabled((p) => {
      const next = !p;
      localStorage.setItem("zerofs-preview", String(next));
      if (!next) {
        setPreviewFile(null);
      } else {
        const target = getTarget();
        if (target) setPreviewFile(target);
      }
      return next;
    });
  }, [setPreviewFile]);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    resizing.current = true;
    const startX = e.clientX;
    const startWidth = previewWidth;
    const onMouseMove = (ev: MouseEvent) => {
      if (!resizing.current) return;
      const delta = startX - ev.clientX;
      setPreviewWidth(Math.max(280, Math.min(800, startWidth + delta)));
    };
    const onMouseUp = () => {
      resizing.current = false;
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setPreviewWidth((w) => {
        localStorage.setItem("zerofs-preview-width", String(w));
        return w;
      });
    };
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  }, [previewWidth]);

  return {
    previewEnabled,
    previewFile,
    previewFileRef,
    previewDirtyRef,
    previewWidth,
    previewFullscreen,
    setPreviewFullscreen,
    previewOpen,
    renderPreview,
    discardConfirm,
    setPreviewFile,
    setPreviewFileRaw,
    togglePreview,
    handleResizeStart,
  };
}
