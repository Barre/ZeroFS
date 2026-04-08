import { useState, useRef, useEffect } from "react";
import { Check, X } from "lucide-react";
import { Button } from "../ui/Button";

export function InlineInput({
  defaultValue,
  onConfirm,
  onCancel,
  selectBase,
}: {
  defaultValue: string;
  onConfirm: (value: string) => void;
  onCancel: () => void;
  selectBase?: boolean;
}) {
  const ref = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [value, setValue] = useState(defaultValue);

  useEffect(() => {
    const raf = requestAnimationFrame(() => {
      const el = ref.current;
      if (!el) return;
      el.focus();
      if (selectBase && defaultValue.includes(".")) {
        const dot = defaultValue.lastIndexOf(".");
        if (dot > 0) {
          el.setSelectionRange(0, dot);
          return;
        }
      }
      el.select();
    });
    return () => cancelAnimationFrame(raf);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const submit = () => {
    const trimmed = value.trim();
    if (trimmed && trimmed !== defaultValue) onConfirm(trimmed);
    else onCancel();
  };

  return (
    <div ref={containerRef} className="flex items-center gap-1.5" onClick={(e) => e.stopPropagation()}>
      <input
        ref={ref}
        type="text"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onBlur={(e) => {
          if (containerRef.current?.contains(e.relatedTarget as Node)) return;
          submit();
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            submit();
          }
          if (e.key === "Escape") {
            e.preventDefault();
            onCancel();
          }
          e.stopPropagation();
        }}
        className="bg-transparent border border-primary rounded px-1.5 py-0.5 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-primary min-w-[120px] select-text"
      />
      <Button
        variant="icon-sm"
        onMouseDown={(e) => e.preventDefault()}
        onClick={submit}
        className="flex items-center justify-center w-6 h-6 text-success hover:bg-success/10"
        title="Confirm"
      >
        <Check size={14} strokeWidth={2.5} />
      </Button>
      <Button
        variant="icon-sm"
        onMouseDown={(e) => e.preventDefault()}
        onClick={onCancel}
        className="flex items-center justify-center w-6 h-6"
        title="Cancel"
      >
        <X size={14} strokeWidth={2} />
      </Button>
    </div>
  );
}
