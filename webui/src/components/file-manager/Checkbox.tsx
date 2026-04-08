import { Check, Minus } from "lucide-react";

interface CheckboxProps {
  checked: boolean;
  indeterminate?: boolean;
  onChange: () => void;
}

export function Checkbox({ checked, indeterminate, onChange }: CheckboxProps) {
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        onChange();
      }}
      className={`w-4 h-4 rounded flex items-center justify-center border transition-colors ${
        checked || indeterminate
          ? "bg-primary border-primary"
          : "border-muted-foreground/30 hover:border-muted-foreground/60"
      }`}
    >
      {checked && <Check size={12} strokeWidth={3} className="text-primary-foreground" />}
      {indeterminate && !checked && <Minus size={12} strokeWidth={3} className="text-primary-foreground" />}
    </button>
  );
}
