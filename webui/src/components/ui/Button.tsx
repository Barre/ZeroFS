import { forwardRef } from "react";

const base = "transition-all duration-150 active:scale-[0.95] active:duration-75";

const variants = {
  primary: `${base} px-4 py-1.5 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-40 disabled:cursor-not-allowed disabled:active:scale-100 glow-primary`,
  ghost: `${base} px-3 py-1.5 text-sm rounded text-muted hover:bg-accent hover:text-foreground`,
  destructive: `${base} px-2.5 py-1.5 text-sm rounded text-destructive hover:bg-destructive/10`,
  "destructive-filled": `${base} px-4 py-1.5 text-sm rounded bg-destructive text-white hover:bg-destructive/90`,
  icon: `${base} p-1.5 rounded text-muted hover:text-foreground hover:bg-accent`,
  "icon-sm": `${base} p-0.5 rounded text-muted hover:text-foreground hover:bg-accent`,
} as const;

type Variant = keyof typeof variants;

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ variant = "ghost", className = "", ...props }, ref) => (
    <button ref={ref} className={`${variants[variant]} ${className}`} {...props} />
  ),
);
Button.displayName = "Button";
