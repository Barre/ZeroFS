import * as Tooltip from "@radix-ui/react-tooltip";

const tooltipContentClass =
  "bg-[#3d444d] text-[#e6edf3] rounded-md px-2.5 py-1 text-xs font-medium z-50 select-none shadow-md animate-[tooltipIn_0.1s_ease-out]";

export function Tip({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content side="bottom" sideOffset={6} className={tooltipContentClass}>
          {label}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}
